/*
Copyright 2014 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package app implements a Server object for running the scheduler.
package app

import (
	"context"
	"fmt"
	"net/http"
	"os"
	goruntime "runtime"

	"github.com/spf13/cobra"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	genericapifilters "k8s.io/apiserver/pkg/endpoints/filters"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/server"
	genericfilters "k8s.io/apiserver/pkg/server/filters"
	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/apiserver/pkg/server/mux"
	"k8s.io/apiserver/pkg/server/routes"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/tools/leaderelection"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/cli/globalflag"
	"k8s.io/component-base/configz"
	"k8s.io/component-base/logs"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/term"
	"k8s.io/component-base/version"
	"k8s.io/component-base/version/verflag"
	"k8s.io/klog/v2"
	schedulerserverconfig "k8s.io/kubernetes/cmd/kube-scheduler/app/config"
	"k8s.io/kubernetes/cmd/kube-scheduler/app/options"
	"k8s.io/kubernetes/pkg/scheduler"
	kubeschedulerconfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/latest"
	"k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/kubernetes/pkg/scheduler/metrics/resources"
	"k8s.io/kubernetes/pkg/scheduler/profile"
)

func init() {
	utilruntime.Must(logsapi.AddFeatureGates(utilfeature.DefaultMutableFeatureGate))
}

// Option configures a framework.Registry.
type Option func(runtime.Registry) error

// NewSchedulerCommand creates a *cobra.Command object with default parameters and registryOptions
// 创建一个带有默认参数和注册表选项的*cobra.Command对象
func NewSchedulerCommand(registryOptions ...Option) *cobra.Command {
	// 创建Scheduler所需要的配置信息并初始化配置
	opts := options.NewOptions()
	// 创建cmd对象，并运行scheduler
	cmd := &cobra.Command{
		Use: "kube-scheduler",
		Long: `The Kubernetes scheduler is a control plane process which assigns
Pods to Nodes. The scheduler determines which Nodes are valid placements for
each Pod in the scheduling queue according to constraints and available
resources. The scheduler then ranks each valid Node and binds the Pod to a
suitable Node. Multiple different schedulers may be used within a cluster;
kube-scheduler is the reference implementation.
See [scheduling](https://kubernetes.io/docs/concepts/scheduling-eviction/)
for more information about scheduling and the kube-scheduler component.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// 运行scheduler
			return runCommand(cmd, opts, registryOptions...)
		},
		Args: func(cmd *cobra.Command, args []string) error {
			for _, arg := range args {
				if len(arg) > 0 {
					return fmt.Errorf("%q does not take any arguments, got %q", cmd.CommandPath(), args)
				}
			}
			return nil
		},
	}

	nfs := opts.Flags
	verflag.AddFlags(nfs.FlagSet("global"))
	globalflag.AddGlobalFlags(nfs.FlagSet("global"), cmd.Name(), logs.SkipLoggingConfigurationFlags())
	fs := cmd.Flags()
	for _, f := range nfs.FlagSets {
		fs.AddFlagSet(f)
	}

	cols, _, _ := term.TerminalSize(cmd.OutOrStdout())
	cliflag.SetUsageAndHelpFunc(cmd, *nfs, cols)

	if err := cmd.MarkFlagFilename("config", "yaml", "yml", "json"); err != nil {
		klog.ErrorS(err, "Failed to mark flag filename")
	}

	return cmd
}

// runCommand runs the scheduler.
// 运行scheduler调度器
func runCommand(cmd *cobra.Command, opts *options.Options, registryOptions ...Option) error {
	verflag.PrintAndExitIfRequested()

	// Activate logging as soon as possible, after that show flags with the final logging configuration.
	// 尽快激活日志记录，之后显示带有最终日志记录配置的标志
	if err := logsapi.ValidateAndApply(opts.Logs, utilfeature.DefaultFeatureGate); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	cliflag.PrintFlags(cmd.Flags())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		stopCh := server.SetupSignalHandler()
		<-stopCh
		cancel()
	}()
	// 1.根据KubeSchedulerConfiguration创建调度类对象，对Profiles中的每个调度类进行初始化，创建插件，绑定插件配置
	cc, sched, err := Setup(ctx, opts, registryOptions...)
	if err != nil {
		return err
	}
	// 2.运行scheduler
	return Run(ctx, cc, sched)
}

// Run executes the scheduler based on the given configuration. It only returns on error or when context is done.
// 根据给定的配置执行调度程序。它只在错误或上下文完成时返回
func Run(ctx context.Context, cc *schedulerserverconfig.CompletedConfig, sched *scheduler.Scheduler) error {
	// To help debugging, immediately log version
	// 为了帮助调试，立即记录版本
	klog.InfoS("Starting Kubernetes Scheduler", "version", version.Get())

	klog.InfoS("Golang settings", "GOGC", os.Getenv("GOGC"), "GOMAXPROCS", os.Getenv("GOMAXPROCS"), "GOTRACEBACK", os.Getenv("GOTRACEBACK"))

	// Configz registration.
	// 1.配置configz全局配置
	if cz, err := configz.New("componentconfig"); err == nil {
		cz.Set(cc.ComponentConfig)
	} else {
		return fmt.Errorf("unable to register configz: %s", err)
	}

	// Start events processing pipeline.
	// 2.准备事件广播管理器
	// Event（事件）作为 Kubernetes 的一种资源对象，用于描述集群产生的事件。
	// 如 kube-scheduler 组件在调度过程中，对某个 Pod 的调度做了什么处理、为什么需要对某些 Pod 做驱逐等决策都通过向 apiserver 请求创建 Event 记录下来，新创建的 Event 默认保留1个小时。
	cc.EventBroadcaster.StartRecordingToSink(ctx.Done())
	defer cc.EventBroadcaster.Shutdown()

	// Setup healthz checks.
	// 选举健康检查
	var checks []healthz.HealthChecker
	if cc.ComponentConfig.LeaderElection.LeaderElect {
		checks = append(checks, cc.LeaderElection.WatchDog)
	}
	// 创建了一个waitingForLeader的channel用来监听信号
	waitingForLeader := make(chan struct{})
	// 调度程序选主函数
	isLeader := func() bool {
		select {
		case _, ok := <-waitingForLeader:
			// if channel is closed, we are leading
			// 如果管道关闭了，则当前节点就是主节点
			return !ok
		default:
			// channel is open, we are waiting for a leader
			// 如果管道是开着的，则继续等待主节点选出
			return false
		}
	}

	// Start up the healthz server.
	// 3.启动http server和健康检查服务
	if cc.SecureServing != nil {
		handler := buildHandlerChain(newHealthzAndMetricsHandler(&cc.ComponentConfig, cc.InformerFactory, isLeader, checks...), cc.Authentication.Authenticator, cc.Authorization.Authorizer)
		// TODO: handle stoppedCh and listenerStoppedCh returned by c.SecureServing.Serve
		if _, _, err := cc.SecureServing.Serve(handler, 0, ctx.Done()); err != nil {
			// fail early for secure handlers, removing the old error loop from above
			// 安全处理程序的早期失败，从上面删除旧的错误循环
			return fmt.Errorf("failed to start secure server: %v", err)
		}
	}

	// Start all informers.
	// 4.启动所有informer
	// k8s中有各种类型的资源，包括自定义的。而Informer的实现就将调度和资源结合了起来。
	// pod informer 的启动逻辑是，只监听 status.phase 不为 succeeded 以及 failed 状态的 pod，即非 terminating 的 pod。
	cc.InformerFactory.Start(ctx.Done())
	// DynInformerFactory can be nil in tests.
	// DynInformerFactory在测试中可以为nil
	if cc.DynInformerFactory != nil {
		cc.DynInformerFactory.Start(ctx.Done())
	}

	// Wait for all caches to sync before scheduling.
	// 在调度之前等待所有informer缓存同步完成
	cc.InformerFactory.WaitForCacheSync(ctx.Done())
	// DynInformerFactory can be nil in tests.
	if cc.DynInformerFactory != nil {
		cc.DynInformerFactory.WaitForCacheSync(ctx.Done())
	}

	// If leader election is enabled, runCommand via LeaderElector until done and exit.
	// 因为master节点可以存在多个，选举一个作为leader
	// 如果启用了领导者选举，请通过Leaderelector运行命令，直到完成并退出。
	// 5.leader选举
	if cc.LeaderElection != nil {
		// 选举的回调函数
		cc.LeaderElection.Callbacks = leaderelection.LeaderCallbacks{
			// 选举成功
			OnStartedLeading: func(ctx context.Context) {
				// 选举成功则会关闭等待信道，并由自己启动sched，与上面isLeader函数第一段对应
				close(waitingForLeader)
				// 1.--->pkg\scheduler\scheduler.go这里就跳转到scheduler具体逻辑里开始正常工作了,sched是Setup函数中初始化的
				sched.Run(ctx)
			},
			// 选举失败
			// 钩子函数，开启Leading时运行调度，结束时打印报错
			OnStoppedLeading: func() {
				// 选举失败则正常结束退出了
				select {
				case <-ctx.Done():
					// We were asked to terminate. Exit 0.
					// 我们被要求终止，Exit 0
					klog.InfoS("Requested to terminate, exiting")
					os.Exit(0)
				default:
					// We lost the lock.
					klog.ErrorS(nil, "Leaderelection lost")
					klog.FlushAndExit(klog.ExitFlushTimeout, 1)
				}
			},
		}
		// 开启新一轮选举
		leaderElector, err := leaderelection.NewLeaderElector(*cc.LeaderElection)
		if err != nil {
			return fmt.Errorf("couldn't create leader elector: %v", err)
		}
		leaderElector.Run(ctx)

		return fmt.Errorf("lost lease")
	}

	// Leader election is disabled, so runCommand inline until done.
	// 领导者选举失败，所以runCommand函数会一直运行直到完成
	close(waitingForLeader)
	// 6.执行sched的Run方法（主调度逻辑）
	// 最后执行的 sched.Run() 调度循环逻辑，若 informer 中的 cache 同步完成后会启动一个循环逻辑执行 sched.scheduleOne 方法
	sched.Run(ctx)
	return fmt.Errorf("finished without leader elect")
}

// buildHandlerChain wraps the given handler with the standard filters.
func buildHandlerChain(handler http.Handler, authn authenticator.Request, authz authorizer.Authorizer) http.Handler {
	requestInfoResolver := &apirequest.RequestInfoFactory{}
	failedHandler := genericapifilters.Unauthorized(scheme.Codecs)

	handler = genericapifilters.WithAuthorization(handler, authz, scheme.Codecs)
	handler = genericapifilters.WithAuthentication(handler, authn, failedHandler, nil)
	handler = genericapifilters.WithRequestInfo(handler, requestInfoResolver)
	handler = genericapifilters.WithCacheControl(handler)
	handler = genericfilters.WithHTTPLogging(handler)
	handler = genericfilters.WithPanicRecovery(handler, requestInfoResolver)

	return handler
}

func installMetricHandler(pathRecorderMux *mux.PathRecorderMux, informers informers.SharedInformerFactory, isLeader func() bool) {
	configz.InstallHandler(pathRecorderMux)
	pathRecorderMux.Handle("/metrics", legacyregistry.HandlerWithReset())

	resourceMetricsHandler := resources.Handler(informers.Core().V1().Pods().Lister())
	pathRecorderMux.HandleFunc("/metrics/resources", func(w http.ResponseWriter, req *http.Request) {
		if !isLeader() {
			return
		}
		resourceMetricsHandler.ServeHTTP(w, req)
	})
}

// newHealthzAndMetricsHandler creates a healthz server from the config, and will also
// embed the metrics handler.
func newHealthzAndMetricsHandler(config *kubeschedulerconfig.KubeSchedulerConfiguration, informers informers.SharedInformerFactory, isLeader func() bool, checks ...healthz.HealthChecker) http.Handler {
	pathRecorderMux := mux.NewPathRecorderMux("kube-scheduler")
	healthz.InstallHandler(pathRecorderMux, checks...)
	installMetricHandler(pathRecorderMux, informers, isLeader)
	if config.EnableProfiling {
		routes.Profiling{}.Install(pathRecorderMux)
		if config.EnableContentionProfiling {
			goruntime.SetBlockProfileRate(1)
		}
		routes.DebugFlags{}.Install(pathRecorderMux, "v", routes.StringFlagPutHandler(logs.GlogSetter))
	}
	return pathRecorderMux
}

func getRecorderFactory(cc *schedulerserverconfig.CompletedConfig) profile.RecorderFactory {
	return func(name string) events.EventRecorder {
		return cc.EventBroadcaster.NewRecorder(name)
	}
}

// WithPlugin creates an Option based on plugin name and factory. Please don't remove this function: it is used to register out-of-tree plugins,
// hence there are no references to it from the kubernetes scheduler code base.
func WithPlugin(name string, factory runtime.PluginFactory) Option {
	return func(registry runtime.Registry) error {
		return registry.Register(name, factory)
	}
}

// Setup creates a completed config and a scheduler based on the command args and options
// 安装程序根据命令参数和选项创建一个完整的配置和一个调度程序
// Setup的主要功能是根据KubeSchedulerConfiguration创建调度类，对Profiles中的每个调度类进行初始化，创建插件，绑定插件配置
func Setup(ctx context.Context, opts *options.Options, outOfTreeRegistryOptions ...Option) (*schedulerserverconfig.CompletedConfig, *scheduler.Scheduler, error) {
	if cfg, err := latest.Default(); err != nil {
		return nil, nil, err
	} else {
		opts.ComponentConfig = cfg
	}
	// 配置校验
	if errs := opts.Validate(); len(errs) > 0 {
		return nil, nil, utilerrors.NewAggregate(errs)
	}
	// 创建kube客户端、创建EventBroadcaster、创建InformerFactory监听所有POD信息
	c, err := opts.Config()
	if err != nil {
		return nil, nil, err
	}

	// Get the completed config
	// 获取完成的配置，认证创建
	cc := c.Complete()

	outOfTreeRegistry := make(runtime.Registry)
	for _, option := range outOfTreeRegistryOptions {
		if err := option(outOfTreeRegistry); err != nil {
			return nil, nil, err
		}
	}

	recorderFactory := getRecorderFactory(&cc)
	completedProfiles := make([]kubeschedulerconfig.KubeSchedulerProfile, 0)
	// Create the scheduler.
	// 创建一个调度器
	sched, err := scheduler.New(cc.Client,
		cc.InformerFactory,
		cc.DynInformerFactory,
		recorderFactory,
		ctx.Done(),
		scheduler.WithComponentConfigVersion(cc.ComponentConfig.TypeMeta.APIVersion),
		scheduler.WithKubeConfig(cc.KubeConfig),
		scheduler.WithProfiles(cc.ComponentConfig.Profiles...),
		scheduler.WithPercentageOfNodesToScore(cc.ComponentConfig.PercentageOfNodesToScore),
		scheduler.WithFrameworkOutOfTreeRegistry(outOfTreeRegistry),
		scheduler.WithPodMaxBackoffSeconds(cc.ComponentConfig.PodMaxBackoffSeconds),
		scheduler.WithPodInitialBackoffSeconds(cc.ComponentConfig.PodInitialBackoffSeconds),
		scheduler.WithPodMaxInUnschedulablePodsDuration(cc.PodMaxInUnschedulablePodsDuration),
		scheduler.WithExtenders(cc.ComponentConfig.Extenders...),
		scheduler.WithParallelism(cc.ComponentConfig.Parallelism),
		scheduler.WithBuildFrameworkCapturer(func(profile kubeschedulerconfig.KubeSchedulerProfile) {
			// Profiles are processed during Framework instantiation to set default plugins and configurations. Capturing them for logging
			// 配置文件在Framework实例化期间进行处理以设置默认插件和配置。捕获它们以进行日志记录
			completedProfiles = append(completedProfiles, profile)
		}),
	)
	if err != nil {
		return nil, nil, err
	}
	if err := options.LogOrWriteConfig(opts.WriteConfigTo, &cc.ComponentConfig, completedProfiles); err != nil {
		return nil, nil, err
	}

	return &cc, sched, nil
}
