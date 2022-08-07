/*
Copyright 2018 The Kubernetes Authors.

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

package config

import (
	"math"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	componentbaseconfig "k8s.io/component-base/config"
)

const (
	// SchedulerPolicyConfigMapKey defines the key of the element in the
	// scheduler's policy ConfigMap that contains scheduler's policy config.
	SchedulerPolicyConfigMapKey = "policy.cfg"

	// DefaultKubeSchedulerPort is the default port for the scheduler status server.
	// May be overridden by a flag at startup.
	DefaultKubeSchedulerPort = 10259
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// KubeSchedulerConfiguration configures a scheduler
// scheduler启动配置参数
type KubeSchedulerConfiguration struct {
	// TypeMeta contains the API version and kind. In kube-scheduler, after
	// conversion from the versioned KubeSchedulerConfiguration type to this
	// internal type, we set the APIVersion field to the scheme group/version of
	// the type we converted from. This is done in cmd/kube-scheduler in two
	// places: (1) when loading config from a file, (2) generating the default
	// config. Based on the versioned type set in this field, we make decisions;
	// for example (1) during validation to check for usage of removed plugins,
	// (2) writing config to a file, (3) initialising the scheduler.
	// TypeMeta包含API版本和类型。
	// 在k8s-调度器中，从版本化的KubeSchedulerConfiguration类型转换为此内部类型后，我们将APIVersion字段设置为我们转换的类型的方案组/版本。
	// 这是在cmd/kube-scheduler中在两个地方完成的：（1）从文件加载配置时，（2）生成默认配置。根据在此字段中设置的版本化类型，我们做出决定；
	// 例如（1）在验证期间检查已删除插件的使用情况，（2）将配置写入文件，（3）初始化调度程序。
	metav1.TypeMeta

	// Parallelism defines the amount of parallelism in algorithms for scheduling a Pods. Must be greater than 0. Defaults to 16
	// 定义调度Pods的算法中的并行度（即：开启几个goroutine）必须大于0。默认为16
	Parallelism int32

	// LeaderElection defines the configuration of leader election client.
	// 定义leader选举的客户端配置
	LeaderElection componentbaseconfig.LeaderElectionConfiguration

	// ClientConnection specifies the kubeconfig file and client connection
	// settings for the proxy server to use when communicating with the apiserver.
	// 指定代理服务器在与apiserver通信时，使用的kubeconfig文件和客户端连接设置
	ClientConnection componentbaseconfig.ClientConnectionConfiguration
	// HealthzBindAddress is the IP address and port for the health check server to serve on.
	// 健康检查服务器的ip地址和端口
	HealthzBindAddress string
	// MetricsBindAddress is the IP address and port for the metrics server to serve on.
	// 服务状态的ip地址和端口
	MetricsBindAddress string

	// DebuggingConfiguration holds configuration for Debugging related features
	// DebuggingConfiguration保存调试相关功能的配置
	// TODO: We might wanna make this a substruct like Debugging componentbaseconfig.DebuggingConfiguration 我们可能想把它变成一个子结构，比如调试组件基础配置图。调试配置
	componentbaseconfig.DebuggingConfiguration

	// PercentageOfNodesToScore is the percentage of all nodes that once found feasible
	// for running a pod, the scheduler stops its search for more feasible nodes in
	// the cluster. This helps improve scheduler's performance. Scheduler always tries to find
	// at least "minFeasibleNodesToFind" feasible nodes no matter what the value of this flag is.
	// Example: if the cluster size is 500 nodes and the value of this flag is 30,
	// then scheduler stops finding further feasible nodes once it finds 150 feasible ones.
	// When the value is 0, default percentage (5%--50% based on the size of the cluster) of the
	// nodes will be scored.
	// PercentageOfNodesToScore是一旦发现运行pod可行，调度程序就停止在集群中搜索更多可行节点的所有节点的百分比。
	// 这有助于提高调度程序的性能。无论此标志的值是多少，调度程序总是尝试至少找到“minFeasibleNodesToFind”可行节点。
	// 示例：如果集群大小为500个节点并且此标志的值为30，则调度程序在找到150个可行节点后停止寻找其他可行节点。
	// 当值为0时，将对节点的默认百分比（基于集群大小的5%--50%）进行评分。
	// 在集群所有节点中，通过过滤的节点占集群中所有节点的百分比,即：过滤出来这个数量的宿主机之后，就不再遍历其他宿主机
	PercentageOfNodesToScore int32

	// PodInitialBackoffSeconds is the initial backoff for unschedulable pods.
	// If specified, it must be greater than 0. If this value is null, the default value (1s)
	// will be used.
	// 不可调度Pod的初始退避时间
	// 如果指定，则必须大于0。如果此值为null，则将使用默认值（1s）
	PodInitialBackoffSeconds int64

	// PodMaxBackoffSeconds is the max backoff for unschedulable pods.
	// If specified, it must be greater than or equal to podInitialBackoffSeconds. If this value is null,
	// the default value (10s) will be used.
	// 不可调度Pod的最大退避时间
	// 如果指定，它必须大于或等于初始退避时间。如果此值为null，将使用默认值（10s）。
	PodMaxBackoffSeconds int64

	// Profiles are scheduling profiles that kube-scheduler supports. Pods can
	// choose to be scheduled under a particular profile by setting its associated
	// scheduler name. Pods that don't specify any scheduler name are scheduled
	// with the "default-scheduler" profile, if present here.
	// k8s-scheduler支持的调度配置文件。Pods可以通过设置其关联的调度程序名称来选择在特定配置文件下进行调度。
	// 不指定任何调度程序名称的Pods使用“default-scheduler”配置文件进行调度（如果此处存在）。
	Profiles []KubeSchedulerProfile

	// Extenders are the list of scheduler extenders, each holding the values of how to communicate
	// with the extender. These extenders are shared by all scheduler profiles.
	// 扩展器是调度程序扩展器的列表，每个扩展器都包含如何与扩展器通信的值。这些扩展器由所有调度程序配置文件共享。
	// scheduler扩展点的列表
	Extenders []Extender
}

// KubeSchedulerProfile is a scheduling profile.
// 调度配置文件
type KubeSchedulerProfile struct {
	// SchedulerName is the name of the scheduler associated to this profile.
	// If SchedulerName matches with the pod's "spec.schedulerName", then the pod
	// is scheduled with this profile.
	// SchedulerName是与此配置文件关联的调度程序的名称。
	// 如果SchedulerName与pod的“spec.schedulerName”匹配，则使用此配置文件调度pod。
	// KubeSchedulerProfile.SchedulerName = Framework.profileName
	SchedulerName string

	// Plugins specify the set of plugins that should be enabled or disabled.
	// Enabled plugins are the ones that should be enabled in addition to the
	// default plugins. Disabled plugins are any of the default plugins that
	// should be disabled.
	// When no enabled or disabled plugin is specified for an extension point,
	// default plugins for that extension point will be used if there is any.
	// If a QueueSort plugin is specified, the same QueueSort Plugin and
	// PluginConfig must be specified for all profiles.
	// 该调度器中指定的所有插件
	// 插件指定应启用或禁用的插件集。
	// 启用的插件是除了默认插件之外还应启用的插件。禁用的插件是应禁用的任何默认插件。
	// 当没有为扩展点指定启用或禁用插件时，将使用该扩展点的默认插件（如果有）。
	// 如果指定了QueueSort插件，则必须为所有配置文件指定相同的QueueSort插件和PluginConfig。
	Plugins *Plugins

	// PluginConfig is an optional set of custom plugin arguments for each plugin.
	// Omitting config args for a plugin is equivalent to using the default config
	// for that plugin.
	// PluginConfig是每个插件的一组可选自定义插件参数
	// 省略插件的配置参数相当于使用该插件的默认配置
	PluginConfig []PluginConfig
}

// Plugins include multiple extension points. When specified, the list of plugins for
// a particular extension point are the only ones enabled. If an extension point is
// omitted from the config, then the default set of plugins is used for that extension point.
// Enabled plugins are called in the order specified here, after default plugins. If they need to
// be invoked before default plugins, default plugins must be disabled and re-enabled here in desired order.
type Plugins struct {
	// QueueSort is a list of plugins that should be invoked when sorting pods in the scheduling queue.
	QueueSort PluginSet

	// PreFilter is a list of plugins that should be invoked at "PreFilter" extension point of the scheduling framework.
	PreFilter PluginSet

	// Filter is a list of plugins that should be invoked when filtering out nodes that cannot run the Pod.
	Filter PluginSet

	// PostFilter is a list of plugins that are invoked after filtering phase, but only when no feasible nodes were found for the pod.
	PostFilter PluginSet

	// PreScore is a list of plugins that are invoked before scoring.
	PreScore PluginSet

	// Score is a list of plugins that should be invoked when ranking nodes that have passed the filtering phase.
	Score PluginSet

	// Reserve is a list of plugins invoked when reserving/unreserving resources
	// after a node is assigned to run the pod.
	Reserve PluginSet

	// Permit is a list of plugins that control binding of a Pod. These plugins can prevent or delay binding of a Pod.
	Permit PluginSet

	// PreBind is a list of plugins that should be invoked before a pod is bound.
	PreBind PluginSet

	// Bind is a list of plugins that should be invoked at "Bind" extension point of the scheduling framework.
	// The scheduler call these plugins in order. Scheduler skips the rest of these plugins as soon as one returns success.
	Bind PluginSet

	// PostBind is a list of plugins that should be invoked after a pod is successfully bound.
	PostBind PluginSet

	// MultiPoint is a simplified config field for enabling plugins for all valid extension points
	MultiPoint PluginSet
}

// PluginSet specifies enabled and disabled plugins for an extension point.
// If an array is empty, missing, or nil, default plugins at that extension point will be used.
type PluginSet struct {
	// Enabled specifies plugins that should be enabled in addition to default plugins.
	// These are called after default plugins and in the same order specified here.
	Enabled []Plugin
	// Disabled specifies default plugins that should be disabled.
	// When all default plugins need to be disabled, an array containing only one "*" should be provided.
	Disabled []Plugin
}

// Plugin specifies a plugin name and its weight when applicable. Weight is used only for Score plugins.
type Plugin struct {
	// Name defines the name of plugin
	Name string
	// Weight defines the weight of plugin, only used for Score plugins.
	Weight int32
}

// PluginConfig specifies arguments that should be passed to a plugin at the time of initialization.
// A plugin that is invoked at multiple extension points is initialized once. Args can have arbitrary structure.
// It is up to the plugin to process these Args.
type PluginConfig struct {
	// Name defines the name of plugin being configured
	Name string
	// Args defines the arguments passed to the plugins at the time of initialization. Args can have arbitrary structure.
	Args runtime.Object
}

/*
 * NOTE: The following variables and methods are intentionally left out of the staging mirror.
 */
const (
	// DefaultPercentageOfNodesToScore defines the percentage of nodes of all nodes
	// that once found feasible, the scheduler stops looking for more nodes.
	// A value of 0 means adaptive, meaning the scheduler figures out a proper default.
	DefaultPercentageOfNodesToScore = 0

	// MaxCustomPriorityScore is the max score UtilizationShapePoint expects.
	MaxCustomPriorityScore int64 = 10

	// MaxTotalScore is the maximum total score.
	MaxTotalScore int64 = math.MaxInt64

	// MaxWeight defines the max weight value allowed for custom PriorityPolicy
	MaxWeight = MaxTotalScore / MaxCustomPriorityScore
)

// Names returns the list of enabled plugin names.
func (p *Plugins) Names() []string {
	if p == nil {
		return nil
	}
	extensions := []PluginSet{
		p.PreFilter,
		p.Filter,
		p.PostFilter,
		p.Reserve,
		p.PreScore,
		p.Score,
		p.PreBind,
		p.Bind,
		p.PostBind,
		p.Permit,
		p.QueueSort,
	}
	n := sets.NewString()
	for _, e := range extensions {
		for _, pg := range e.Enabled {
			n.Insert(pg.Name)
		}
	}
	return n.List()
}

// Extender holds the parameters used to communicate with the extender. If a verb is unspecified/empty,
// it is assumed that the extender chose not to provide that extension.
// 扩展程序保存用于与扩展程序通信的参数。如果动词未指定/为空，则假定扩展程序选择不提供该扩展。
type Extender struct {
	// URLPrefix at which the extender is available
	URLPrefix string
	// Verb for the filter call, empty if not supported. This verb is appended to the URLPrefix when issuing the filter call to extender.
	FilterVerb string
	// Verb for the preempt call, empty if not supported. This verb is appended to the URLPrefix when issuing the preempt call to extender.
	PreemptVerb string
	// Verb for the prioritize call, empty if not supported. This verb is appended to the URLPrefix when issuing the prioritize call to extender.
	PrioritizeVerb string
	// The numeric multiplier for the node scores that the prioritize call generates.
	// The weight should be a positive integer
	Weight int64
	// Verb for the bind call, empty if not supported. This verb is appended to the URLPrefix when issuing the bind call to extender.
	// If this method is implemented by the extender, it is the extender's responsibility to bind the pod to apiserver. Only one extender
	// can implement this function.
	BindVerb string
	// EnableHTTPS specifies whether https should be used to communicate with the extender
	EnableHTTPS bool
	// TLSConfig specifies the transport layer security config
	TLSConfig *ExtenderTLSConfig
	// HTTPTimeout specifies the timeout duration for a call to the extender. Filter timeout fails the scheduling of the pod. Prioritize
	// timeout is ignored, k8s/other extenders priorities are used to select the node.
	HTTPTimeout metav1.Duration
	// NodeCacheCapable specifies that the extender is capable of caching node information,
	// so the scheduler should only send minimal information about the eligible nodes
	// assuming that the extender already cached full details of all nodes in the cluster
	NodeCacheCapable bool
	// ManagedResources is a list of extended resources that are managed by
	// this extender.
	// - A pod will be sent to the extender on the Filter, Prioritize and Bind
	//   (if the extender is the binder) phases iff the pod requests at least
	//   one of the extended resources in this list. If empty or unspecified,
	//   all pods will be sent to this extender.
	// - If IgnoredByScheduler is set to true for a resource, kube-scheduler
	//   will skip checking the resource in predicates.
	// +optional
	ManagedResources []ExtenderManagedResource
	// Ignorable specifies if the extender is ignorable, i.e. scheduling should not
	// fail when the extender returns an error or is not reachable.
	Ignorable bool
}

// ExtenderManagedResource describes the arguments of extended resources
// managed by an extender.
type ExtenderManagedResource struct {
	// Name is the extended resource name.
	Name string
	// IgnoredByScheduler indicates whether kube-scheduler should ignore this
	// resource when applying predicates.
	IgnoredByScheduler bool
}

// ExtenderTLSConfig contains settings to enable TLS with extender
type ExtenderTLSConfig struct {
	// Server should be accessed without verifying the TLS certificate. For testing only.
	Insecure bool
	// ServerName is passed to the server for SNI and is used in the client to check server
	// certificates against. If ServerName is empty, the hostname used to contact the
	// server is used.
	ServerName string

	// Server requires TLS client certificate authentication
	CertFile string
	// Server requires TLS client certificate authentication
	KeyFile string
	// Trusted root certificates for server
	CAFile string

	// CertData holds PEM-encoded bytes (typically read from a client certificate file).
	// CertData takes precedence over CertFile
	CertData []byte
	// KeyData holds PEM-encoded bytes (typically read from a client certificate key file).
	// KeyData takes precedence over KeyFile
	KeyData []byte `datapolicy:"security-key"`
	// CAData holds PEM-encoded bytes (typically read from a root certificates bundle).
	// CAData takes precedence over CAFile
	CAData []byte
}
