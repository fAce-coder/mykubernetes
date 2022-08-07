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

package scheduler

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/apis/core/validation"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/parallelize"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	internalqueue "k8s.io/kubernetes/pkg/scheduler/internal/queue"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	"k8s.io/kubernetes/pkg/scheduler/util"
	utiltrace "k8s.io/utils/trace"
)

const (
	// SchedulerError is the reason recorded for events when an error occurs during scheduling a pod.
	SchedulerError = "SchedulerError"
	// Percentage of plugin metrics to be sampled.
	pluginMetricsSamplePercent = 10
	// minFeasibleNodesToFind is the minimum number of nodes that would be scored
	// in each scheduling cycle. This is a semi-arbitrary value to ensure that a
	// certain minimum of nodes are checked for feasibility. This in turn helps
	// ensure a minimum level of spreading.
	minFeasibleNodesToFind = 100
	// minFeasibleNodesPercentageToFind is the minimum percentage of nodes that
	// would be scored in each scheduling cycle. This is a semi-arbitrary value
	// to ensure that a certain minimum of nodes are checked for feasibility.
	// This in turn helps ensure a minimum level of spreading.
	minFeasibleNodesPercentageToFind = 5
)

var clearNominatedNode = &framework.NominatingInfo{NominatingMode: framework.ModeOverride, NominatedNodeName: ""}

// scheduleOne does the entire scheduling workflow for a single pod. It is serialized on the scheduling algorithm's host fitting.
// 为单个pod执行整个调度工作流程，它在调度算法的主机拟合上序列化

// scheduleOne
//
//	@Description: 调度的主方法，为单个Pod执行整个调度工作流，执行调度流程中所有的Extension扩展点上的插件
//	@receiver sched *Scheduler对象
//	@param ctx 请求上下文
func (sched *Scheduler) scheduleOne(ctx context.Context) {
	// 1.从所有要调度的Pod队列中，获取下一个需要调度的Pod
	podInfo := sched.NextPod()
	// pod could be nil when schedulerQueue is closed
	// 当调度队列关闭时，Pod可能为nil
	if podInfo == nil || podInfo.Pod == nil {
		return
	}
	pod := podInfo.Pod
	// 2.获取pod绑定的具体framework调度器
	fwk, err := sched.frameworkForPod(pod)
	if err != nil {
		// This shouldn't happen, because we only accept for scheduling the pods
		// which specify a scheduler name that matches one of the profiles.
		// 这不应该发生，因为我们只接受调度指定与配置文件之一匹配的调度程序名称的pod。
		klog.ErrorS(err, "Error occurred")
		return
	}
	// 3.跳过特殊情况下不调度的Pod，继续为下一个Pod调度
	if sched.skipPodSchedule(fwk, pod) {
		return
	}

	klog.V(3).InfoS("Attempting to schedule pod", "pod", klog.KObj(pod))

	// Synchronously attempt to find a fit for the pod.
	// 同步地尝试为Pod选择合适宿主机
	start := time.Now()
	// 为当前Pod新建一个储存整个调度流程中状态数据的CycleState对象
	state := framework.NewCycleState()
	state.SetRecordPluginMetrics(rand.Intn(100) < pluginMetricsSamplePercent)
	// Initialize an empty podsToActivate struct, which will be filled up by plugins or stay empty.
	// 初始化一个空的podsToActivate结构体，它将被插件填充或保持为空。
	podsToActivate := framework.NewPodsToActivate()
	state.Write(framework.PodsToActivateKey, podsToActivate)

	schedulingCycleCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	// 4.执行预选和优选，为该Pod选择出合适的那个最终宿主机节点
	scheduleResult, err := sched.SchedulePod(schedulingCycleCtx, fwk, state, pod)
	if err != nil {
		// SchedulePod() may have failed because the pod would not fit on any host, so we try to
		// preempt, with the expectation that the next time the pod is tried for scheduling it
		// will fit due to the preemption. It is also possible that a different pod will schedule
		// into the resources that were preempted, but this is harmless.
		// schedulePod可能失败了，因为pod不适合任何宿主机，所以我们尝试抢占，期望下次尝试调度pod时，由于抢占，它会适合。
		// 也有可能不同的pod会调度到被抢占的资源中，但这是无害的
		var nominatingInfo *framework.NominatingInfo
		if fitError, ok := err.(*framework.FitError); ok {
			// 没有注册PostFilter插件，因此不会执行抢占
			if !fwk.HasPostFilterPlugins() {
				klog.V(3).InfoS("No PostFilter plugins are registered, so no preemption will be performed")
			} else {
				// Run PostFilter plugins to try to make the pod schedulable in a future scheduling cycle.
				// 运行PostFilter插件以尝试使pod在未来的调度周期中可调度
				result, status := fwk.RunPostFilterPlugins(ctx, state, pod, fitError.Diagnosis.NodeToStatusMap)
				if status.Code() == framework.Error {
					klog.ErrorS(nil, "Status after running PostFilter plugins for pod", "pod", klog.KObj(pod), "status", status)
				} else {
					fitError.Diagnosis.PostFilterMsg = status.Message()
					klog.V(5).InfoS("Status after running PostFilter plugins for pod", "pod", klog.KObj(pod), "status", status)
				}
				if result != nil {
					nominatingInfo = result.NominatingInfo
				}
			}
			// Pod did not fit anywhere, so it is counted as a failure. If preemption
			// succeeds, the pod should get counted as a success the next time we try to
			// schedule it. (hopefully)
			// Pod不适合任何地方，因此被视为失败。如果抢占成功，则在我们下次尝试安排它时，Pod应该被视为成功。（希望如此）
			metrics.PodUnschedulable(fwk.ProfileName(), metrics.SinceInSeconds(start))
		} else if err == ErrNoNodesAvailable {
			nominatingInfo = clearNominatedNode
			// No nodes available is counted as unschedulable rather than an error.
			// 没有可用的节点被视为不可调度而不是错误
			metrics.PodUnschedulable(fwk.ProfileName(), metrics.SinceInSeconds(start))
		} else {
			nominatingInfo = clearNominatedNode
			klog.ErrorS(err, "Error selecting node for pod", "pod", klog.KObj(pod))
			metrics.PodScheduleError(fwk.ProfileName(), metrics.SinceInSeconds(start))
		}
		sched.handleSchedulingFailure(ctx, fwk, podInfo, err, v1.PodReasonUnschedulable, nominatingInfo)
		return
	}
	metrics.SchedulingAlgorithmLatency.Observe(metrics.SinceInSeconds(start))
	// Tell the cache to assume that a pod now is running on a given node, even though it hasn't been bound yet.
	// This allows us to keep scheduling without waiting on binding to occur.
	// 假性绑定/内存绑定：告诉缓存假设一个pod现在正在给定节点上运行，即使它还没有被绑定。
	// 这允许我们保持调度，而无需等待绑定发生，即调度和绑定是异步的
	assumedPodInfo := podInfo.DeepCopy()
	assumedPod := assumedPodInfo.Pod
	// assume modifies `assumedPod` by setting NodeName=scheduleResult.SuggestedHost
	// 5.将目标宿主机节点与Pod在内存中进行假性绑定，并更新scheduler缓存
	// 实现方式：将Pod中的assumed.Spec.NodeName置为了该主机名scheduleResult.SuggestedHost
	err = sched.assume(assumedPod, scheduleResult.SuggestedHost)
	if err != nil {
		metrics.PodScheduleError(fwk.ProfileName(), metrics.SinceInSeconds(start))
		// This is most probably result of a BUG in retrying logic.
		// We report an error here so that pod scheduling can be retried.
		// This relies on the fact that Error will check if the pod has been bound
		// to a node and if so will not add it back to the unscheduled pods queue
		// (otherwise this would cause an infinite loop).
		// 这很可能是重试逻辑中的BUG的结果。
		// 我们在这里报告一个错误，以便可以重试pod调度。
		// 这依赖于这样一个事实，即Error将检查pod是否已绑定到节点，如果是，则不会将其添加回未调度的pod队列（否则将导致无限循环）
		sched.handleSchedulingFailure(ctx, fwk, assumedPodInfo, err, SchedulerError, clearNominatedNode)
		return
	}

	// Run the Reserve method of reserve plugins.
	// 6.内存资源视图扣减，运行预留的插件中的预留方法
	if sts := fwk.RunReservePluginsReserve(schedulingCycleCtx, state, assumedPod, scheduleResult.SuggestedHost); !sts.IsSuccess() {
		metrics.PodScheduleError(fwk.ProfileName(), metrics.SinceInSeconds(start))
		// trigger un-reserve to clean up state associated with the reserved Pod
		// 一旦出现问题，就触发回滚操作，将内存资源视图回滚到假性绑定前的状态，以清理与保留Pod关联的状态
		fwk.RunReservePluginsUnreserve(schedulingCycleCtx, state, assumedPod, scheduleResult.SuggestedHost)
		if forgetErr := sched.Cache.ForgetPod(assumedPod); forgetErr != nil {
			klog.ErrorS(forgetErr, "Scheduler cache ForgetPod failed")
		}
		sched.handleSchedulingFailure(ctx, fwk, assumedPodInfo, sts.AsError(), SchedulerError, clearNominatedNode)
		return
	}

	// Run "permit" plugins.
	// 7.运行钩子插件，对假性绑定关系进行检查过滤
	runPermitStatus := fwk.RunPermitPlugins(schedulingCycleCtx, state, assumedPod, scheduleResult.SuggestedHost)
	if !runPermitStatus.IsWait() && !runPermitStatus.IsSuccess() {
		var reason string
		if runPermitStatus.IsUnschedulable() {
			metrics.PodUnschedulable(fwk.ProfileName(), metrics.SinceInSeconds(start))
			reason = v1.PodReasonUnschedulable
		} else {
			metrics.PodScheduleError(fwk.ProfileName(), metrics.SinceInSeconds(start))
			reason = SchedulerError
		}
		// One of the plugins returned status different than success or wait.
		// 其中一个插件返回的状态与success或wait不同
		fwk.RunReservePluginsUnreserve(schedulingCycleCtx, state, assumedPod, scheduleResult.SuggestedHost)
		if forgetErr := sched.Cache.ForgetPod(assumedPod); forgetErr != nil {
			klog.ErrorS(forgetErr, "Scheduler cache ForgetPod failed")
		}
		sched.handleSchedulingFailure(ctx, fwk, assumedPodInfo, runPermitStatus.AsError(), reason, clearNominatedNode)
		return
	}

	// At the end of a successful scheduling cycle, pop and move up Pods if needed.
	// 8.更新Pod队列
	if len(podsToActivate.Map) != 0 {
		sched.SchedulingQueue.Activate(podsToActivate.Map)
		// Clear the entries after activation.
		// 激活后清除条目
		podsToActivate.Map = make(map[string]*v1.Pod)
	}

	// bind the pod to its host asynchronously (we can do this b/c of the assumption step above).
	// 将pod异步绑定到它的主机（我们可以执行上面假设步骤的b/c）。
	// 9.开启一个goroutine来异步执行Pod和宿主机节点真正的绑定工作
	go func() {
		// goroutine的一些上下文准备工作
		bindingCycleCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		metrics.SchedulerGoroutines.WithLabelValues(metrics.Binding).Inc()
		defer metrics.SchedulerGoroutines.WithLabelValues(metrics.Binding).Dec()
		// 执行WaitOnPermit
		waitOnPermitStatus := fwk.WaitOnPermit(bindingCycleCtx, assumedPod)
		if !waitOnPermitStatus.IsSuccess() {
			var reason string
			if waitOnPermitStatus.IsUnschedulable() {
				metrics.PodUnschedulable(fwk.ProfileName(), metrics.SinceInSeconds(start))
				reason = v1.PodReasonUnschedulable
			} else {
				metrics.PodScheduleError(fwk.ProfileName(), metrics.SinceInSeconds(start))
				reason = SchedulerError
			}
			// trigger un-reserve plugins to clean up state associated with the reserved Pod
			// 触发取消保留插件以清理与保留Pod关联的状态
			fwk.RunReservePluginsUnreserve(bindingCycleCtx, state, assumedPod, scheduleResult.SuggestedHost)
			if forgetErr := sched.Cache.ForgetPod(assumedPod); forgetErr != nil {
				klog.ErrorS(forgetErr, "scheduler cache ForgetPod failed")
			} else {
				// "Forget"ing an assumed Pod in binding cycle should be treated as a PodDelete event,
				// as the assumed Pod had occupied a certain amount of resources in scheduler cache.
				// TODO(#103853): de-duplicate the logic.
				// Avoid moving the assumed Pod itself as it's always Unschedulable.
				// It's intentional to "defer" this operation; otherwise MoveAllToActiveOrBackoffQueue() would
				// update `q.moveRequest` and thus move the assumed pod to backoffQ anyways.
				// “忘记”在绑定周期中读取假设的Pod应被视为PodDelete事件，因为假设的Pod在调度程序缓存中占用了一定数量的资源。
				// 待办事项（#103853）：删除重复逻辑。
				// 避免移动假定的Pod本身，因为它总是不可调度的。
				// 有意“推迟”此操作；否则MoveAllToActiveOrBackoffQueue（）会更新“q.move请求”，从而将假定的pod移动到backoffQ。
				defer sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(internalqueue.AssignedPodDelete, func(pod *v1.Pod) bool {
					return assumedPod.UID != pod.UID
				})
			}
			sched.handleSchedulingFailure(ctx, fwk, assumedPodInfo, waitOnPermitStatus.AsError(), reason, clearNominatedNode)
			return
		}

		// Run "prebind" plugins.
		// 9.1 执行PreBind插件
		preBindStatus := fwk.RunPreBindPlugins(bindingCycleCtx, state, assumedPod, scheduleResult.SuggestedHost)
		if !preBindStatus.IsSuccess() {
			metrics.PodScheduleError(fwk.ProfileName(), metrics.SinceInSeconds(start))
			// trigger un-reserve plugins to clean up state associated with the reserved Pod
			fwk.RunReservePluginsUnreserve(bindingCycleCtx, state, assumedPod, scheduleResult.SuggestedHost)
			if forgetErr := sched.Cache.ForgetPod(assumedPod); forgetErr != nil {
				klog.ErrorS(forgetErr, "scheduler cache ForgetPod failed")
			} else {
				// "Forget"ing an assumed Pod in binding cycle should be treated as a PodDelete event,
				// as the assumed Pod had occupied a certain amount of resources in scheduler cache.
				// TODO(#103853): de-duplicate the logic.
				sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(internalqueue.AssignedPodDelete, nil)
			}
			sched.handleSchedulingFailure(ctx, fwk, assumedPodInfo, preBindStatus.AsError(), SchedulerError, clearNominatedNode)
			return
		}
		// 9.2 运行Bind插件及相关的拓展插件
		err := sched.bind(bindingCycleCtx, fwk, assumedPod, scheduleResult.SuggestedHost, state)
		if err != nil {
			// 错误处理，清除状态并重试
			metrics.PodScheduleError(fwk.ProfileName(), metrics.SinceInSeconds(start))
			// trigger un-reserve plugins to clean up state associated with the reserved Pod
			fwk.RunReservePluginsUnreserve(bindingCycleCtx, state, assumedPod, scheduleResult.SuggestedHost)
			if err := sched.Cache.ForgetPod(assumedPod); err != nil {
				klog.ErrorS(err, "scheduler cache ForgetPod failed")
			} else {
				// "Forget"ing an assumed Pod in binding cycle should be treated as a PodDelete event,
				// as the assumed Pod had occupied a certain amount of resources in scheduler cache.
				// TODO(#103853): de-duplicate the logic.
				sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(internalqueue.AssignedPodDelete, nil)
			}
			sched.handleSchedulingFailure(ctx, fwk, assumedPodInfo, fmt.Errorf("binding rejected: %w", err), SchedulerError, clearNominatedNode)
			return
		}
		// Calculating nodeResourceString can be heavy. Avoid it if klog verbosity is below 2.
		// 计算nodeResourceString可能很重。如果klog冗长度低于2，请避免使用它。
		klog.V(2).InfoS("Successfully bound pod to node", "pod", klog.KObj(pod), "node", scheduleResult.SuggestedHost, "evaluatedNodes", scheduleResult.EvaluatedNodes, "feasibleNodes", scheduleResult.FeasibleNodes)
		metrics.PodScheduled(fwk.ProfileName(), metrics.SinceInSeconds(start))
		metrics.PodSchedulingAttempts.Observe(float64(podInfo.Attempts))
		metrics.PodSchedulingDuration.WithLabelValues(getAttemptsLabel(podInfo)).Observe(metrics.SinceInSeconds(podInfo.InitialAttemptTimestamp))

		// Run "postbind" plugins.
		// 9.3 执行PostBind插件
		fwk.RunPostBindPlugins(bindingCycleCtx, state, assumedPod, scheduleResult.SuggestedHost)

		// At the end of a successful binding cycle, move up Pods if needed.
		// 在成功的绑定周期结束时，如果需要，向上移动Pods
		if len(podsToActivate.Map) != 0 {
			sched.SchedulingQueue.Activate(podsToActivate.Map)
			// Unlike the logic in scheduling cycle, we don't bother deleting the entries
			// as `podsToActivate.Map` is no longer consumed.
			// 与调度周期中的逻辑不同，我们不会费心删除“podsToActivate. Map”中的条目。Map不再被消耗。
		}
	}()
}

// frameworkForPod
//
//	@Description:
//		获取Pod想要使用的具体的调度器插件
//	@receiver sched *Scheduler 对象
//	@param pod 待调度的Pod
//	@return framework.Framework 返回一个具体的framework对象，贯穿调度全文
//	@return error 错误信息
func (sched *Scheduler) frameworkForPod(pod *v1.Pod) (framework.Framework, error) {
	// pod.Spec.SchedulerName对应的就是我们定义yaml文件中的spec下schedulerName参数
	// 但是一般使用情况不会去定义，所以是默认的default-scheduler
	// 我们可以为不同pod定义不同的调度器达到更适合的效果
	// 根据yaml文件中的不同的字符串类型的键，获取该键对应的特定类型的framework.Framework接口类型的值，该值实则是一个frameworkImpl结构体对象
	fwk, ok := sched.Profiles[pod.Spec.SchedulerName]
	if !ok {
		return nil, fmt.Errorf("profile not found for scheduler name %q", pod.Spec.SchedulerName)
	}
	return fwk, nil
}

// skipPodSchedule returns true if we could skip scheduling the pod for specified cases.
// 如果我们可以跳过为指定情况调度pod，则返回true，即跳过特殊的Pod
func (sched *Scheduler) skipPodSchedule(fwk framework.Framework, pod *v1.Pod) bool {
	// Case 1: pod is being deleted.
	// 1.第一种情况：Pod被标记要删除
	if pod.DeletionTimestamp != nil {
		fwk.EventRecorder().Eventf(pod, nil, v1.EventTypeWarning, "FailedScheduling", "Scheduling", "skip schedule deleting pod: %v/%v", pod.Namespace, pod.Name)
		klog.V(3).InfoS("Skip schedule deleting pod", "pod", klog.KObj(pod))
		return true
	}

	// Case 2: pod that has been assumed could be skipped.
	// An assumed pod can be added again to the scheduling queue if it got an update event
	// during its previous scheduling cycle but before getting assumed.
	// 假设可以跳过的Pod
	// 假设跳过的pod可以再次添加到调度队列中，如果它在其上一个调度周期期间跳过，但在获得假设之前收到更新事件。
	// 2.第二种情况：pod在被调度前信息又被变更了，则将其跳过此次调度，放到调度队列中参加下一次调度
	isAssumed, err := sched.Cache.IsAssumedPod(pod)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to check whether pod %s/%s is assumed: %v", pod.Namespace, pod.Name, err))
		return false
	}
	return isAssumed
}

// schedulePod tries to schedule the given pod to one of the nodes in the node list.
// If it succeeds, it will return the name of the node.
// If it fails, it will return a FitError with reasons.
// 具体的调度函数，尝试将给定pod调度到节点列表中的一个节点。
// 如果成功，它将返回节点的名称。
// 如果失败，它将返回一个带有原因的FitError。

// schedulePod
//
//	@Description:
//		具体的调度函数，包括预选和优选，选出最适合当前Pod的那一个宿主机节点
//	@receiver sched *Scheduler 对象
//	@param ctx 请求上下文
//	@param fwk 为该Pod指定的专属调度器对象
//	@param state 存储该Pod在整个调度过程中的所有状态数据
//	@param pod 待调度Pod
//	@return result 调度结果的那个宿主机
//	@return err 错误信息
func (sched *Scheduler) schedulePod(ctx context.Context, fwk framework.Framework, state *framework.CycleState, pod *v1.Pod) (result ScheduleResult, err error) {
	// 初始化一个可以对操作进行跟踪的对象
	trace := utiltrace.New("Scheduling", utiltrace.Field{Key: "namespace", Value: pod.Namespace}, utiltrace.Field{Key: "name", Value: pod.Name})
	defer trace.LogIfLong(100 * time.Millisecond)
	// 1.更新快照操作，更新当前所有宿主机的信息（如资源等）
	if err := sched.Cache.UpdateSnapshot(sched.nodeInfoSnapshot); err != nil {
		return result, err
	}
	trace.Step("Snapshotting scheduler cache and node infos done")
	// 2.当可用的宿主机节点的数量是0，说明Pod无法调度到任何一个节点上，报错
	if sched.nodeInfoSnapshot.NumNodes() == 0 {
		return result, ErrNoNodesAvailable
	}
	// 3.预选过程 Predicates
	feasibleNodes, diagnosis, err := sched.findNodesThatFitPod(ctx, fwk, state, pod)
	if err != nil {
		return result, err
	}
	trace.Step("Computing predicates done")
	// 如果预选出来的宿主机节点数量为0，说明没有符合条件的宿主机节点，返回FitError错误
	if len(feasibleNodes) == 0 {
		return result, &framework.FitError{
			Pod:         pod,
			NumAllNodes: sched.nodeInfoSnapshot.NumNodes(),
			Diagnosis:   diagnosis,
		}
	}

	// When only one node after predicate, just use it.
	// 当预选过程只返回了1个可用节点，则直接使用该宿主机节点进行开机
	if len(feasibleNodes) == 1 {
		return ScheduleResult{
			SuggestedHost:  feasibleNodes[0].Name,
			EvaluatedNodes: 1 + len(diagnosis.NodeToStatusMap),
			FeasibleNodes:  1,
		}, nil
	}
	// 4.优选过程 Priorities，即对预选出来的宿主机节点进行排序
	priorityList, err := prioritizeNodes(ctx, sched.Extenders, fwk, state, pod, feasibleNodes)
	if err != nil {
		return result, err
	}
	// 5.选择节点，selectHost获取节点的优先级列表，然后以水库采样方式从得分最高的节点中选择一个，每个Node有1/n的概率成最优Node
	host, err := selectHost(priorityList)
	trace.Step("Prioritizing done")
	// 6.将结果返回
	return ScheduleResult{
		SuggestedHost:  host,
		EvaluatedNodes: len(feasibleNodes) + len(diagnosis.NodeToStatusMap),
		FeasibleNodes:  len(feasibleNodes),
	}, err
}

// Filters the nodes to find the ones that fit the pod based on the framework
// filter plugins and filter extenders.
// 预选Predicates，根据框架过滤器插件和过滤器扩展器过滤节点以找到适合pod的宿主机节点

// findNodesThatFitPod
//
//	@Description:
//		预选过程，选出所有可以满足开机条件的宿主机节点
//	@receiver sched *Scheduler 对象
//	@param ctx 请求上下文
//	@param fwk 该Pod所使用的专属调度器
//	@param state 存储该Pod在整个调度过程中的所有状态数据
//	@param pod 待调度的Pod
//	@return []*v1.Node 返回获取到的通过预选过滤条件的宿主机节点列表
//	@return framework.Diagnosis 返回调度器诊断信息
//	@return error 返回错误信息
func (sched *Scheduler) findNodesThatFitPod(ctx context.Context, fwk framework.Framework, state *framework.CycleState, pod *v1.Pod) ([]*v1.Node, framework.Diagnosis, error) {
	// 诊断，记录详细信息以诊断调度失败
	diagnosis := framework.Diagnosis{
		NodeToStatusMap:      make(framework.NodeToStatusMap),
		UnschedulablePlugins: sets.NewString(),
	}
	// 1.获取所有的宿主机列表
	allNodes, err := sched.nodeInfoSnapshot.NodeInfos().List()
	if err != nil {
		return nil, diagnosis, err
	}
	// Run "prefilter" plugins.
	// 2.根据请求中的pod参数，运行PreFilter，来获取初步满足条件的节点列表
	// --->pkg\scheduler\framework\runtime\framework.go
	preRes, s := fwk.RunPreFilterPlugins(ctx, state, pod)
	if !s.IsSuccess() {
		if !s.IsUnschedulable() {
			return nil, diagnosis, s.AsError()
		}
		// All nodes will have the same status. Some non trivial refactoring is
		// needed to avoid this copy.
		// 所有节点都将具有相同的状态，需要一些重要的重构来避免这种复制
		for _, n := range allNodes {
			diagnosis.NodeToStatusMap[n.Node().Name] = s
		}
		// Status satisfying IsUnschedulable() gets injected into diagnosis.UnschedulablePlugins.
		// 满足IsUnschedulable()的状态被注入诊断。不可调度插件
		if s.FailedPlugin() != "" {
			diagnosis.UnschedulablePlugins.Insert(s.FailedPlugin())
		}
		return nil, diagnosis, nil
	}

	// "NominatedNodeName" can potentially be set in a previous scheduling cycle as a result of preemption.
	// This node is likely the only candidate that will fit the pod, and hence we try it first before iterating over all nodes.
	// 由于抢占，“NominatedNodeName”可能会在之前的调度周期中设置。该节点可能是唯一适合pod的候选节点，因此我们在迭代所有节点之前先尝试它
	// 3.评估Pod中指定的提名节点是否符合要求（类似于请求中指定的，如force之类的节点）
	// 这些提名宿主机节点可能是一开始就指定的，也可能是在上一轮调度中指定的，所以这些节点是最可能满足条件的，因此直接检查这些宿主机节是否符合条件即可
	if len(pod.Status.NominatedNodeName) > 0 {
		// 3.1 评估指定提名节点
		feasibleNodes, err := sched.evaluateNominatedNode(ctx, pod, fwk, state, diagnosis)
		// 3.2 如果连指定的这些宿主机节点都不符合，则直接记录错误信息
		if err != nil {
			klog.ErrorS(err, "Evaluation failed on nominated node", "pod", klog.KObj(pod), "node", pod.Status.NominatedNodeName)
		}
		// Nominated node passes all the filters, scheduler is good to assign this node to the pod.
		// 指定节点通过所有过滤器，调度程序很好地将此节点分配给pod
		// 3.3 如果指定节点中有能满足的，就直接返回，不用寻找全部节点了
		if len(feasibleNodes) != 0 {
			return feasibleNodes, diagnosis, nil
		}
	}
	// 4.遍历初步满足条件的宿主机列表，将其加入到一个切片中
	nodes := allNodes
	if !preRes.AllNodes() {
		nodes = make([]*framework.NodeInfo, 0, len(preRes.NodeNames))
		for n := range preRes.NodeNames {
			nInfo, err := sched.nodeInfoSnapshot.NodeInfos().Get(n)
			if err != nil {
				return nil, diagnosis, err
			}
			nodes = append(nodes, nInfo)
		}
	}
	// 5.对所有的宿主机节点执行预选插件
	feasibleNodes, err := sched.findNodesThatPassFilters(ctx, fwk, state, pod, diagnosis, nodes)
	if err != nil {
		return nil, diagnosis, err
	}
	// 6.对所有的宿主机节点执行扩展预选过滤插件,留给用户自定义的外部扩展插件
	feasibleNodes, err = findNodesThatPassExtenders(sched.Extenders, pod, feasibleNodes, diagnosis.NodeToStatusMap)
	if err != nil {
		return nil, diagnosis, err
	}
	// 6.将所有通过预选过滤的，可以满足开机条件的宿主机节点、诊断信息、错误信息返回
	return feasibleNodes, diagnosis, nil
}

// evaluateNominatedNode
//
//	@Description:
//		对Pod指定提名节点的评估流程和方法
//	@receiver sched *Scheduler 对象
//	@param ctx 请求上下文
//	@param pod 待调度Pod
//	@param fwk 该Pod使用的专属调度器
//	@param state 存储该Pod在整个调度过程中的所有状态数据
//	@param diagnosis 诊断信息
//	@return []*v1.Node 返回通过的宿主机列表
//	@return error 错误信息
func (sched *Scheduler) evaluateNominatedNode(ctx context.Context, pod *v1.Pod, fwk framework.Framework, state *framework.CycleState, diagnosis framework.Diagnosis) ([]*v1.Node, error) {
	nnn := pod.Status.NominatedNodeName
	// 1.获取宿主机中的评估提名节点
	nodeInfo, err := sched.nodeInfoSnapshot.Get(nnn)
	if err != nil {
		return nil, err
	}
	node := []*framework.NodeInfo{nodeInfo}
	// 2.执行预选的插件
	feasibleNodes, err := sched.findNodesThatPassFilters(ctx, fwk, state, pod, diagnosis, node)
	if err != nil {
		return nil, err
	}
	// 3.执行扩展的预选插件
	feasibleNodes, err = findNodesThatPassExtenders(sched.Extenders, pod, feasibleNodes, diagnosis.NodeToStatusMap)
	if err != nil {
		return nil, err
	}
	// 4.返回Pod指定节点列表中，满足了开机条件的宿主机节点
	return feasibleNodes, nil
}

// findNodesThatPassFilters finds the nodes that fit the filter plugins.
// 查找适合过滤器插件的节点

// findNodesThatPassFilters
// @Description:
//
//	基础预选，获取通过filter扩展点的宿主机节点，即通过filter扩展点的所有过滤器插件的宿主机节点
//
// @receiver sched *Scheduler 对象
// @param ctx 上下文对象
// @param fwk 插件模板
// @param state 当前参加调度的Pod的调度状态信息
// @param pod 当前正在参加调度的Pod
// @param diagnosis 错误诊断信息
// @param nodes 参与筛选的宿主机节点信息列表
// @return []*v1.Node 返回所有通过filter扩展点插件的宿主机节点
// @return error 错误信息。
func (sched *Scheduler) findNodesThatPassFilters(
	ctx context.Context,
	fwk framework.Framework,
	state *framework.CycleState,
	pod *v1.Pod,
	diagnosis framework.Diagnosis,
	nodes []*framework.NodeInfo) ([]*v1.Node, error) {

	// 1.获取要遍历的宿主机计算节点的数量限制，即本轮参加过滤的节点个数
	numNodesToFind := sched.numFeasibleNodesToFind(int32(len(nodes)))
	// Create feasible list with enough space to avoid growing it
	// and allow assigning.
	// 创建一个存放通过filters扩展点的所有宿主机的切片
	feasibleNodes := make([]*v1.Node, numNodesToFind)
	// 2.如果没有定义任何filter扩展点的插件，说明不过滤，就直接截取所有宿主机node列表中的一段，并返回
	if !fwk.HasFilterPlugins() {
		length := len(nodes)
		for i := range feasibleNodes {
			// 这里面的逻辑我们可以看出scheduler是有一个nextStartNodeIndex标记位的，我们每次调度都会更新这个调度位
			// 下次调度会从该标志位开始向后找，这是因为在大规模集群中节点数量超过了100，但是每次最多选举100个节点进行调度
			// 如果没有标志位那就会一直循环前100个节点，这是不公平也是错误的
			// 2.1 从上一次停止查找的node后面开始截
			feasibleNodes[i] = nodes[(sched.nextStartNodeIndex+i)%length].Node()
		}
		// 2.2 标记下一次调度时，filter的重新查找节点的索引起点，来保证集群中每个宿主机都能公平遍历
		sched.nextStartNodeIndex = (sched.nextStartNodeIndex + len(feasibleNodes)) % length
		// 2.3 将截取到的所有宿主机的一段返回
		return feasibleNodes, nil
	}

	errCh := parallelize.NewErrorChannel()
	var statusesLock sync.Mutex
	var feasibleNodesLen int32
	// 新建一个cancel的context，如果接收到cancel信号，routine退出
	ctx, cancel := context.WithCancel(ctx)
	// 3.checkNode, 执行了一个匿名方法，该方法的目的是针对单个宿主机节点执行filter插件，检查其是否通过
	checkNode := func(i int) {
		// We check the nodes starting from where we left off in the previous scheduling cycle,
		// this is to make sure all nodes have the same chance of being examined across pods.
		// 3.1 从上一个调度周期中中断的地方开始对节点执行filter，这是为了确保所有节点都有相同的机会对pod进行检查
		// 顺序从宿主机列表中取出1个宿主机节点
		nodeInfo := nodes[(sched.nextStartNodeIndex+i)%len(nodes)]
		// 3.2 对该宿主机节点执行所有的Filter插件，看是否满足pod的要求
		status := fwk.RunFilterPluginsWithNominatedPods(ctx, state, pod, nodeInfo)
		// filter插件中任意一个插件报错则直接跳出函数
		if status.Code() == framework.Error {
			errCh.SendErrorWithCancel(status.AsError(), cancel)
			return
		}
		// 3.3 如果这个节点合适，那么就把他放到feasibleNodes列表中
		if status.IsSuccess() {
			// 使用atomic操作是因为该筛选是所有节点一起并行进行的
			// 而atomic可以对多goroutine共享的变量进行线程安全的加减操作
			// 疑问：
			// 		这个+1操作应该是不论过滤成功与否，只要是有一个宿主机节点参加了过滤，都进行+1
			// 		这样才能受numNodesToFind控制，当参加过滤的宿主机节点超过了numNodesToFind限制时，调用cancel退出协程
			// 		因此这一步是否应该调到if status.IsSuccess()语句之上？？
			length := atomic.AddInt32(&feasibleNodesLen, 1)
			// 如果通过的宿主机节点超过了宿主机数量限制，说明个数够了，就取消上下文停止过滤
			if length > numNodesToFind {
				cancel()
				atomic.AddInt32(&feasibleNodesLen, -1)
			} else {
				// 如果数量还没够，则加入结果集
				feasibleNodes[length-1] = nodeInfo.Node()
			}
		} else {
			// 过滤不通过，添加错误诊断信息，将不通过的宿主机节点加入
			statusesLock.Lock()
			diagnosis.NodeToStatusMap[nodeInfo.Node().Name] = status
			diagnosis.UnschedulablePlugins.Insert(status.FailedPlugin())
			statusesLock.Unlock()
		}
	}

	beginCheckNode := time.Now()
	statusCode := framework.Success
	defer func() {
		// We record Filter extension point latency here instead of in framework.go because framework.RunFilterPlugins
		// function is called for each node, whereas we want to have an overall latency for all nodes per scheduling cycle.
		// Note that this latency also includes latency for `addNominatedPods`, which calls framework.RunPreFilterAddPod.
		// 我们在这里记录过滤器扩展点延迟，而不是在framework.go因为框架中。RunFilterPlugins
		// 函数为每个节点调用，而我们希望每个调度周期的所有节点都有一个整体延迟
		// 请注意，此延迟还包括调用框架的“addNominatedPods”的延迟。RunPreFilterAddPod
		metrics.FrameworkExtensionPointDuration.WithLabelValues(frameworkruntime.Filter, statusCode.String(), fwk.ProfileName()).Observe(metrics.SinceInSeconds(beginCheckNode))
	}()

	// Stops searching for more nodes once the configured number of feasible nodes
	// are found.
	// 4.开N个协程执行checkNode（N=feasibleNodes），一旦找到配置的可行节点数量，就停止搜索更多节点
	// 该协程对每个节点的过滤是并行的，即不是一个节点过滤完才过滤下一个节点，而是所有宿主机节点同时参加过滤
	fwk.Parallelizer().Until(ctx, len(nodes), checkNode)
	// 更新标志位，设置下次开始遍历node的位置，跳过通过的节点和未通过的节点
	processedNodes := int(feasibleNodesLen) + len(diagnosis.NodeToStatusMap)
	sched.nextStartNodeIndex = (sched.nextStartNodeIndex + processedNodes) % len(nodes)
	// 取出过滤配置指定数量的、已经通过过滤的宿主机节点
	feasibleNodes = feasibleNodes[:feasibleNodesLen]
	if err := errCh.ReceiveError(); err != nil {
		statusCode = framework.Error
		return nil, err
	}
	// 5.将通过过滤的宿主机节点列表返回
	return feasibleNodes, nil
}

// numFeasibleNodesToFind returns the number of feasible nodes that once found, the scheduler stops
// its search for more feasible nodes.
// 返回找到的可行节点数，调度程序停止搜索更多可行节点

// numFeasibleNodesToFind
// @Description:
//
//	参与调度的计算节点数量限制，即当集群中宿主机规模很大时，遍历集群中百分比数量的宿主机；当集群中宿主机数量较少，则全部遍历
//
// @receiver sched *Scheduler 对象
// @param numAllNodes int32类型，表示集群下的所有宿主机节点的数量
// @return numNodes int32类型，最终要进行遍历的宿主机节点的数量
func (sched *Scheduler) numFeasibleNodesToFind(numAllNodes int32) (numNodes int32) {
	// 1.如果集群中宿主机节点的数量<100，或者集群所有节点参与数量的百分比（如果设置为100，就是所有节点都参与调度，默认为0）>=100
	// 则说明是小规模集群，集群下所有宿主机节点都参与调度
	if numAllNodes < minFeasibleNodesToFind || sched.percentageOfNodesToScore >= 100 {
		return numAllNodes
	}

	// 2.当numAllNodes大于100时，且配置的百分比小于等于0，那么这里需要计算出一个百分比
	// 计算公式：集群所有节点参与数量的百分比 = 50 - (总节点数)/125
	// 该公式的含义为，随着集群内节点变多，那么选取的百分比会逐渐变低，最低到5%
	adaptivePercentage := sched.percentageOfNodesToScore
	// 该值默认为0
	if adaptivePercentage <= 0 {
		basePercentageOfNodesToScore := int32(50)
		adaptivePercentage = basePercentageOfNodesToScore - numAllNodes/125
		// 如果计算出的百分比<配置中最小的百分比，则将当前百分比设置为最小百分比
		if adaptivePercentage < minFeasibleNodesPercentageToFind {
			adaptivePercentage = minFeasibleNodesPercentageToFind
		}
	}
	// 3.算出最终要参与遍历的宿主机节点的数量
	numNodes = numAllNodes * adaptivePercentage / 100
	// 如果最终参与遍历的宿主机节点数量<配置中最小数量限制，则将要返回的数量设置为最小数量限制，即最少有n个宿主机节点参加调度
	if numNodes < minFeasibleNodesToFind {
		return minFeasibleNodesToFind
	}
	// 4.返回集群中最终参与遍历的宿主机节点数量
	return numNodes
}

func findNodesThatPassExtenders(extenders []framework.Extender, pod *v1.Pod, feasibleNodes []*v1.Node, statuses framework.NodeToStatusMap) ([]*v1.Node, error) {
	// Extenders are called sequentially.
	// Nodes in original feasibleNodes can be excluded in one extender, and pass on to the next
	// extender in a decreasing manner.
	for _, extender := range extenders {
		if len(feasibleNodes) == 0 {
			break
		}
		if !extender.IsInterested(pod) {
			continue
		}

		// Status of failed nodes in failedAndUnresolvableMap will be added or overwritten in <statuses>,
		// so that the scheduler framework can respect the UnschedulableAndUnresolvable status for
		// particular nodes, and this may eventually improve preemption efficiency.
		// Note: users are recommended to configure the extenders that may return UnschedulableAndUnresolvable
		// status ahead of others.
		feasibleList, failedMap, failedAndUnresolvableMap, err := extender.Filter(pod, feasibleNodes)
		if err != nil {
			if extender.IsIgnorable() {
				klog.InfoS("Skipping extender as it returned error and has ignorable flag set", "extender", extender, "err", err)
				continue
			}
			return nil, err
		}

		for failedNodeName, failedMsg := range failedAndUnresolvableMap {
			var aggregatedReasons []string
			if _, found := statuses[failedNodeName]; found {
				aggregatedReasons = statuses[failedNodeName].Reasons()
			}
			aggregatedReasons = append(aggregatedReasons, failedMsg)
			statuses[failedNodeName] = framework.NewStatus(framework.UnschedulableAndUnresolvable, aggregatedReasons...)
		}

		for failedNodeName, failedMsg := range failedMap {
			if _, found := failedAndUnresolvableMap[failedNodeName]; found {
				// failedAndUnresolvableMap takes precedence over failedMap
				// note that this only happens if the extender returns the node in both maps
				continue
			}
			if _, found := statuses[failedNodeName]; !found {
				statuses[failedNodeName] = framework.NewStatus(framework.Unschedulable, failedMsg)
			} else {
				statuses[failedNodeName].AppendReason(failedMsg)
			}
		}

		feasibleNodes = feasibleList
	}
	return feasibleNodes, nil
}

// prioritizeNodes prioritizes the nodes by running the score plugins,
// which return a score for each node from the call to RunScorePlugins().
// The scores from each plugin are added together to make the score for that node, then
// any extenders are run as well.
// All scores are finally combined (added) to get the total weighted scores of all nodes
// 通过运行分数插件对节点进行优先级排序，该插件从对RunScorePlugins（）的调用中返回每个节点的分数。
// 每个插件的分数相加以生成该节点的分数，然后也运行任何扩展器。
// 所有分数最终合并（相加）得到所有节点的总加权分数
func prioritizeNodes(ctx context.Context, extenders []framework.Extender, fwk framework.Framework, state *framework.CycleState, pod *v1.Pod, nodes []*v1.Node) (framework.NodeScoreList, error) {
	// If no priority configs are provided, then all nodes will have a score of one.
	// This is required to generate the priority list in the required format
	// 1.如果Pod没有指定任何score插件，则所有节点的得分为1。
	// 这是生成所需格式的优先级列表所必需的
	if len(extenders) == 0 && !fwk.HasScorePlugins() {
		result := make(framework.NodeScoreList, 0, len(nodes))
		for i := range nodes {
			result = append(result, framework.NodeScore{
				Name:  nodes[i].Name,
				Score: 1,
			})
		}
		return result, nil
	}

	// Run PreScore plugins.
	// 2.执行prescore插件
	preScoreStatus := fwk.RunPreScorePlugins(ctx, state, pod, nodes)
	if !preScoreStatus.IsSuccess() {
		return nil, preScoreStatus.AsError()
	}

	// Run the Score plugins.
	// 3.执行score插件对node进行评分
	// scoresMap的类型是map[string][]NodeScore。scoresMap的key是插件名字，value是该插件对所有Node的评分
	scoresMap, scoreStatus := fwk.RunScorePlugins(ctx, state, pod, nodes)
	if !scoreStatus.IsSuccess() {
		return nil, scoreStatus.AsError()
	}

	// Additional details logged at level 10 if enabled.
	// 如果日志等级到10，你可以看到每个插件为每个节点打分的情况
	klogV := klog.V(10)
	if klogV.Enabled() {
		for plugin, nodeScoreList := range scoresMap {
			for _, nodeScore := range nodeScoreList {
				klogV.InfoS("Plugin scored node for pod", "pod", klog.KObj(pod), "plugin", plugin, "node", nodeScore.Name, "score", nodeScore.Score)
			}
		}
	}

	// Summarize all scores.
	// result用于汇总所有分数
	result := make(framework.NodeScoreList, 0, len(nodes))
	// 4.循环scoresMap并取每个节点对应分值最终加和为该节点最终得分
	// 将分数按照node的维度进行汇总，循环执行len(nodes)次
	for i := range nodes {
		// 先在result中塞满所有node的Name，Score初始化为0；
		result = append(result, framework.NodeScore{Name: nodes[i].Name, Score: 0})
		// 执行了多少个scoresMap就有多少个Score，所以这里遍历len(scoresMap)次；
		for j := range scoresMap {
			// 每个算法对应第i个node的结果分值加权后累加；
			result[i].Score += scoresMap[j][i].Score
		}
	}
	// 5.如果有拓展打分插件，还要调用Extender对Node评分并累加到result中
	if len(extenders) != 0 && nodes != nil {
		// 因为要多协程并发调用Extender并统计分数，所以需要锁来互斥写入Node分数
		// 在评分的设置里面，使用了多协程来并发进行评分，在最后分数进行汇总的时候会出现并发写的问题。
		// 为了避免这种现象的出现，k8s的程序中对从prioritizedList里面读取节点名称和分数，然后写入combinedScores的过程中上了互斥锁。
		var mu sync.Mutex
		// 为了记录所有并发读取Extender的协程，这里使用了wait Group这样的数据结构来保证，所有的go routines结束再进行最后的分数累加。
		// 这里存在一个程序性能的问题，所有的线程只要有一个没有运行完毕，程序就会卡在这一步。
		// 即便是多协程并发调用Extender，也会存在木桶效应，即调用时间取决于最慢的Extender。
		// 虽然Extender可能都很快，但是网络延时是一个比较常见的事情，更严重的是如果Extender异常造成调度超时，那么就拖累了整个kube-scheduler的调度效率。这是一个后续需要解决的问题
		var wg sync.WaitGroup
		// combinedScores的key是Node名字，value是Node评分
		combinedScores := make(map[string]int64, len(nodes))
		for i := range extenders {
			// 如果Extender不管理Pod申请的资源则跳过
			if !extenders[i].IsInterested(pod) {
				continue
			}
			// 启动协程调用Extender对所有Node评分。
			wg.Add(1)
			go func(extIndex int) {
				metrics.SchedulerGoroutines.WithLabelValues(metrics.PrioritizingExtender).Inc()
				defer func() {
					metrics.SchedulerGoroutines.WithLabelValues(metrics.PrioritizingExtender).Dec()
					wg.Done()
				}()
				// 拓展插件按权重打分
				prioritizedList, weight, err := extenders[extIndex].Prioritize(pod, nodes)
				if err != nil {
					// Prioritization errors from extender can be ignored, let k8s/other extenders determine the priorities
					klog.V(5).InfoS("Failed to run extender's priority function. No score given by this extender.", "error", err, "pod", klog.KObj(pod), "extender", extenders[extIndex].Name())
					// 扩展器中如果出现了评分的错误，可以忽略，让k8s/其他扩展器确定优先级，而不是想预选阶段那样直接返回报错。
					// 能这样做的原因是，因为评分不同于过滤，对错误不敏感。
					// 过滤如果失败是要返回错误的(如果不能忽略)，因为Node可能无法满足Pod需求；
					// 而评分无非是选择最优的节点，评分错误只会对选择最优有一点影响，但是不会造成故障
					return
				}
				mu.Lock()
				for i := range *prioritizedList {
					host, score := (*prioritizedList)[i].Host, (*prioritizedList)[i].Score
					// Extender的权重是通过Prioritize()返回的，其实该权重是人工配置的，只是通过Prioritize()返回使用上更方便。
					// 合并后的评分是每个Extender对Node评分乘以权重的累加和
					if klogV.Enabled() {
						klogV.InfoS("Extender scored node for pod", "pod", klog.KObj(pod), "extender", extenders[extIndex].Name(), "node", host, "score", score)
					}
					combinedScores[host] += score * weight
				}
				mu.Unlock()
			}(i)
		}
		// wait for all go routines to finish
		// 等待所有的go routines结束，调用时间取决于最慢的Extender
		wg.Wait()
		// 分数的累加，返回结果集priorityList
		// 使用了combinedScores来记录分数，考虑到Extender和Score插件返回的评分的体系会存在出入，所以这边并没有直接累加。
		// 而是后续再进行一次遍历麻将Extender的评分标准化之后才与原先的Score插件评分进行累加。
		for i := range result {
			// MaxExtenderPriority may diverge from the max priority used in the scheduler and defined by MaxNodeScore,
			// therefore we need to scale the score returned by extenders to the score range used by the scheduler.
			// 最终Node的评分是所有ScorePlugin分数总和+所有Extender分数总和
			// 此处标准化了Extender的评分，使其范围与ScorePlugin一致，否则二者没法累加在一起。
			result[i].Score += combinedScores[result[i].Name] * (framework.MaxNodeScore / extenderv1.MaxExtenderPriority)
		}
	}

	if klogV.Enabled() {
		for i := range result {
			klogV.InfoS("Calculated node's final score for pod", "pod", klog.KObj(pod), "node", result[i].Name, "score", result[i].Score)
		}
	}
	return result, nil
}

// selectHost takes a prioritized list of nodes and then picks one
// in a reservoir sampling manner from the nodes that had the highest score.
// 获取节点的优先级列表，然后以水库采样方式从得分最高且相同的节点中选择一个。

// selectHost
//
//	@Description:
//		获取优选结束后的宿主机节点中得分最高的节点
//		如果有多个得分最高的宿主机节点，每个Node有1/n的概率成最优Node
//	@param nodeScoreList 宿主机及其得分的映射列表
//	@return string 选出的宿主机名字
//	@return error 错误信息
func selectHost(nodeScoreList framework.NodeScoreList) (string, error) {
	// 1.没有可行Node的评分，返回错误
	if len(nodeScoreList) == 0 {
		return "", fmt.Errorf("empty priorityList")
	}
	// 在nodeScoreList中找到分数最高的Node，初始化第0个Node分数最高
	maxScore := nodeScoreList[0].Score
	selected := nodeScoreList[0].Name
	// 如果最高分数相同，先统计数量(cntOfMaxScore)
	cntOfMaxScore := 1
	for _, ns := range nodeScoreList[1:] {
		if ns.Score > maxScore {
			maxScore = ns.Score
			selected = ns.Name
			cntOfMaxScore = 1
		} else if ns.Score == maxScore {
			// 分数相同就累计数量
			cntOfMaxScore++
			if rand.Intn(cntOfMaxScore) == 0 {
				// Replace the candidate with probability of 1/cntOfMaxScore
				// 以1/cntOfMaxScore的概率成为最优Node
				selected = ns.Name
			}
		}
	}
	return selected, nil
}

// assume signals to the cache that a pod is already in the cache, so that binding can be asynchronous.
// assume modifies `assumed`.
func (sched *Scheduler) assume(assumed *v1.Pod, host string) error {
	// Optimistically assume that the binding will succeed and send it to apiserver
	// in the background.
	// If the binding fails, scheduler will release resources allocated to assumed pod
	// immediately.
	assumed.Spec.NodeName = host

	if err := sched.Cache.AssumePod(assumed); err != nil {
		klog.ErrorS(err, "Scheduler cache AssumePod failed")
		return err
	}
	// if "assumed" is a nominated pod, we should remove it from internal cache
	if sched.SchedulingQueue != nil {
		sched.SchedulingQueue.DeleteNominatedPodIfExists(assumed)
	}

	return nil
}

// bind binds a pod to a given node defined in a binding object.
// The precedence for binding is: (1) extenders and (2) framework plugins.
// We expect this to run asynchronously, so we handle binding metrics internally.
func (sched *Scheduler) bind(ctx context.Context, fwk framework.Framework, assumed *v1.Pod, targetNode string, state *framework.CycleState) (err error) {
	defer func() {
		sched.finishBinding(fwk, assumed, targetNode, err)
	}()

	bound, err := sched.extendersBinding(assumed, targetNode)
	if bound {
		return err
	}
	bindStatus := fwk.RunBindPlugins(ctx, state, assumed, targetNode)
	if bindStatus.IsSuccess() {
		return nil
	}
	if bindStatus.Code() == framework.Error {
		return bindStatus.AsError()
	}
	return fmt.Errorf("bind status: %s, %v", bindStatus.Code().String(), bindStatus.Message())
}

// TODO(#87159): Move this to a Plugin.
func (sched *Scheduler) extendersBinding(pod *v1.Pod, node string) (bool, error) {
	for _, extender := range sched.Extenders {
		if !extender.IsBinder() || !extender.IsInterested(pod) {
			continue
		}
		return true, extender.Bind(&v1.Binding{
			ObjectMeta: metav1.ObjectMeta{Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID},
			Target:     v1.ObjectReference{Kind: "Node", Name: node},
		})
	}
	return false, nil
}

func (sched *Scheduler) finishBinding(fwk framework.Framework, assumed *v1.Pod, targetNode string, err error) {
	if finErr := sched.Cache.FinishBinding(assumed); finErr != nil {
		klog.ErrorS(finErr, "Scheduler cache FinishBinding failed")
	}
	if err != nil {
		klog.V(1).InfoS("Failed to bind pod", "pod", klog.KObj(assumed))
		return
	}

	fwk.EventRecorder().Eventf(assumed, nil, v1.EventTypeNormal, "Scheduled", "Binding", "Successfully assigned %v/%v to %v", assumed.Namespace, assumed.Name, targetNode)
}

func getAttemptsLabel(p *framework.QueuedPodInfo) string {
	// We breakdown the pod scheduling duration by attempts capped to a limit
	// to avoid ending up with a high cardinality metric.
	if p.Attempts >= 15 {
		return "15+"
	}
	return strconv.Itoa(p.Attempts)
}

// handleSchedulingFailure records an event for the pod that indicates the
// pod has failed to schedule. Also, update the pod condition and nominated node name if set.
func (sched *Scheduler) handleSchedulingFailure(ctx context.Context, fwk framework.Framework, podInfo *framework.QueuedPodInfo, err error, reason string, nominatingInfo *framework.NominatingInfo) {
	sched.Error(podInfo, err)

	// Update the scheduling queue with the nominated pod information. Without
	// this, there would be a race condition between the next scheduling cycle
	// and the time the scheduler receives a Pod Update for the nominated pod.
	// Here we check for nil only for tests.
	if sched.SchedulingQueue != nil {
		sched.SchedulingQueue.AddNominatedPod(podInfo.PodInfo, nominatingInfo)
	}

	pod := podInfo.Pod
	msg := truncateMessage(err.Error())
	fwk.EventRecorder().Eventf(pod, nil, v1.EventTypeWarning, "FailedScheduling", "Scheduling", msg)
	if err := updatePod(ctx, sched.client, pod, &v1.PodCondition{
		Type:    v1.PodScheduled,
		Status:  v1.ConditionFalse,
		Reason:  reason,
		Message: err.Error(),
	}, nominatingInfo); err != nil {
		klog.ErrorS(err, "Error updating pod", "pod", klog.KObj(pod))
	}
}

// truncateMessage truncates a message if it hits the NoteLengthLimit.
func truncateMessage(message string) string {
	max := validation.NoteLengthLimit
	if len(message) <= max {
		return message
	}
	suffix := " ..."
	return message[:max-len(suffix)] + suffix
}

func updatePod(ctx context.Context, client clientset.Interface, pod *v1.Pod, condition *v1.PodCondition, nominatingInfo *framework.NominatingInfo) error {
	klog.V(3).InfoS("Updating pod condition", "pod", klog.KObj(pod), "conditionType", condition.Type, "conditionStatus", condition.Status, "conditionReason", condition.Reason)
	podStatusCopy := pod.Status.DeepCopy()
	// NominatedNodeName is updated only if we are trying to set it, and the value is
	// different from the existing one.
	nnnNeedsUpdate := nominatingInfo.Mode() == framework.ModeOverride && pod.Status.NominatedNodeName != nominatingInfo.NominatedNodeName
	if !podutil.UpdatePodCondition(podStatusCopy, condition) && !nnnNeedsUpdate {
		return nil
	}
	if nnnNeedsUpdate {
		podStatusCopy.NominatedNodeName = nominatingInfo.NominatedNodeName
	}
	return util.PatchPodStatus(ctx, client, pod, podStatusCopy)
}
