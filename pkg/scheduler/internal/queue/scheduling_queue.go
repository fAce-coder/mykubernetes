/*
Copyright 2017 The Kubernetes Authors.

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

// This file contains structures that implement scheduling queue types.
// Scheduling queues hold pods waiting to be scheduled. This file implements a
// priority queue which has two sub queues and a additional data structure,
// namely: activeQ, backoffQ and unschedulablePods.
// - activeQ holds pods that are being considered for scheduling.
// - backoffQ holds pods that moved from unschedulablePods and will move to
//   activeQ when their backoff periods complete.
// - unschedulablePods holds pods that were already attempted for scheduling and
//   are currently determined to be unschedulable.

package queue

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/interpodaffinity"
	"k8s.io/kubernetes/pkg/scheduler/internal/heap"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	"k8s.io/kubernetes/pkg/scheduler/util"
	"k8s.io/utils/clock"
)

const (
	// DefaultPodMaxInUnschedulablePodsDuration is the default value for the maximum
	// time a pod can stay in unschedulablePods. If a pod stays in unschedulablePods
	// for longer than this value, the pod will be moved from unschedulablePods to
	// backoffQ or activeQ. If this value is empty, the default value (5min)
	// will be used.
	// 是pod可以在unschedulableQ中停留的最长时间的默认值。
	// 如果pod在非调度Pods中停留的时间超过此值，则pod将从unschedulableQ移动到backoffQ或activeQ。
	// 如果此值为空，则将使用默认值（5min）。
	DefaultPodMaxInUnschedulablePodsDuration time.Duration = 5 * time.Minute

	queueClosed = "scheduling queue is closed"
)

const (
	// DefaultPodInitialBackoffDuration is the default value for the initial backoff duration
	// for unschedulable pods. To change the default podInitialBackoffDurationSeconds used by the
	// scheduler, update the ComponentConfig value in defaults.go
	// 是不可调度pod的初始退避持续时间的默认值。
	// 要更改调度程序使用的默认podFirst alBackoffDuration秒，请更新defaults.go中的ComponentConfig值
	// 1.backoffQ中Pod的初始退避时间
	DefaultPodInitialBackoffDuration time.Duration = 1 * time.Second
	// DefaultPodMaxBackoffDuration is the default value for the max backoff duration
	// for unschedulable pods. To change the default podMaxBackoffDurationSeconds used by the
	// scheduler, update the ComponentConfig value in defaults.go
	// 是不可调度pod的最大退避持续时间的默认值。
	// 要更改调度程序使用的默认podMaxBackoffDuration秒，请更新defaults.go中的ComponentConfig值
	// 2.backoffQ中Pod的最大退避时间
	DefaultPodMaxBackoffDuration time.Duration = 10 * time.Second
)

// PreEnqueueCheck is a function type. It's used to build functions that
// run against a Pod and the caller can choose to enqueue or skip the Pod
// by the checking result.
type PreEnqueueCheck func(pod *v1.Pod) bool

// SchedulingQueue is an interface for a queue to store pods waiting to be scheduled.
// The interface follows a pattern similar to cache.FIFO and cache.Heap and
// makes it easy to use those data structures as a SchedulingQueue.
type SchedulingQueue interface {
	framework.PodNominator
	Add(pod *v1.Pod) error
	// Activate moves the given pods to activeQ iff they're in unschedulablePods or backoffQ.
	// The passed-in pods are originally compiled from plugins that want to activate Pods,
	// by injecting the pods through a reserved CycleState struct (PodsToActivate).
	Activate(pods map[string]*v1.Pod)
	// AddUnschedulableIfNotPresent adds an unschedulable pod back to scheduling queue.
	// The podSchedulingCycle represents the current scheduling cycle number which can be
	// returned by calling SchedulingCycle().
	AddUnschedulableIfNotPresent(pod *framework.QueuedPodInfo, podSchedulingCycle int64) error
	// SchedulingCycle returns the current number of scheduling cycle which is
	// cached by scheduling queue. Normally, incrementing this number whenever
	// a pod is popped (e.g. called Pop()) is enough.
	SchedulingCycle() int64
	// Pop removes the head of the queue and returns it. It blocks if the
	// queue is empty and waits until a new item is added to the queue.
	Pop() (*framework.QueuedPodInfo, error)
	Update(oldPod, newPod *v1.Pod) error
	Delete(pod *v1.Pod) error
	MoveAllToActiveOrBackoffQueue(event framework.ClusterEvent, preCheck PreEnqueueCheck)
	AssignedPodAdded(pod *v1.Pod)
	AssignedPodUpdated(pod *v1.Pod)
	PendingPods() []*v1.Pod
	// Close closes the SchedulingQueue so that the goroutine which is
	// waiting to pop items can exit gracefully.
	Close()
	// Run starts the goroutines managing the queue.
	Run()
}

// NewSchedulingQueue initializes a priority queue as a new scheduling queue.
func NewSchedulingQueue(
	lessFn framework.LessFunc,
	informerFactory informers.SharedInformerFactory,
	opts ...Option) SchedulingQueue {
	return NewPriorityQueue(lessFn, informerFactory, opts...)
}

// NominatedNodeName returns nominated node name of a Pod.
func NominatedNodeName(pod *v1.Pod) string {
	return pod.Status.NominatedNodeName
}

// PriorityQueue implements a scheduling queue.
// The head of PriorityQueue is the highest priority pending pod. This structure
// has two sub queues and a additional data structure, namely: activeQ,
// backoffQ and unschedulablePods.
//   - activeQ holds pods that are being considered for scheduling.
//   - backoffQ holds pods that moved from unschedulablePods and will move to
//     activeQ when their backoff periods complete.
//   - unschedulablePods holds pods that were already attempted for scheduling and
//     are currently determined to be unschedulable.
//
// PriorityQueue实现了一个调度队列。
// PriorityQueue的头部是最高优先级的待处理pod。这种结构有两个子队列和一个额外的数据结构，即：activeQ、backoffQ和untimeulablePods。
// -activeQ保存正在考虑进行调度的pod。
// -backoffQ保存从unschedulablePods移出的pod，并在它们的退避期完成时移动到activeQ。
// -unschedulablePods保存已经尝试进行调度并且当前确定为不可调度的pod。
type PriorityQueue struct {
	// PodNominator abstracts the operations to maintain nominated Pods.
	// PodNominator抽象出维护指定Pod的操作
	framework.PodNominator

	stop  chan struct{}
	clock clock.Clock

	// pod initial backoff duration.
	// pod在backoffQ中的初始重试等待时间
	podInitialBackoffDuration time.Duration
	// pod maximum backoff duration.
	// pod在backoffQ中的最大重试等待时间
	podMaxBackoffDuration time.Duration
	// the maximum time a pod can stay in the unschedulablePods.
	// pod在unschedulableQ中的最大重试等待时间
	podMaxInUnschedulablePodsDuration time.Duration
	// 使用sync中的RWMutex锁（读写锁）作为调度队列的悲观锁机制
	lock sync.RWMutex
	cond sync.Cond

	// activeQ is heap structure that scheduler actively looks at to find pods to
	// schedule. Head of heap is the highest priority pod.
	// activeQ是调度器主动查看以查找要调度的pod的堆结构。堆头是最高优先级的pod。
	activeQ *heap.Heap
	// podBackoffQ is a heap ordered by backoff expiry. Pods which have completed backoff
	// are popped from this heap before the scheduler looks at activeQ
	// podBackoffQ是一个按回退到期排序的堆。在调度程序查看activeQ之前，已完成回退的Pods会从该堆中弹出
	podBackoffQ *heap.Heap
	// unschedulablePods holds pods that have been tried and determined unschedulable.
	// 不可调度Pods持有已尝试并确定不可调度的Pod。
	unschedulablePods *UnschedulablePods
	// schedulingCycle represents sequence number of scheduling cycle and is incremented
	// when a pod is popped.
	// 表示调度周期的序列号，并在弹出pod时递增。
	schedulingCycle int64
	// moveRequestCycle caches the sequence number of scheduling cycle when we
	// received a move request. Unschedulable pods in and before this scheduling
	// cycle will be put back to activeQueue if we were trying to schedule them
	// when we received move request.
	// moveRequestCycle缓存schedulingCycle
	// 当UNschedulableQ中的pod重新被添加到activeQueue中,会保存schedulingCycle到moveRequestCycle中
	moveRequestCycle int64

	clusterEventMap map[framework.ClusterEvent]sets.String

	// closed indicates that the queue is closed.
	// It is mainly used to let Pop() exit its control loop while waiting for an item.
	closed bool

	nsLister listersv1.NamespaceLister
}

type priorityQueueOptions struct {
	clock                             clock.Clock
	podInitialBackoffDuration         time.Duration
	podMaxBackoffDuration             time.Duration
	podMaxInUnschedulablePodsDuration time.Duration
	podNominator                      framework.PodNominator
	clusterEventMap                   map[framework.ClusterEvent]sets.String
}

// Option configures a PriorityQueue
type Option func(*priorityQueueOptions)

// WithClock sets clock for PriorityQueue, the default clock is clock.RealClock.
func WithClock(clock clock.Clock) Option {
	return func(o *priorityQueueOptions) {
		o.clock = clock
	}
}

// WithPodInitialBackoffDuration sets pod initial backoff duration for PriorityQueue.
func WithPodInitialBackoffDuration(duration time.Duration) Option {
	return func(o *priorityQueueOptions) {
		o.podInitialBackoffDuration = duration
	}
}

// WithPodMaxBackoffDuration sets pod max backoff duration for PriorityQueue.
func WithPodMaxBackoffDuration(duration time.Duration) Option {
	return func(o *priorityQueueOptions) {
		o.podMaxBackoffDuration = duration
	}
}

// WithPodNominator sets pod nominator for PriorityQueue.
func WithPodNominator(pn framework.PodNominator) Option {
	return func(o *priorityQueueOptions) {
		o.podNominator = pn
	}
}

// WithClusterEventMap sets clusterEventMap for PriorityQueue.
func WithClusterEventMap(m map[framework.ClusterEvent]sets.String) Option {
	return func(o *priorityQueueOptions) {
		o.clusterEventMap = m
	}
}

// WithPodMaxInUnschedulablePodsDuration sets podMaxInUnschedulablePodsDuration for PriorityQueue.
func WithPodMaxInUnschedulablePodsDuration(duration time.Duration) Option {
	return func(o *priorityQueueOptions) {
		o.podMaxInUnschedulablePodsDuration = duration
	}
}

var defaultPriorityQueueOptions = priorityQueueOptions{
	clock:                             clock.RealClock{},
	podInitialBackoffDuration:         DefaultPodInitialBackoffDuration,
	podMaxBackoffDuration:             DefaultPodMaxBackoffDuration,
	podMaxInUnschedulablePodsDuration: DefaultPodMaxInUnschedulablePodsDuration,
}

// Making sure that PriorityQueue implements SchedulingQueue.
var _ SchedulingQueue = &PriorityQueue{}

// newQueuedPodInfoForLookup builds a QueuedPodInfo object for a lookup in the queue.
func newQueuedPodInfoForLookup(pod *v1.Pod, plugins ...string) *framework.QueuedPodInfo {
	// Since this is only used for a lookup in the queue, we only need to set the Pod,
	// and so we avoid creating a full PodInfo, which is expensive to instantiate frequently.
	return &framework.QueuedPodInfo{
		PodInfo:              &framework.PodInfo{Pod: pod},
		UnschedulablePlugins: sets.NewString(plugins...),
	}
}

// NewPriorityQueue creates a PriorityQueue object.
func NewPriorityQueue(
	lessFn framework.LessFunc,
	informerFactory informers.SharedInformerFactory,
	opts ...Option,
) *PriorityQueue {
	options := defaultPriorityQueueOptions
	for _, opt := range opts {
		opt(&options)
	}

	comp := func(podInfo1, podInfo2 interface{}) bool {
		pInfo1 := podInfo1.(*framework.QueuedPodInfo)
		pInfo2 := podInfo2.(*framework.QueuedPodInfo)
		return lessFn(pInfo1, pInfo2)
	}

	if options.podNominator == nil {
		options.podNominator = NewPodNominator(informerFactory.Core().V1().Pods().Lister())
	}

	pq := &PriorityQueue{
		PodNominator:                      options.podNominator,
		clock:                             options.clock,
		stop:                              make(chan struct{}),
		podInitialBackoffDuration:         options.podInitialBackoffDuration,
		podMaxBackoffDuration:             options.podMaxBackoffDuration,
		podMaxInUnschedulablePodsDuration: options.podMaxInUnschedulablePodsDuration,
		activeQ:                           heap.NewWithRecorder(podInfoKeyFunc, comp, metrics.NewActivePodsRecorder()),
		unschedulablePods:                 newUnschedulablePods(metrics.NewUnschedulablePodsRecorder()),
		moveRequestCycle:                  -1,
		clusterEventMap:                   options.clusterEventMap,
	}
	pq.cond.L = &pq.lock
	pq.podBackoffQ = heap.NewWithRecorder(podInfoKeyFunc, pq.podsCompareBackoffCompleted, metrics.NewBackoffPodsRecorder())
	pq.nsLister = informerFactory.Core().V1().Namespaces().Lister()

	return pq
}

// Run starts the goroutine to pump from podBackoffQ to activeQ
func (p *PriorityQueue) Run() {
	// 1.将backoffQ中所有已完成重试等待时间的pod移动到activeQ
	go wait.Until(p.flushBackoffQCompleted, 1.0*time.Second, p.stop)
	// 2.将unschedulableQ中达到重试等待时间的Pod，移动到activeQ或backoffQ中
	go wait.Until(p.flushUnschedulablePodsLeftover, 30*time.Second, p.stop)
}

// Add adds a pod to the active queue. It should be called only when a new pod
// is added so there is no chance the pod is already in active/unschedulable/backoff queues
// 将pod添加到活动队列中。
// 只有在添加新pod时才应该调用它，这样该pod就不可能已经处于active/unschedulable/backoff队列中

// Add
//
//	@Description:
//		将Pod加入到activeQ中
//	@receiver p *PriorityQueue 对象
//	@param pod 待添加到activeQ的Pod
//	@return error 错误信息
func (p *PriorityQueue) Add(pod *v1.Pod) error {
	// 1.加悲观锁
	p.lock.Lock()
	defer p.lock.Unlock()
	// 2.获取要添加到activeQ的Pod信息
	pInfo := p.newQueuedPodInfo(pod)
	// 3.将该Pod加入到activeQ中
	if err := p.activeQ.Add(pInfo); err != nil {
		klog.ErrorS(err, "Error adding pod to the active queue", "pod", klog.KObj(pod))
		return err
	}
	// 4.如果该Pod已经在unschedulableQ中，则将该Pod从unschedulableQ中删除
	if p.unschedulablePods.get(pod) != nil {
		klog.ErrorS(nil, "Error: pod is already in the unschedulable queue", "pod", klog.KObj(pod))
		p.unschedulablePods.delete(pod)
	}
	// Delete pod from backoffQ if it is backing off
	// 5.如果该Pod已经在podBackoffQ中，则将该Pod从podBackoffQ中删除
	if err := p.podBackoffQ.Delete(pInfo); err == nil {
		klog.ErrorS(nil, "Error: pod is already in the podBackoff queue", "pod", klog.KObj(pod))
	}
	metrics.SchedulerQueueIncomingPods.WithLabelValues("active", PodAdd).Inc()
	// 6.存储Pod和被提名的Node（nil）
	p.PodNominator.AddNominatedPod(pInfo.PodInfo, nil)
	// 往activeQ中添加pod的时候通过cond.Broadcast来实现通知
	p.cond.Broadcast()

	return nil
}

// Activate moves the given pods to activeQ iff they're in unschedulablePods or backoffQ.

// Activate
//
//	@Description:
//		将unschedulableQ或backoffQ中给定的Pod，移动到activeQ中
//	@receiver p *PriorityQueue 对象
//	@param pods 待移动到activeQ中的Pod列表
func (p *PriorityQueue) Activate(pods map[string]*v1.Pod) {
	// 1.加悲观锁
	p.lock.Lock()
	defer p.lock.Unlock()
	// 2.遍历待移动到activeQ的Pods列表，调用activate方法
	activated := false
	for _, pod := range pods {
		if p.activate(pod) {
			activated = true
		}
	}
	// 3.如果移动成功，就通过cond.Broadcast发出广播
	if activated {
		p.cond.Broadcast()
	}
}

// 将unschedulableQ或backoffQ中给定的Pod，移动到activeQ中
func (p *PriorityQueue) activate(pod *v1.Pod) bool {
	// Verify if the pod is present in activeQ.
	// 1.判断该Pod是否已经在activeQ中，如果已经存在，就返回false
	if _, exists, _ := p.activeQ.Get(newQueuedPodInfoForLookup(pod)); exists {
		// No need to activate if it's already present in activeQ.
		return false
	}
	var pInfo *framework.QueuedPodInfo
	// Verify if the pod is present in unschedulablePods or backoffQ.
	// 2.判断该Pod是否在unschedulableQ或backoffQ中
	// 如果Pod不在这两个队列中，就直接返回false
	if pInfo = p.unschedulablePods.get(pod); pInfo == nil {
		// If the pod doesn't belong to unschedulablePods or backoffQ, don't activate it.
		if obj, exists, _ := p.podBackoffQ.Get(newQueuedPodInfoForLookup(pod)); !exists {
			klog.ErrorS(nil, "To-activate pod does not exist in unschedulablePods or backoffQ", "pod", klog.KObj(pod))
			return false
		} else {
			pInfo = obj.(*framework.QueuedPodInfo)
		}
	}

	if pInfo == nil {
		// Redundant safe check. We shouldn't reach here.
		// 多余的安全检查。我们不应该到达这里
		klog.ErrorS(nil, "Internal error: cannot obtain pInfo")
		return false
	}
	// 3.将该Pod加入到activeQ中
	if err := p.activeQ.Add(pInfo); err != nil {
		klog.ErrorS(err, "Error adding pod to the scheduling queue", "pod", klog.KObj(pod))
		return false
	}
	// 4.将该Pod从unschedulableQ和backoffQ中删除
	p.unschedulablePods.delete(pod)
	p.podBackoffQ.Delete(pInfo)
	metrics.SchedulerQueueIncomingPods.WithLabelValues("active", ForceActivate).Inc()
	p.PodNominator.AddNominatedPod(pInfo.PodInfo, nil)
	return true
}

// isPodBackingoff returns true if a pod is still waiting for its backoff timer.
// If this returns true, the pod should not be re-tried.
// 如果pod仍在等待其退避计时器，则返回true。
// 如果返回true，则不应重新尝试pod
func (p *PriorityQueue) isPodBackingoff(podInfo *framework.QueuedPodInfo) bool {
	boTime := p.getBackoffTime(podInfo)
	return boTime.After(p.clock.Now())
}

// SchedulingCycle returns current scheduling cycle.
func (p *PriorityQueue) SchedulingCycle() int64 {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.schedulingCycle
}

// AddUnschedulableIfNotPresent inserts a pod that cannot be scheduled into
// the queue, unless it is already in the queue. Normally, PriorityQueue puts
// unschedulable pods in `unschedulablePods`. But if there has been a recent move
// request, then the pod is put in `podBackoffQ`.
// 将一个不可调度的Pod加入到相关队列中，除非它已经在队列中。
// 通常，PriorityQueue会将不可调度的pod放入unschedulableQ中。但如果最近有移动请求，则将pod放入“podBackoffQ”中。

// AddUnschedulableIfNotPresent
//
//	@Description:
//		将一个调度失败的Pod加入到backoffQ或unschedulableQ中，除非它已经在队列中。
//		通常，PriorityQueue会将不可调度的pod放入unschedulableQ中。但如果最近有资源变更，则将pod放入“podBackoffQ”中
//	@receiver p *PriorityQueue 对象
//	@param pInfo Pod的信息
//	@param podSchedulingCycle 调度队列
//	@return error 错误信息
func (p *PriorityQueue) AddUnschedulableIfNotPresent(pInfo *framework.QueuedPodInfo, podSchedulingCycle int64) error {
	// 1.加悲观锁
	p.lock.Lock()
	defer p.lock.Unlock()
	// 2.获取调度失败的Pod信息
	pod := pInfo.Pod
	// 3.判断该Pod是否已经在activeQ或backoffQ或unschedulableQ中，如果在任何一个队列中存在，则返回错误信息
	if p.unschedulablePods.get(pod) != nil {
		return fmt.Errorf("Pod %v is already present in unschedulable queue", klog.KObj(pod))
	}

	if _, exists, _ := p.activeQ.Get(pInfo); exists {
		return fmt.Errorf("Pod %v is already present in the active queue", klog.KObj(pod))
	}
	if _, exists, _ := p.podBackoffQ.Get(pInfo); exists {
		return fmt.Errorf("Pod %v is already present in the backoff queue", klog.KObj(pod))
	}

	// Refresh the timestamp since the pod is re-added.
	// 重新添加pod后刷新时间戳
	pInfo.Timestamp = p.clock.Now()

	for plugin := range pInfo.UnschedulablePlugins {
		metrics.UnschedulableReason(plugin, pInfo.Pod.Spec.SchedulerName).Inc()
	}
	// If a move request has been received, move it to the BackoffQ, otherwise move
	// it to unschedulablePods.
	// 4.如果资源发生变更，则将该Pod移动到backoffQ，否则移动到unschedulableQ中
	// 4.1 如果moveRequestCycle >= podSchedulingCycle，则说明资源发生变更，则将其移动到backoffQ，进行快重试
	if p.moveRequestCycle >= podSchedulingCycle {
		if err := p.podBackoffQ.Add(pInfo); err != nil {
			return fmt.Errorf("error adding pod %v to the backoff queue: %v", pod.Name, err)
		}
		metrics.SchedulerQueueIncomingPods.WithLabelValues("backoff", ScheduleAttemptFailure).Inc()
	} else {
		// 4.2 否则，说明资源没有发生变更，则将其移动到unschedulableQ中，进行慢重试
		p.unschedulablePods.addOrUpdate(pInfo)
		metrics.SchedulerQueueIncomingPods.WithLabelValues("unschedulable", ScheduleAttemptFailure).Inc()

	}

	p.PodNominator.AddNominatedPod(pInfo.PodInfo, nil)
	return nil
}

// flushBackoffQCompleted Moves all pods from backoffQ which have completed backoff in to activeQ

// flushBackoffQCompleted
//
//	@Description:
//		将backoffQ中所有已完成重试等待时间的pod移动到activeQ
//	@receiver p *PriorityQueue 对象
func (p *PriorityQueue) flushBackoffQCompleted() {
	// 1.加悲观锁
	p.lock.Lock()
	defer p.lock.Unlock()
	activated := false
	for {
		// 2.获取backoffQ堆顶元素，堆顶为空则退出
		rawPodInfo := p.podBackoffQ.Peek()
		if rawPodInfo == nil {
			break
		}
		pod := rawPodInfo.(*framework.QueuedPodInfo).Pod
		// 3.获取该Pod的重试等待时间
		// 因为堆顶的Pod重试等待时间一定是最短的，所以如果堆顶Pod没有达到重试等待时间，其他Pod肯定也没有到达，则退出
		boTime := p.getBackoffTime(rawPodInfo.(*framework.QueuedPodInfo))
		if boTime.After(p.clock.Now()) {
			break
		}
		// 4.将backoffQ堆顶的Pod弹出
		_, err := p.podBackoffQ.Pop()
		if err != nil {
			klog.ErrorS(err, "Unable to pop pod from backoff queue despite backoff completion", "pod", klog.KObj(pod))
			break
		}
		// 5.将backoffQ堆顶的Pod加入到activeQ中
		p.activeQ.Add(rawPodInfo)
		metrics.SchedulerQueueIncomingPods.WithLabelValues("active", BackoffComplete).Inc()
		activated = true
	}

	if activated {
		p.cond.Broadcast()
	}
}

// flushUnschedulablePodsLeftover moves pods which stay in unschedulablePods
// longer than podMaxInUnschedulablePodsDuration to backoffQ or activeQ.

// flushUnschedulablePodsLeftover
//
//	@Description:
//		将unschedulableQ中超过unschedulableQ中最大重试等待时间podMaxInUnschedulablePodsDuration的Pod移动到activeQ或backoffQ中
//	@receiver p *PriorityQueue 对象
func (p *PriorityQueue) flushUnschedulablePodsLeftover() {
	// 1.加悲观锁
	p.lock.Lock()
	defer p.lock.Unlock()

	var podsToMove []*framework.QueuedPodInfo
	currentTime := p.clock.Now()
	// 2.遍历unschedulableQ中的所有Pod，将达到重试等待时间的Pod加入集合中
	for _, pInfo := range p.unschedulablePods.podInfoMap {
		lastScheduleTime := pInfo.Timestamp
		// 如果当前Pod的重试等待时间，超过了在unschedulableQ中的最大重试等待时间，则将其加入集合中
		if currentTime.Sub(lastScheduleTime) > p.podMaxInUnschedulablePodsDuration {
			podsToMove = append(podsToMove, pInfo)
		}
	}
	// 3.调用函数，将超过最大重试等待时间的Pod移动到activeQ或者backoffQ中
	if len(podsToMove) > 0 {
		p.movePodsToActiveOrBackoffQueue(podsToMove, UnschedulableTimeout)
	}
}

// Pop removes the head of the active queue and returns it. It blocks if the
// activeQ is empty and waits until a new item is added to the queue. It
// increments scheduling cycle when a pod is popped.
// 删除activeQ的头部并返回它。
// 如果activeQ为空，它会阻塞并等待直到新项目添加到队列中。
// 弹出pod时，它会增加调度周期。

// Pop
//
//	@Description:
//		弹出堆头的Pod并返回
//	@receiver p *PriorityQueue 对象
//	@return *framework.QueuedPodInfo 弹出的Pod信息
//	@return error 错误信息
func (p *PriorityQueue) Pop() (*framework.QueuedPodInfo, error) {
	// 1.从调度队列activeQ中取Pod使用了悲观锁
	p.lock.Lock()
	defer p.lock.Unlock()
	// 2.activeQ队列为空，就一直阻塞，直到activeQ中加入了新Pod或者activeQ被关闭
	for p.activeQ.Len() == 0 {
		// When the queue is empty, invocation of Pop() is blocked until new item is enqueued.
		// When Close() is called, the p.closed is set and the condition is broadcast,
		// which causes this loop to continue and return from the Pop().
		// 当队列为空时，Pop的调用将被阻塞，直到新Pod入队。
		// 调用Close时，设置p.closed并广播条件，这会导致此循环继续并从Pop返回。

		// activeQ被关闭，退出
		if p.closed {
			return nil, fmt.Errorf(queueClosed)
		}
		// activeQ阻塞
		// 当前队列中没有可调度的pod的时候，则通过cond.Wait来进行阻塞
		p.cond.Wait()
	}
	// 3.从activeQ中取出一个Pod，并获取该Pod信息
	obj, err := p.activeQ.Pop()
	if err != nil {
		return nil, err
	}
	pInfo := obj.(*framework.QueuedPodInfo)
	// 4.将该Pod调度尝试次数+1，将调度周期+1
	pInfo.Attempts++
	// schedulingCycle是一个递增的序列每次从activeQ中pop出一个pod都会递增
	p.schedulingCycle++
	// 5.返回该Pod信息
	return pInfo, nil
}

// isPodUpdated checks if the pod is updated in a way that it may have become
// schedulable. It drops status of the pod and compares it with old version.
func isPodUpdated(oldPod, newPod *v1.Pod) bool {
	strip := func(pod *v1.Pod) *v1.Pod {
		p := pod.DeepCopy()
		p.ResourceVersion = ""
		p.Generation = 0
		p.Status = v1.PodStatus{}
		p.ManagedFields = nil
		p.Finalizers = nil
		return p
	}
	return !reflect.DeepEqual(strip(oldPod), strip(newPod))
}

// Update updates a pod in the active or backoff queue if present. Otherwise, it removes
// the item from the unschedulable queue if pod is updated in a way that it may
// become schedulable and adds the updated one to the active queue.
// If pod is not present in any of the queues, it is added to the active queue.
func (p *PriorityQueue) Update(oldPod, newPod *v1.Pod) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if oldPod != nil {
		oldPodInfo := newQueuedPodInfoForLookup(oldPod)
		// If the pod is already in the active queue, just update it there.
		if oldPodInfo, exists, _ := p.activeQ.Get(oldPodInfo); exists {
			pInfo := updatePod(oldPodInfo, newPod)
			p.PodNominator.UpdateNominatedPod(oldPod, pInfo.PodInfo)
			return p.activeQ.Update(pInfo)
		}

		// If the pod is in the backoff queue, update it there.
		if oldPodInfo, exists, _ := p.podBackoffQ.Get(oldPodInfo); exists {
			pInfo := updatePod(oldPodInfo, newPod)
			p.PodNominator.UpdateNominatedPod(oldPod, pInfo.PodInfo)
			return p.podBackoffQ.Update(pInfo)
		}
	}

	// If the pod is in the unschedulable queue, updating it may make it schedulable.
	if usPodInfo := p.unschedulablePods.get(newPod); usPodInfo != nil {
		pInfo := updatePod(usPodInfo, newPod)
		p.PodNominator.UpdateNominatedPod(oldPod, pInfo.PodInfo)
		if isPodUpdated(oldPod, newPod) {
			if p.isPodBackingoff(usPodInfo) {
				if err := p.podBackoffQ.Add(pInfo); err != nil {
					return err
				}
				p.unschedulablePods.delete(usPodInfo.Pod)
			} else {
				if err := p.activeQ.Add(pInfo); err != nil {
					return err
				}
				p.unschedulablePods.delete(usPodInfo.Pod)
				p.cond.Broadcast()
			}
		} else {
			// Pod update didn't make it schedulable, keep it in the unschedulable queue.
			p.unschedulablePods.addOrUpdate(pInfo)
		}

		return nil
	}
	// If pod is not in any of the queues, we put it in the active queue.
	pInfo := p.newQueuedPodInfo(newPod)
	if err := p.activeQ.Add(pInfo); err != nil {
		return err
	}
	p.PodNominator.AddNominatedPod(pInfo.PodInfo, nil)
	p.cond.Broadcast()
	return nil
}

// Delete deletes the item from either of the two queues. It assumes the pod is
// only in one queue.
func (p *PriorityQueue) Delete(pod *v1.Pod) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.PodNominator.DeleteNominatedPodIfExists(pod)
	if err := p.activeQ.Delete(newQueuedPodInfoForLookup(pod)); err != nil {
		// The item was probably not found in the activeQ.
		p.podBackoffQ.Delete(newQueuedPodInfoForLookup(pod))
		p.unschedulablePods.delete(pod)
	}
	return nil
}

// AssignedPodAdded is called when a bound pod is added. Creation of this pod
// may make pending pods with matching affinity terms schedulable.
// 在添加绑定pod时调用。创建此pod可能会使因匹配亲和力而挂起的pod变成可调度状态
func (p *PriorityQueue) AssignedPodAdded(pod *v1.Pod) {
	p.lock.Lock()
	p.movePodsToActiveOrBackoffQueue(p.getUnschedulablePodsWithMatchingAffinityTerm(pod), AssignedPodAdd)
	p.lock.Unlock()
}

// AssignedPodUpdated is called when a bound pod is updated. Change of labels
// may make pending pods with matching affinity terms schedulable.
// 在更新绑定的pod时调用。更改标签可能会使具有匹配亲和力项的挂起pod可调度
func (p *PriorityQueue) AssignedPodUpdated(pod *v1.Pod) {
	p.lock.Lock()
	p.movePodsToActiveOrBackoffQueue(p.getUnschedulablePodsWithMatchingAffinityTerm(pod), AssignedPodUpdate)
	p.lock.Unlock()
}

// MoveAllToActiveOrBackoffQueue moves all pods from unschedulablePods to activeQ or backoffQ.
// This function adds all pods and then signals the condition variable to ensure that
// if Pop() is waiting for an item, it receives the signal after all the pods are in the
// queue and the head is the highest priority pod.
// 将所有Pod从unschedulableQ移动到activeQ或backoffQ。
// 这个函数添加所有pod，然后向条件变量发出信号，以确保如果Pop（）正在等待一个项目，它在所有pod都在队列中并且头部是最高优先级的pod后接收到信号。
func (p *PriorityQueue) MoveAllToActiveOrBackoffQueue(event framework.ClusterEvent, preCheck PreEnqueueCheck) {
	// 1.加悲观锁
	p.lock.Lock()
	defer p.lock.Unlock()

	unschedulablePods := make([]*framework.QueuedPodInfo, 0, len(p.unschedulablePods.podInfoMap))
	for _, pInfo := range p.unschedulablePods.podInfoMap {
		if preCheck == nil || preCheck(pInfo.Pod) {
			unschedulablePods = append(unschedulablePods, pInfo)
		}
	}
	p.movePodsToActiveOrBackoffQueue(unschedulablePods, event)
}

// NOTE: this function assumes lock has been acquired in caller

// movePodsToActiveOrBackoffQueue
//
//	@Description:
//		当集群资源发生变更时，将unschedulableQ中的Pod，移动到activeQ或者backoffQ中
//	@receiver p *PriorityQueue 对象
//	@param podInfoList 待移动的Pod列表
//	@param event 集群事件
func (p *PriorityQueue) movePodsToActiveOrBackoffQueue(podInfoList []*framework.QueuedPodInfo, event framework.ClusterEvent) {
	activated := false
	for _, pInfo := range podInfoList {
		// If the event doesn't help making the Pod schedulable, continue.
		// Note: we don't run the check if pInfo.UnschedulablePlugins is nil, which denotes
		// either there is some abnormal error, or scheduling the pod failed by plugins other than PreFilter, Filter and Permit.
		// In that case, it's desired to move it anyways.
		// 如果事件无助于使Pod可调度，请继续。
		// 注意：如果pInfo存在，我们不会运行检查。
		// UnschedulablePlugins为nil，表示要么有一些异常错误，要么调度pod被PreFilter、Filter和Permit以外的插件失败。
		// 在这种情况下，无论如何都希望移动它。
		if len(pInfo.UnschedulablePlugins) != 0 && !p.podMatchesEvent(pInfo, event) {
			continue
		}
		pod := pInfo.Pod
		// 1.如果发现当前pod还未到达unschedulableQ中的重试等待时间,就加入到BackoffQ中
		if p.isPodBackingoff(pInfo) {
			if err := p.podBackoffQ.Add(pInfo); err != nil {
				klog.ErrorS(err, "Error adding pod to the backoff queue", "pod", klog.KObj(pod))
			} else {
				metrics.SchedulerQueueIncomingPods.WithLabelValues("backoff", event.Label).Inc()
				p.unschedulablePods.delete(pod)
			}
		} else {
			// 2.如果当前Pod到达了unschedulableQ中的重试等待时间，则说明可以直接用于调度，则加入到activeQ中
			if err := p.activeQ.Add(pInfo); err != nil {
				klog.ErrorS(err, "Error adding pod to the scheduling queue", "pod", klog.KObj(pod))
			} else {
				activated = true
				metrics.SchedulerQueueIncomingPods.WithLabelValues("active", event.Label).Inc()
				p.unschedulablePods.delete(pod)
			}
		}
	}
	p.moveRequestCycle = p.schedulingCycle
	if activated {
		p.cond.Broadcast()
	}
}

// getUnschedulablePodsWithMatchingAffinityTerm returns unschedulable pods which have
// any affinity term that matches "pod".
// NOTE: this function assumes lock has been acquired in caller.
func (p *PriorityQueue) getUnschedulablePodsWithMatchingAffinityTerm(pod *v1.Pod) []*framework.QueuedPodInfo {
	var nsLabels labels.Set
	nsLabels = interpodaffinity.GetNamespaceLabelsSnapshot(pod.Namespace, p.nsLister)

	var podsToMove []*framework.QueuedPodInfo
	for _, pInfo := range p.unschedulablePods.podInfoMap {
		for _, term := range pInfo.RequiredAffinityTerms {
			if term.Matches(pod, nsLabels) {
				podsToMove = append(podsToMove, pInfo)
				break
			}
		}

	}
	return podsToMove
}

// PendingPods returns all the pending pods in the queue. This function is
// used for debugging purposes in the scheduler cache dumper and comparer.
func (p *PriorityQueue) PendingPods() []*v1.Pod {
	p.lock.RLock()
	defer p.lock.RUnlock()
	var result []*v1.Pod
	for _, pInfo := range p.activeQ.List() {
		result = append(result, pInfo.(*framework.QueuedPodInfo).Pod)
	}
	for _, pInfo := range p.podBackoffQ.List() {
		result = append(result, pInfo.(*framework.QueuedPodInfo).Pod)
	}
	for _, pInfo := range p.unschedulablePods.podInfoMap {
		result = append(result, pInfo.Pod)
	}
	return result
}

// Close closes the priority queue.
func (p *PriorityQueue) Close() {
	p.lock.Lock()
	defer p.lock.Unlock()
	close(p.stop)
	p.closed = true
	p.cond.Broadcast()
}

// DeleteNominatedPodIfExists deletes <pod> from nominatedPods.
func (npm *nominator) DeleteNominatedPodIfExists(pod *v1.Pod) {
	npm.Lock()
	npm.delete(pod)
	npm.Unlock()
}

// AddNominatedPod adds a pod to the nominated pods of the given node.
// This is called during the preemption process after a node is nominated to run
// the pod. We update the structure before sending a request to update the pod
// object to avoid races with the following scheduling cycles.
func (npm *nominator) AddNominatedPod(pi *framework.PodInfo, nominatingInfo *framework.NominatingInfo) {
	npm.Lock()
	npm.add(pi, nominatingInfo)
	npm.Unlock()
}

// NominatedPodsForNode returns a copy of pods that are nominated to run on the given node,
// but they are waiting for other pods to be removed from the node.
func (npm *nominator) NominatedPodsForNode(nodeName string) []*framework.PodInfo {
	npm.RLock()
	defer npm.RUnlock()
	// Make a copy of the nominated Pods so the caller can mutate safely.
	pods := make([]*framework.PodInfo, len(npm.nominatedPods[nodeName]))
	for i := 0; i < len(pods); i++ {
		pods[i] = npm.nominatedPods[nodeName][i].DeepCopy()
	}
	return pods
}

// podsCompareBackoffCompleted
//
//	@Description:
//		比较2个Pod的在backoffQ中的重试等待时间
//		podBackoffQ实际上会根据pod的backoffTime来进行优先级排序，所以podBackoffQ的队列头部，就是剩余重试等待时间最短的Pod
//	@receiver p *PriorityQueue 对象
//	@param podInfo1 第1个待比较Pod
//	@param podInfo2 第2个待比较Pod
//	@return bool 返回比较结果
func (p *PriorityQueue) podsCompareBackoffCompleted(podInfo1, podInfo2 interface{}) bool {
	pInfo1 := podInfo1.(*framework.QueuedPodInfo)
	pInfo2 := podInfo2.(*framework.QueuedPodInfo)
	bo1 := p.getBackoffTime(pInfo1)
	bo2 := p.getBackoffTime(pInfo2)
	return bo1.Before(bo2)
}

// newQueuedPodInfo builds a QueuedPodInfo object.
func (p *PriorityQueue) newQueuedPodInfo(pod *v1.Pod, plugins ...string) *framework.QueuedPodInfo {
	now := p.clock.Now()
	return &framework.QueuedPodInfo{
		PodInfo:                 framework.NewPodInfo(pod),
		Timestamp:               now,
		InitialAttemptTimestamp: now,
		UnschedulablePlugins:    sets.NewString(plugins...),
	}
}

// getBackoffTime returns the time that podInfo completes backoff
func (p *PriorityQueue) getBackoffTime(podInfo *framework.QueuedPodInfo) time.Time {
	duration := p.calculateBackoffDuration(podInfo)
	backoffTime := podInfo.Timestamp.Add(duration)
	return backoffTime
}

// calculateBackoffDuration is a helper function for calculating the backoffDuration
// based on the number of attempts the pod has made.

// calculateBackoffDuration
//
//	@Description:
//		根据pod的尝试次数计算backoffDuration
//	@receiver p *PriorityQueue 对象
//	@param podInfo 待计算的Pod信息
//	@return time.Duration 返回该Pod当前应该退避的时间（重试等待时间）
func (p *PriorityQueue) calculateBackoffDuration(podInfo *framework.QueuedPodInfo) time.Duration {
	// 1.初始的重试等待时间是1s
	duration := p.podInitialBackoffDuration
	// 2.根据当前Pod的重试次数，获取当前的重试等待时间
	for i := 1; i < podInfo.Attempts; i++ {
		// Use subtraction instead of addition or multiplication to avoid overflow.
		// 使用减法代替加法或乘法以避免溢出
		// 2.1 如果当前重试等待时间已经超过了最大值（默认10s），则保持10s,不再增大
		if duration > p.podMaxBackoffDuration-duration {
			return p.podMaxBackoffDuration
		}
		// 2.2 如果当前重试等待时间还没有超过最大值，则将重试等待时间+1s
		duration += duration
	}
	// 3.返回重试等待时间
	return duration
}

func updatePod(oldPodInfo interface{}, newPod *v1.Pod) *framework.QueuedPodInfo {
	pInfo := oldPodInfo.(*framework.QueuedPodInfo)
	pInfo.Update(newPod)
	return pInfo
}

// UnschedulablePods holds pods that cannot be scheduled. This data structure
// is used to implement unschedulablePods.
type UnschedulablePods struct {
	// podInfoMap is a map key by a pod's full-name and the value is a pointer to the QueuedPodInfo.
	podInfoMap map[string]*framework.QueuedPodInfo
	keyFunc    func(*v1.Pod) string
	// metricRecorder updates the counter when elements of an unschedulablePodsMap
	// get added or removed, and it does nothing if it's nil
	metricRecorder metrics.MetricRecorder
}

// Add adds a pod to the unschedulable podInfoMap.
func (u *UnschedulablePods) addOrUpdate(pInfo *framework.QueuedPodInfo) {
	podID := u.keyFunc(pInfo.Pod)
	if _, exists := u.podInfoMap[podID]; !exists && u.metricRecorder != nil {
		u.metricRecorder.Inc()
	}
	u.podInfoMap[podID] = pInfo
}

// Delete deletes a pod from the unschedulable podInfoMap.
func (u *UnschedulablePods) delete(pod *v1.Pod) {
	podID := u.keyFunc(pod)
	if _, exists := u.podInfoMap[podID]; exists && u.metricRecorder != nil {
		u.metricRecorder.Dec()
	}
	delete(u.podInfoMap, podID)
}

// Get returns the QueuedPodInfo if a pod with the same key as the key of the given "pod"
// is found in the map. It returns nil otherwise.
func (u *UnschedulablePods) get(pod *v1.Pod) *framework.QueuedPodInfo {
	podKey := u.keyFunc(pod)
	if pInfo, exists := u.podInfoMap[podKey]; exists {
		return pInfo
	}
	return nil
}

// Clear removes all the entries from the unschedulable podInfoMap.
func (u *UnschedulablePods) clear() {
	u.podInfoMap = make(map[string]*framework.QueuedPodInfo)
	if u.metricRecorder != nil {
		u.metricRecorder.Clear()
	}
}

// newUnschedulablePods initializes a new object of UnschedulablePods.
func newUnschedulablePods(metricRecorder metrics.MetricRecorder) *UnschedulablePods {
	return &UnschedulablePods{
		podInfoMap:     make(map[string]*framework.QueuedPodInfo),
		keyFunc:        util.GetPodFullName,
		metricRecorder: metricRecorder,
	}
}

// nominator is a structure that stores pods nominated to run on nodes.
// It exists because nominatedNodeName of pod objects stored in the structure
// may be different than what scheduler has here. We should be able to find pods
// by their UID and update/delete them.
type nominator struct {
	// podLister is used to verify if the given pod is alive.
	podLister listersv1.PodLister
	// nominatedPods is a map keyed by a node name and the value is a list of
	// pods which are nominated to run on the node. These are pods which can be in
	// the activeQ or unschedulablePods.
	nominatedPods map[string][]*framework.PodInfo
	// nominatedPodToNode is map keyed by a Pod UID to the node name where it is
	// nominated.
	nominatedPodToNode map[types.UID]string

	sync.RWMutex
}

func (npm *nominator) add(pi *framework.PodInfo, nominatingInfo *framework.NominatingInfo) {
	// Always delete the pod if it already exists, to ensure we never store more than
	// one instance of the pod.
	npm.delete(pi.Pod)

	var nodeName string
	if nominatingInfo.Mode() == framework.ModeOverride {
		nodeName = nominatingInfo.NominatedNodeName
	} else if nominatingInfo.Mode() == framework.ModeNoop {
		if pi.Pod.Status.NominatedNodeName == "" {
			return
		}
		nodeName = pi.Pod.Status.NominatedNodeName
	}

	if npm.podLister != nil {
		// If the pod was removed or if it was already scheduled, don't nominate it.
		updatedPod, err := npm.podLister.Pods(pi.Pod.Namespace).Get(pi.Pod.Name)
		if err != nil {
			klog.V(4).InfoS("Pod doesn't exist in podLister, aborted adding it to the nominator", "pod", klog.KObj(pi.Pod))
			return
		}
		if updatedPod.Spec.NodeName != "" {
			klog.V(4).InfoS("Pod is already scheduled to a node, aborted adding it to the nominator", "pod", klog.KObj(pi.Pod), "node", updatedPod.Spec.NodeName)
			return
		}
	}

	npm.nominatedPodToNode[pi.Pod.UID] = nodeName
	for _, npi := range npm.nominatedPods[nodeName] {
		if npi.Pod.UID == pi.Pod.UID {
			klog.V(4).InfoS("Pod already exists in the nominator", "pod", klog.KObj(npi.Pod))
			return
		}
	}
	npm.nominatedPods[nodeName] = append(npm.nominatedPods[nodeName], pi)
}

func (npm *nominator) delete(p *v1.Pod) {
	nnn, ok := npm.nominatedPodToNode[p.UID]
	if !ok {
		return
	}
	for i, np := range npm.nominatedPods[nnn] {
		if np.Pod.UID == p.UID {
			npm.nominatedPods[nnn] = append(npm.nominatedPods[nnn][:i], npm.nominatedPods[nnn][i+1:]...)
			if len(npm.nominatedPods[nnn]) == 0 {
				delete(npm.nominatedPods, nnn)
			}
			break
		}
	}
	delete(npm.nominatedPodToNode, p.UID)
}

// UpdateNominatedPod updates the <oldPod> with <newPod>.
func (npm *nominator) UpdateNominatedPod(oldPod *v1.Pod, newPodInfo *framework.PodInfo) {
	npm.Lock()
	defer npm.Unlock()
	// In some cases, an Update event with no "NominatedNode" present is received right
	// after a node("NominatedNode") is reserved for this pod in memory.
	// In this case, we need to keep reserving the NominatedNode when updating the pod pointer.
	var nominatingInfo *framework.NominatingInfo
	// We won't fall into below `if` block if the Update event represents:
	// (1) NominatedNode info is added
	// (2) NominatedNode info is updated
	// (3) NominatedNode info is removed
	if NominatedNodeName(oldPod) == "" && NominatedNodeName(newPodInfo.Pod) == "" {
		if nnn, ok := npm.nominatedPodToNode[oldPod.UID]; ok {
			// This is the only case we should continue reserving the NominatedNode
			nominatingInfo = &framework.NominatingInfo{
				NominatingMode:    framework.ModeOverride,
				NominatedNodeName: nnn,
			}
		}
	}
	// We update irrespective of the nominatedNodeName changed or not, to ensure
	// that pod pointer is updated.
	npm.delete(oldPod)
	npm.add(newPodInfo, nominatingInfo)
}

// NewPodNominator creates a nominator as a backing of framework.PodNominator.
// A podLister is passed in so as to check if the pod exists
// before adding its nominatedNode info.
func NewPodNominator(podLister listersv1.PodLister) framework.PodNominator {
	return &nominator{
		podLister:          podLister,
		nominatedPods:      make(map[string][]*framework.PodInfo),
		nominatedPodToNode: make(map[types.UID]string),
	}
}

// MakeNextPodFunc returns a function to retrieve the next pod from a given
// scheduling queue
func MakeNextPodFunc(queue SchedulingQueue) func() *framework.QueuedPodInfo {
	return func() *framework.QueuedPodInfo {
		podInfo, err := queue.Pop()
		if err == nil {
			klog.V(4).InfoS("About to try and schedule pod", "pod", klog.KObj(podInfo.Pod))
			for plugin := range podInfo.UnschedulablePlugins {
				metrics.UnschedulableReason(plugin, podInfo.Pod.Spec.SchedulerName).Dec()
			}
			return podInfo
		}
		klog.ErrorS(err, "Error while retrieving next pod from scheduling queue")
		return nil
	}
}

func podInfoKeyFunc(obj interface{}) (string, error) {
	return cache.MetaNamespaceKeyFunc(obj.(*framework.QueuedPodInfo).Pod)
}

// Checks if the Pod may become schedulable upon the event.
// This is achieved by looking up the global clusterEventMap registry.
func (p *PriorityQueue) podMatchesEvent(podInfo *framework.QueuedPodInfo, clusterEvent framework.ClusterEvent) bool {
	if clusterEvent.IsWildCard() {
		return true
	}

	for evt, nameSet := range p.clusterEventMap {
		// Firstly verify if the two ClusterEvents match:
		// - either the registered event from plugin side is a WildCardEvent,
		// - or the two events have identical Resource fields and *compatible* ActionType.
		//   Note the ActionTypes don't need to be *identical*. We check if the ANDed value
		//   is zero or not. In this way, it's easy to tell Update&Delete is not compatible,
		//   but Update&All is.
		evtMatch := evt.IsWildCard() ||
			(evt.Resource == clusterEvent.Resource && evt.ActionType&clusterEvent.ActionType != 0)

		// Secondly verify the plugin name matches.
		// Note that if it doesn't match, we shouldn't continue to search.
		if evtMatch && intersect(nameSet, podInfo.UnschedulablePlugins) {
			return true
		}
	}

	return false
}

func intersect(x, y sets.String) bool {
	if len(x) > len(y) {
		x, y = y, x
	}
	for v := range x {
		if y.Has(v) {
			return true
		}
	}
	return false
}
