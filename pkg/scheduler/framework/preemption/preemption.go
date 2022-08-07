/*
Copyright 2021 The Kubernetes Authors.

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

package preemption

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	v1 "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	corelisters "k8s.io/client-go/listers/core/v1"
	policylisters "k8s.io/client-go/listers/policy/v1"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/klog/v2"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	"k8s.io/kubernetes/pkg/scheduler/util"
)

// Candidate represents a nominated node on which the preemptor can be scheduled,
// along with the list of victims that should be evicted for the preemptor to fit the node.
// 候选表示一个指定节点，可以在该节点上调度抢占器，以及应该被驱逐以使抢占器适合该节点的受害者列表。
type Candidate interface {
	// Victims wraps a list of to-be-preempted Pods and the number of PDB violation.
	// 受害者包装了一个将要被抢占的Pod列表和违反PDB的数量。
	Victims() *extenderv1.Victims
	// Name returns the target node name where the preemptor gets nominated to run.
	// name返回指定抢占器运行的目标宿主机节点名称
	Name() string
}

type candidate struct {
	victims *extenderv1.Victims
	name    string
}

// Victims returns s.victims.
func (s *candidate) Victims() *extenderv1.Victims {
	return s.victims
}

// Name returns s.name.
func (s *candidate) Name() string {
	return s.name
}

type candidateList struct {
	idx   int32
	items []Candidate
}

func newCandidateList(size int32) *candidateList {
	return &candidateList{idx: -1, items: make([]Candidate, size)}
}

// add adds a new candidate to the internal array atomically.
func (cl *candidateList) add(c *candidate) {
	if idx := atomic.AddInt32(&cl.idx, 1); idx < int32(len(cl.items)) {
		cl.items[idx] = c
	}
}

// size returns the number of candidate stored. Note that some add() operations
// might still be executing when this is called, so care must be taken to
// ensure that all add() operations complete before accessing the elements of
// the list.
func (cl *candidateList) size() int32 {
	n := atomic.LoadInt32(&cl.idx) + 1
	if n >= int32(len(cl.items)) {
		n = int32(len(cl.items))
	}
	return n
}

// get returns the internal candidate array. This function is NOT atomic and
// assumes that all add() operations have been completed.
func (cl *candidateList) get() []Candidate {
	return cl.items[:cl.size()]
}

// Interface is expected to be implemented by different preemption plugins as all those member
// methods might have different behavior compared with the default preemption.
type Interface interface {
	// GetOffsetAndNumCandidates chooses a random offset and calculates the number of candidates that should be
	// shortlisted for dry running preemption.
	// 选择一个随机偏移量，并计算应该入围运行抢占的候选宿主机的数量。
	GetOffsetAndNumCandidates(nodes int32) (int32, int32)
	// CandidatesToVictimsMap builds a map from the target node to a list of to-be-preempted Pods and the number of PDB violation.
	// 构建从目标节点到待抢占Pod列表和PDB违规数量的映射。
	CandidatesToVictimsMap(candidates []Candidate) map[string]*extenderv1.Victims
	// PodEligibleToPreemptOthers returns one bool and one string. The bool indicates whether this pod should be considered for
	// preempting other pods or not. The string includes the reason if this pod isn't eligible.
	PodEligibleToPreemptOthers(pod *v1.Pod, nominatedNodeStatus *framework.Status) (bool, string)
	// SelectVictimsOnNode finds minimum set of pods on the given node that should be preempted in order to make enough room
	// for "pod" to be scheduled.
	// Note that both `state` and `nodeInfo` are deep copied.
	// 在给定节点上找到应该被抢占的最小Pod集（即受害者Pod），以便为计划“Pod”留出足够的空间。
	// 请注意，'state'和'nodeInfo'都是深度复制的。
	SelectVictimsOnNode(ctx context.Context, state *framework.CycleState,
		pod *v1.Pod, nodeInfo *framework.NodeInfo, pdbs []*policy.PodDisruptionBudget) ([]*v1.Pod, int, *framework.Status)
}

type Evaluator struct {
	PluginName string
	Handler    framework.Handle
	PodLister  corelisters.PodLister
	PdbLister  policylisters.PodDisruptionBudgetLister
	State      *framework.CycleState
	Interface
}

// Preempt returns a PostFilterResult carrying suggested nominatedNodeName, along with a Status.
// The semantics of returned <PostFilterResult, Status> varies on different scenarios:
// - <nil, Error>. This denotes it's a transient/rare error that may be self-healed in future cycles.
// - <nil, Unschedulable>. This status is mostly as expected like the preemptor is waiting for the
//   victims to be fully terminated.
// - In both cases above, a nil PostFilterResult is returned to keep the pod's nominatedNodeName unchanged.
//
// - <non-nil PostFilterResult, Unschedulable>. It indicates the pod cannot be scheduled even with preemption.
//   In this case, a non-nil PostFilterResult is returned and result.NominatingMode instructs how to deal with
//   the nominatedNodeName.
// - <non-nil PostFilterResult}, Success>. It's the regular happy path
//   and the non-empty nominatedNodeName will be applied to the preemptor pod.
//
// 抢占返回一个PostFilterResult，其中包含建议的naminatedNodeName以及一个状态。
// 返回的<PostFilterResult， status>的语义学在不同的场景下有所不同：
// -<nil， Error>。这表示它是一个短暂/罕见的错误，可能会在未来的周期中自我修复。
// -<nil，不可调度>。此状态主要如预期的那样，就像抢占者正在等待
// 受害者将被完全终止。
// -在上述两种情况下，都会返回nil PostFilterResult以保持pod的naminatedNodeName不变。
//
// -<non-nil PostFilterResult， Un调度>。它表示即使使用抢占也无法调度pod。
// 在这种情况下，返回一个非nil的PostFilterResult并产生结果。NominatingMode指导如何处理
// 被提名的NodeName。
// -<non-nil PostFilterResult}，成功>。这是常规的快乐路径
// 并且非空的nominatedNodeName将应用于抢占器pod。

//
// Preempt
//  @Description:
//  	PostFilter抢占流程的主方法
//  @receiver ev *Evaluator 对象
//  @param ctx 上下文
//  @param pod 请求中的Pod
//  @param m 宿主机节点及其状态的映射
//  @return *framework.PostFilterResult 返回抢占结果
//  @return *framework.Status 返回Pod此时的状态
//
func (ev *Evaluator) Preempt(ctx context.Context, pod *v1.Pod, m framework.NodeToStatusMap) (*framework.PostFilterResult, *framework.Status) {
	// 0) Fetch the latest version of <pod>.
	// It's safe to directly fetch pod here. Because the informer cache has already been
	// initialized when creating the Scheduler obj, i.e., factory.go#MakeDefaultErrorFunc().
	// However, tests may need to manually initialize the shared pod informer.
	// 0.获取最新版本的Pod，即获取更新的抢占者Pod对象
	podNamespace, podName := pod.Namespace, pod.Name
	pod, err := ev.PodLister.Pods(pod.Namespace).Get(pod.Name)
	if err != nil {
		klog.ErrorS(err, "Getting the updated preemptor pod object", "pod", klog.KRef(podNamespace, podName))
		return nil, framework.AsStatus(err)
	}

	// 1) Ensure the preemptor is eligible to preempt other pods.
	// 1.检查Pod是否可实施抢占，即确保抢占者有资格抢占其他pod
	// 调用PodEligibleToPreemptOthers
	if ok, msg := ev.PodEligibleToPreemptOthers(pod, m[pod.Status.NominatedNodeName]); !ok {
		// Pod不符合抢占条件，即只需要等待Pod提名宿主机中的Pod优雅退出后，其优雅占用即可
		klog.V(5).InfoS("Pod is not eligible for preemption", "pod", klog.KObj(pod), "reason", msg)
		return nil, framework.NewStatus(framework.Unschedulable, msg)
	}

	// 2) Find all preemption candidates.
	// 2.找到所有适合抢占的候选宿主机，即每个宿主机上对应的受害者Pod
	// 调用findCandidates
	candidates, nodeToStatusMap, err := ev.findCandidates(ctx, pod, m)
	if err != nil && len(candidates) == 0 {
		return nil, framework.AsStatus(err)
	}

	// Return a FitError only when there are no candidates that fit the pod.
	// 仅当没有适合pod的候选者时才返回FitError。
	if len(candidates) == 0 {
		fitError := &framework.FitError{
			Pod:         pod,
			NumAllNodes: len(nodeToStatusMap),
			Diagnosis: framework.Diagnosis{
				NodeToStatusMap: nodeToStatusMap,
				// Leave FailedPlugins as nil as it won't be used on moving Pods.
			},
		}
		// Specify nominatedNodeName to clear the pod's nominatedNodeName status, if applicable.
		// 将FailedPlugins保留为nil，因为它不会用于移动Pod
		return framework.NewPostFilterResultWithNominatedNode(""), framework.NewStatus(framework.Unschedulable, fitError.Error())
	}

	// 3) Interact with registered Extenders to filter out some candidates if needed.
	// 3.如果需要，与自定义注册的扩展点交互以过滤掉一些候选宿主机
	candidates, status := ev.callExtenders(pod, candidates)
	if !status.IsSuccess() {
		return nil, status
	}

	// 4) Find the best candidate.
	// 4.寻找最佳抢占宿主机
	// 调用SelectCandidate
	bestCandidate := ev.SelectCandidate(candidates)
	if bestCandidate == nil || len(bestCandidate.Name()) == 0 {
		return nil, framework.NewStatus(framework.Unschedulable, "no candidate node for preemption")
	}

	// 5) Perform preparation work before nominating the selected candidate.
	// 5.执行抢占，删除被抢占的node节点中victim pod（牺牲者），以及清除牺牲者的NominatedNodeName字段信息
	// 调用prepareCandidate
	if status := ev.prepareCandidate(ctx, bestCandidate, pod, ev.PluginName); !status.IsSuccess() {
		return nil, status
	}
	// 6.将抢占到的宿主机节点返回
	return framework.NewPostFilterResultWithNominatedNode(bestCandidate.Name()), framework.NewStatus(framework.Success)
}

// FindCandidates calculates a slice of preemption candidates.
// Each candidate is executable to make the given <pod> schedulable.

//
// findCandidates
//  @Description:
//  	找到所有宿主机节点中，能够被抢占的宿主机节点
//  	返回候选node列表以及node节点中需要被删除的pod（牺牲者）
//  @receiver ev *Evaluator 对象
//  @param ctx 上下文
//  @param pod 抢占者Pod
//  @param m 宿主机节点名字与状态的映射
//  @return []Candidate 所有可抢占的宿主机列表
//  @return framework.NodeToStatusMap 可抢占宿主机列表的状态
//  @return error 错误信息
//
func (ev *Evaluator) findCandidates(ctx context.Context, pod *v1.Pod, m framework.NodeToStatusMap) ([]Candidate, framework.NodeToStatusMap, error) {
	// 1.获取所有的宿主机列表
	allNodes, err := ev.Handler.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		return nil, nil, err
	}
	if len(allNodes) == 0 {
		return nil, nil, errors.New("no nodes available")
	}
	// 2.找到所有不可调度但可抢占的宿主机节点
	potentialNodes, unschedulableNodeStatus := nodesWherePreemptionMightHelp(allNodes, m)
	// 如果集群中不存在可抢占的宿主机节点，则直接返回抢占失败
	if len(potentialNodes) == 0 {
		klog.V(3).InfoS("Preemption will not help schedule pod on any node", "pod", klog.KObj(pod))
		// In this case, we should clean-up any existing nominated node name of the pod.
		// 在这种情况下，我们应该清理pod的任何现有指定节点名称
		// 即如果当前pod不存在可以抢占的node，那么就把pod的提名node信息给删掉
		if err := util.ClearNominatedNodeName(ctx, ev.Handler.ClientSet(), pod); err != nil {
			klog.ErrorS(err, "Cannot clear 'NominatedNodeName' field of pod", "pod", klog.KObj(pod))
			// We do not return as this error is not critical.
		}
		return nil, unschedulableNodeStatus, nil
	}
	// 3.获取PDB对象，PDB能够限制同时中断的pod资源对象的数量，以保证集群的高可用性
	// 相关内容：https://blog.csdn.net/u012124304/article/details/118001301
	// 该对象用来限制同时驱逐的Pod数量，使被驱逐的Pod能够平滑的转移到别的宿主机上，防止一次性大量Pod被驱逐导致集群出现问题
	pdbs, err := getPodDisruptionBudgets(ev.PdbLister)
	if err != nil {
		return nil, nil, err
	}
	// 获取一个随机偏移量，和入围抢占逻辑的宿主机数量（即指定要初步筛选出多少可以用于抢占的宿主机节点）
	offset, numCandidates := ev.GetOffsetAndNumCandidates(int32(len(potentialNodes)))
	if klogV := klog.V(5); klogV.Enabled() {
		var sample []string
		for i := offset; i < offset+10 && i < int32(len(potentialNodes)); i++ {
			sample = append(sample, potentialNodes[i].Node().Name)
		}
		klogV.InfoS("Selecting candidates from a pool of nodes", "potentialNodesCount", len(potentialNodes), "offset", offset, "sampleLength", len(sample), "sample", sample, "candidates", numCandidates)
	}
	// 4.寻找需求数量的、符合条件的node及需要驱逐的Pod对象，并封装成candidate数组返回
	candidates, nodeStatuses, err := ev.DryRunPreemption(ctx, pod, potentialNodes, pdbs, offset, numCandidates)
	// 更新所有可以用于抢占的宿主机状态
	for node, nodeStatus := range unschedulableNodeStatus {
		nodeStatuses[node] = nodeStatus
	}
	// 5.返回所有符合抢占条件的宿主机节点、宿主机名字与其对应状态、错误码
	return candidates, nodeStatuses, err
}

// callExtenders calls given <extenders> to select the list of feasible candidates.
// We will only check <candidates> with extenders that support preemption.
// Extenders which do not support preemption may later prevent preemptor from being scheduled on the nominated
// node. In that case, scheduler will find a different host for the preemptor in subsequent scheduling cycles.
func (ev *Evaluator) callExtenders(pod *v1.Pod, candidates []Candidate) ([]Candidate, *framework.Status) {
	extenders := ev.Handler.Extenders()
	nodeLister := ev.Handler.SnapshotSharedLister().NodeInfos()
	if len(extenders) == 0 {
		return candidates, nil
	}

	// Migrate candidate slice to victimsMap to adapt to the Extender interface.
	// It's only applicable for candidate slice that have unique nominated node name.
	victimsMap := ev.CandidatesToVictimsMap(candidates)
	if len(victimsMap) == 0 {
		return candidates, nil
	}
	for _, extender := range extenders {
		if !extender.SupportsPreemption() || !extender.IsInterested(pod) {
			continue
		}
		nodeNameToVictims, err := extender.ProcessPreemption(pod, victimsMap, nodeLister)
		if err != nil {
			if extender.IsIgnorable() {
				klog.InfoS("Skipping extender as it returned error and has ignorable flag set",
					"extender", extender, "err", err)
				continue
			}
			return nil, framework.AsStatus(err)
		}
		// Check if the returned victims are valid.
		for nodeName, victims := range nodeNameToVictims {
			if victims == nil || len(victims.Pods) == 0 {
				if extender.IsIgnorable() {
					delete(nodeNameToVictims, nodeName)
					klog.InfoS("Ignoring node without victims", "node", klog.KRef("", nodeName))
					continue
				}
				return nil, framework.AsStatus(fmt.Errorf("expected at least one victim pod on node %q", nodeName))
			}
		}

		// Replace victimsMap with new result after preemption. So the
		// rest of extenders can continue use it as parameter.
		victimsMap = nodeNameToVictims

		// If node list becomes empty, no preemption can happen regardless of other extenders.
		if len(victimsMap) == 0 {
			break
		}
	}

	var newCandidates []Candidate
	for nodeName := range victimsMap {
		newCandidates = append(newCandidates, &candidate{
			victims: victimsMap[nodeName],
			name:    nodeName,
		})
	}
	return newCandidates, nil
}

// SelectCandidate chooses the best-fit candidate from given <candidates> and return it.
// NOTE: This method is exported for easier testing in default preemption.
// SelectCandidate从给定的<候选人>中选择最合适的候选人并返回它。
// 注意：导出此方法以便于在默认抢占中进行测试。

//
// SelectCandidate
//  @Description:
//  	抢占逻辑的优选过程，从多个适合的宿主机节点中，选择出最适合被抢占的那一个宿主机节点及其上的最适合的受害者Pod
//  @receiver ev *Evaluator 对象
//  @param candidates 通过findCandidate函数的，所有可抢占宿主机列表及其对应Pod
//  @return Candidate 返回最适合的那个宿主机和Pod
//
func (ev *Evaluator) SelectCandidate(candidates []Candidate) Candidate {
	// 如果抢占预选出来的宿主机节点数量为0，则返回空
	if len(candidates) == 0 {
		return nil
	}
	// 如果抢占预选出来的宿主机节点是1,则直接将这唯一一个宿主机节点及Pod返回
	if len(candidates) == 1 {
		return candidates[0]
	}
	// 1.建立所有候选宿主机节点,与其上需要牺牲的Pod的映射关系
	victimsMap := ev.CandidatesToVictimsMap(candidates)
	// 2.执行抢占优选过程，选出一个最适合抢占的宿主机节点和上面的Pod
	candidateNode := pickOneNodeForPreemption(victimsMap)

	// Same as candidatesToVictimsMap, this logic is not applicable for out-of-tree
	// preemption plugins that exercise different candidates on the same nominated node.
	// 与候选者ToVictimsMap相同，此逻辑不适用于在同一提名节点上行使不同候选者的树外抢占插件。
	// 3.将优选出来的最佳抢占宿主机节点和受害者Pod信息返回
	if victims := victimsMap[candidateNode]; victims != nil {
		return &candidate{
			victims: victims,
			name:    candidateNode,
		}
	}

	// We shouldn't reach here.
	klog.ErrorS(errors.New("no candidate selected"), "Should not reach here", "candidates", candidates)
	// To not break the whole flow, return the first candidate.
	// 为了不破坏整个流程，返回第一个候选者。
	// 4.如果没有优选出最适合的节点，则将候选抢占节点列表中的第一个节点返回
	return candidates[0]
}

// prepareCandidate does some preparation work before nominating the selected candidate:
// - Evict the victim pods
// - Reject the victim pods if they are in waitingPod map
// - Clear the low-priority pods' nominatedNodeName status if needed
// 候选人在提名被选中的候选人之前做一些准备工作：
// -驱逐受害者pod
// -如果受害者pod在waitingPod地图中，则拒绝他们
// -如果需要，清除低优先级pod的提名节点naminatedNodeName状态

//
// prepareCandidate
//  @Description:
//  	执行抢占，删除宿主机节点上的牺牲者Pod
//  	让提名该抢占宿主机，且优先级比当前抢占者Pod低的待调度Pod，重新加入activeQ队列，重新调度到别的宿主机节点
//  @receiver ev *Evaluator 对象
//  @param ctx 请求上下文
//  @param c 被抢占的宿主机及其上的受害者Pod
//  @param pod 抢占者Pod
//  @param pluginName 插件名字
//  @return *framework.Status 返回抢占状态
//
func (ev *Evaluator) prepareCandidate(ctx context.Context, c Candidate, pod *v1.Pod, pluginName string) *framework.Status {
	fh := ev.Handler
	cs := ev.Handler.ClientSet()
	// 1.遍历被抢占宿主机上的所有Pod，删除受害者Pod
	for _, victim := range c.Victims().Pods {
		// If the victim is a WaitingPod, send a reject message to the PermitPlugin.
		// Otherwise we should delete the victim.
		// 如果受害者是WaitingPod，则向PermitPlugin发送拒绝消息，否则我们应该删除受害者
		if waitingPod := fh.GetWaitingPod(victim.UID); waitingPod != nil {
			waitingPod.Reject(pluginName, "preempted")
		} else if err := util.DeletePod(ctx, cs, victim); err != nil {
			klog.ErrorS(err, "Preempting pod", "pod", klog.KObj(victim), "preemptor", klog.KObj(pod))
			return framework.AsStatus(err)
		}
		fh.EventRecorder().Eventf(victim, pod, v1.EventTypeNormal, "Preempted", "Preempting", "Preempted by %v/%v on node %v",
			pod.Namespace, pod.Name, c.Name())
	}
	metrics.PreemptionVictims.Observe(float64(len(c.Victims().Pods)))

	// Lower priority pods nominated to run on this node, may no longer fit on
	// this node. So, we should remove their nomination. Removing their
	// nomination updates these pods and moves them to the active queue. It
	// lets scheduler find another place for them.
	// 指定在此节点上运行的较低优先级的pod可能不再适合此节点。
	// 因此，我们应该删除它们的提名。
	// 删除它们的提名会更新这些pod并将它们移动到活动队列。它让调度程序为它们找到另一个位置。
	// 2.将调度过程中，提名到该宿主机节点，但优先级没有抢占者Pod高的Pod的提名删除，将其加入ActiveQ重调度队列
	// 2.1 获取这些低优先级的提名Pod
	nominatedPods := getLowerPriorityNominatedPods(fh, pod, c.Name())
	// 2.2 将这些低优先级Pod从宿主机节点上除名
	if err := util.ClearNominatedNodeName(ctx, cs, nominatedPods...); err != nil {
		klog.ErrorS(err, "Cannot clear 'NominatedNodeName' field")
		// We do not return as this error is not critical.
		// 我们不返回，因为这个错误并不严重。
	}

	return nil
}

// nodesWherePreemptionMightHelp returns a list of nodes with failed predicates
// that may be satisfied by removing pods from the node.

//
// nodesWherePreemptionMightHelp
//  @Description:
//  	获取predicates阶段失败的宿主机列表
//  	即filter扩展点插件不通过的宿主机
//  @param nodes 集群下所有的宿主机列表
//  @param m 宿主机名字与其状态的映射
//  @return []*framework.NodeInfo 返回可抢占的宿主机节点列表
//  @return framework.NodeToStatusMap 返回可抢占宿主机名字和状态的映射
//
func nodesWherePreemptionMightHelp(nodes []*framework.NodeInfo, m framework.NodeToStatusMap) ([]*framework.NodeInfo, framework.NodeToStatusMap) {
	var potentialNodes []*framework.NodeInfo
	nodeStatuses := make(framework.NodeToStatusMap)
	// 1.遍历集群下所有的宿主机节点
	for _, node := range nodes {
		name := node.Node().Name
		// We rely on the status by each plugin - 'Unschedulable' or 'UnschedulableAndUnresolvable'
		// to determine whether preemption may help or not on the node.
		// 1.1 当该宿主机节点不可调度且不可抢占,则跳过该宿主机
		if m[name].Code() == framework.UnschedulableAndUnresolvable {
			nodeStatuses[node.Node().Name] = framework.NewStatus(framework.UnschedulableAndUnresolvable, "Preemption is not helpful for scheduling")
			continue
		}
		// 1.2 如果宿主机可抢占，则将该宿主机节点加入到可抢占宿主机节点列表中
		potentialNodes = append(potentialNodes, node)
	}
	// 2.将可抢占宿主机节点及其状态返回
	return potentialNodes, nodeStatuses
}

func getPodDisruptionBudgets(pdbLister policylisters.PodDisruptionBudgetLister) ([]*policy.PodDisruptionBudget, error) {
	if pdbLister != nil {
		return pdbLister.List(labels.Everything())
	}
	return nil, nil
}

// pickOneNodeForPreemption chooses one node among the given nodes. It assumes
// pods in each map entry are ordered by decreasing priority.
// It picks a node based on the following criteria:
// 1. A node with minimum number of PDB violations.
// 2. A node with minimum highest priority victim is picked.
// 3. Ties are broken by sum of priorities of all victims.
// 4. If there are still ties, node with the minimum number of victims is picked.
// 5. If there are still ties, node with the latest start time of all highest priority victims is picked.
// 6. If there are still ties, the first such node is picked (sort of randomly).
// The 'minNodes1' and 'minNodes2' are being reused here to save the memory
// allocation and garbage collection time.
// 在给定的节点中选择一个节点。它假设每个map条目中的pod按优先级递减排序。
// 它根据以下标准选择一个节点：
// 1.PDB违规次数最少的节点
// 2.先在节点中选出最高优先级的受害者Pod，再将每个节点上的该Pod比较，选出最小优先级的Pod
// 3.所有受害者的优先事项之和最小的节点
// 4.如果仍然没选出来，则选择具有最少受害者数量的节点
// 5.如果仍然没选出来，则选择所有最高优先级受害者的最新开始时间的节点。
// 6.如果仍然没选出来，则选择第一个节点（有点随机）。
// 'minNodes1'和'minNodes2'在这里被重用，以节省内存分配和垃圾回收机制的时间。

//
// pickOneNodeForPreemption
//  @Description:
//  	优选出最适合抢占的宿主机节点及其上面的Pod
//  	它根据以下顺序标准选择一个节点，当到达某一步筛选出来的宿主机节点数量为1时，就直接返回，不再执行后续：
//  	1.PDB违规次数最少的节点
//  	2.先在节点中选出最高优先级的受害者Pod，再将每个节点上的该Pod比较，选出最小优先级的Pod
//  	3.所有受害者的优先事项之和最小的节点
//  	4.选择具有最少受害者数量的节点
//  	5.选择所有最高优先级受害者的最新开始时间的节点。
//  	6.选择第一个节点（有点随机）
//  @param nodesToVictims 候选宿主机节点和受害者Pod的映射
//  @return string 最优抢占宿主机节点的名字
//
func pickOneNodeForPreemption(nodesToVictims map[string]*extenderv1.Victims) string {
	// 如果候选宿主机节点数量为0，则返回空字符串
	if len(nodesToVictims) == 0 {
		return ""
	}
	minNumPDBViolatingPods := int64(math.MaxInt32)
	var minNodes1 []string
	lenNodes1 := 0
	// 1.遍历候选宿主机列表，找到PDB违规次数最少的节点
	for node, victims := range nodesToVictims {
		numPDBViolatingPods := victims.NumPDBViolations
		if numPDBViolatingPods < minNumPDBViolatingPods {
			minNumPDBViolatingPods = numPDBViolatingPods
			minNodes1 = nil
			lenNodes1 = 0
		}
		// 说明当前节点的PDB违规次数比上次的最小值小，将该节点加入到最小点切片中
		if numPDBViolatingPods == minNumPDBViolatingPods {
			// 该切片中的节点，从左到右PDB违规次数逐渐减少
			minNodes1 = append(minNodes1, node)
			lenNodes1++
		}
	}
	// 如果最小PDB违规节点列表中只有一个值，则直接将其返回，不再执行后续
	if lenNodes1 == 1 {
		return minNodes1[0]
	}

	// There are more than one node with minimum number PDB violating pods. Find
	// the one with minimum highest priority victim.
	// 2.当违反pod的PDB数量最少的节点不止一个，找到具有最低优先级受害者的节点
	minHighestPriority := int32(math.MaxInt32)
	var minNodes2 = make([]string, lenNodes1)
	lenNodes2 := 0
	// 遍历第一步筛选出来的宿主机节点
	for i := 0; i < lenNodes1; i++ {
		node := minNodes1[i]
		// 取出受害者Pod
		victims := nodesToVictims[node]
		// highestPodPriority is the highest priority among the victims on this node.
		// 最高优先级是此节点上受害者中的最高优先级
		highestPodPriority := corev1helpers.PodPriority(victims.Pods[0])
		if highestPodPriority < minHighestPriority {
			minHighestPriority = highestPodPriority
			lenNodes2 = 0
		}
		// 只要当前节点的最高优先级受害者Pod的优先级低于之前最低的，就将其加入节点列表中
		if highestPodPriority == minHighestPriority {
			minNodes2[lenNodes2] = node
			lenNodes2++
		}
	}
	// 如果到这里节点列表的长度变为1，则直接将该节点返回，不再执行后续
	if lenNodes2 == 1 {
		return minNodes2[0]
	}

	// There are a few nodes with minimum highest priority victim. Find the
	// smallest sum of priorities.
	// 有几个节点具有最小的最高优先级受害者。找到最小的优先级和。
	// 3.选出每个宿主机node上所有受害者Pod优先级之和最小的节点
	minSumPriorities := int64(math.MaxInt64)
	lenNodes1 = 0
	// 遍历第2步得到的宿主机节点列表
	for i := 0; i < lenNodes2; i++ {
		var sumPriorities int64
		node := minNodes2[i]
		// 遍历该节点下的所有Pod，将所有Pod的优先级相加
		for _, pod := range nodesToVictims[node].Pods {
			// We add MaxInt32+1 to all priorities to make all of them >= 0. This is
			// needed so that a node with a few pods with negative priority is not
			// picked over a node with a smaller number of pods with the same negative
			// priority (and similar scenarios).
			// 我们将MaxInt32+1添加到所有优先级以使所有优先级都>=0。这是必需的，这样具有少数负优先级pod的节点就不会被具有较少数量的pod具有相同负优先级的节点（以及类似的场景）选中。
			sumPriorities += int64(corev1helpers.PodPriority(pod)) + int64(math.MaxInt32+1)
		}
		if sumPriorities < minSumPriorities {
			minSumPriorities = sumPriorities
			lenNodes1 = 0
		}
		// 只要当前节点的优先级之和低于之前最低的，就将其加入节点列表中
		if sumPriorities == minSumPriorities {
			minNodes1[lenNodes1] = node
			lenNodes1++
		}
	}
	// 如果到这里节点列表的长度变为1，则直接将该节点返回，不再执行后续
	if lenNodes1 == 1 {
		return minNodes1[0]
	}

	// There are a few nodes with minimum highest priority victim and sum of priorities.
	// Find one with the minimum number of pods.
	// 有几个节点具有相同最小的最高优先级受害者和优先级之和，找到一个具有最少数量的pod。
	// 4.选择具有最少受害者数量的宿主机节点
	minNumPods := math.MaxInt32
	lenNodes2 = 0
	// 遍历第3步的宿主机节点列表
	for i := 0; i < lenNodes1; i++ {
		node := minNodes1[i]
		numPods := len(nodesToVictims[node].Pods)
		if numPods < minNumPods {
			minNumPods = numPods
			lenNodes2 = 0
		}
		// 只要当前宿主机的节点数量比上一个最小值小，就将该宿主机节点加入列表中
		if numPods == minNumPods {
			minNodes2[lenNodes2] = node
			lenNodes2++
		}
	}
	// 如果这一步的数量为0，则直接返回，不再执行后续
	if lenNodes2 == 1 {
		return minNodes2[0]
	}

	// There are a few nodes with same number of pods.
	// Find the node that satisfies latest(earliestStartTime(all highest-priority pods on node))
	// 有几个节点具有相同数量的pod，找到满足最新的节点（earliestStartTime（节点上所有最高优先级的pod））
	// 5.获取最早开始时间的Pod
	latestStartTime := util.GetEarliestPodStartTime(nodesToVictims[minNodes2[0]])
	if latestStartTime == nil {
		// If the earliest start time of all pods on the 1st node is nil, just return it,
		// which is not expected to happen.
		// 如果第一个节点上所有pod的最早开始时间为nil，只需返回它，预计不会发生。
		klog.ErrorS(errors.New("earliestStartTime is nil for node"), "Should not reach here", "node", klog.KRef("", minNodes2[0]))
		return minNodes2[0]
	}
	nodeToReturn := minNodes2[0]
	for i := 1; i < lenNodes2; i++ {
		node := minNodes2[i]
		// Get earliest start time of all pods on the current node.
		// 获取当前节点上所有pod的最早开始时间
		earliestStartTimeOnNode := util.GetEarliestPodStartTime(nodesToVictims[node])
		if earliestStartTimeOnNode == nil {
			klog.ErrorS(errors.New("earliestStartTime is nil for node"), "Should not reach here", "node", klog.KRef("", node))
			continue
		}
		// 找到最早的那个宿主机节点
		if earliestStartTimeOnNode.After(latestStartTime.Time) {
			latestStartTime = earliestStartTimeOnNode
			nodeToReturn = node
		}
	}
	// 6.将这个宿主机节点返回
	return nodeToReturn
}

// getLowerPriorityNominatedPods returns pods whose priority is smaller than the
// priority of the given "pod" and are nominated to run on the given node.
// Note: We could possibly check if the nominated lower priority pods still fit
// and return those that no longer fit, but that would require lots of
// manipulation of NodeInfo and PreFilter state per nominated pod. It may not be
// worth the complexity, especially because we generally expect to have a very
// small number of nominated pods per node.
// 返回优先级小于给定“pod”优先级并被指定在给定节点上运行的pod。
// 注意：我们可能会检查指定的较低优先级pod是否仍然适合并返回不再适合的pod，但这需要对每个指定pod的NodeInfo和PreFilter状态进行大量操作。
// 这可能不值得复杂，特别是因为我们通常期望每个节点有非常少量的指定pod。

//
// getLowerPriorityNominatedPods
//  @Description:
//  	获取调度时提名到该宿主机节点、且优先级低于参数中Pod（通常是抢占逻辑中的抢占者Pod）的Pod列表
//  @param pn framework.PodNominator 对象
//  @param pod 抢占者Pod
//  @param nodeName 待抢占的宿主机节点名称
//  @return []*v1.Pod 返回提名且优先级低的Pod列表
//
func getLowerPriorityNominatedPods(pn framework.PodNominator, pod *v1.Pod, nodeName string) []*v1.Pod {
	// 1.获取该宿主机节点node上的提名节点
	podInfos := pn.NominatedPodsForNode(nodeName)
	// 如果调度中没有提名到该宿主机的Pod，则直接返回
	if len(podInfos) == 0 {
		return nil
	}

	var lowerPriorityPods []*v1.Pod
	podPriority := corev1helpers.PodPriority(pod)
	// 2.遍历这些Pod
	for _, pi := range podInfos {
		// 如果提名Pod的优先级，低于抢占者Pod的优先级，则将该提名者Pod放入结果集
		if corev1helpers.PodPriority(pi.Pod) < podPriority {
			lowerPriorityPods = append(lowerPriorityPods, pi.Pod)
		}
	}
	// 3.返回结果集
	return lowerPriorityPods
}

// DryRunPreemption simulates Preemption logic on <potentialNodes> in parallel,
// returns preemption candidates and a map indicating filtered nodes statuses.
// The number of candidates depends on the constraints defined in the plugin's args. In the returned list of
// candidates, ones that do not violate PDB are preferred over ones that do.
// NOTE: This method is exported for easier testing in default preemption.
// DryRunPreemption并行模拟<potentialNodes>上的抢占逻辑，返回抢占候选者和指示过滤节点状态的映射。
// 候选者的数量取决于插件参数中定义的约束。在返回的候选者列表中，不违反PDB的候选者优先于违反PDB的候选者。
// 注意：导出此方法以便于在默认抢占中进行测试。

//
// DryRunPreemption
//  @Description:
//  	找到指定数量的可抢占节点及其宿主机状态，将其返回
//  @receiver ev *Evaluator 对象
//  @param ctx 上下文
//  @param pod 抢占者Pod
//  @param potentialNodes 所有不可调度、但是可以抢占的宿主机节点
//  @param pdbs pdbs对象，防止一次驱逐过多对象导致集群可用性降低
//  @param offset 偏移量
//  @param numCandidates 需要返回的可抢占宿主机数量
//  @return []Candidate 返回所有可抢占的宿主机节点
//  @return framework.NodeToStatusMap 可抢占宿主机节点信息
//  @return error
//
func (ev *Evaluator) DryRunPreemption(ctx context.Context, pod *v1.Pod, potentialNodes []*framework.NodeInfo,
	pdbs []*policy.PodDisruptionBudget, offset int32, numCandidates int32) ([]Candidate, framework.NodeToStatusMap, error) {
	fh := ev.Handler
	// 非违规候选宿主机
	nonViolatingCandidates := newCandidateList(numCandidates)
	// 违规候选宿主机
	violatingCandidates := newCandidateList(numCandidates)
	// 并发上下文
	parallelCtx, cancel := context.WithCancel(ctx)
	nodeStatuses := make(framework.NodeToStatusMap)
	var statusesLock sync.Mutex
	var errs []error
	// 并发协程函数
	checkNode := func(i int) {
		// 1.取出一个不可调度但可抢占的宿主机节点
		nodeInfoCopy := potentialNodes[(int(offset)+i)%len(potentialNodes)].Clone()
		stateCopy := ev.State.Clone()
		// 2.选择出所有可以被驱逐的Pod节点列表
		pods, numPDBViolations, status := ev.SelectVictimsOnNode(ctx, stateCopy, pod, nodeInfoCopy, pdbs)
		// 3.如果查找成功，并且找出的受害者Pod数量不等于0
		if status.IsSuccess() && len(pods) != 0 {
			victims := extenderv1.Victims{
				Pods:             pods,
				NumPDBViolations: int64(numPDBViolations),
			}
			c := &candidate{
				victims: &victims,
				name:    nodeInfoCopy.Node().Name,
			}
			if numPDBViolations == 0 {
				nonViolatingCandidates.add(c)
			} else {
				violatingCandidates.add(c)
			}
			nvcSize, vcSize := nonViolatingCandidates.size(), violatingCandidates.size()
			if nvcSize > 0 && nvcSize+vcSize >= numCandidates {
				cancel()
			}
			return
		}
		// 4.如果查找成功，但找出的受害者数量=0
		if status.IsSuccess() && len(pods) == 0 {
			status = framework.AsStatus(fmt.Errorf("expected at least one victim pod on node %q", nodeInfoCopy.Node().Name))
		}
		// 更新错误信息
		statusesLock.Lock()
		if status.Code() == framework.Error {
			errs = append(errs, status.AsError())
		}
		nodeStatuses[nodeInfoCopy.Node().Name] = status
		statusesLock.Unlock()
	}
	// 并发执行该函数
	fh.Parallelizer().Until(parallelCtx, len(potentialNodes), checkNode)
	// 将结果返回
	return append(nonViolatingCandidates.get(), violatingCandidates.get()...), nodeStatuses, utilerrors.NewAggregate(errs)
}
