/*
Copyright 2020 The Kubernetes Authors.

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

package defaultpreemption

import (
	"context"
	"fmt"
	"math/rand"
	"sort"

	v1 "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	corelisters "k8s.io/client-go/listers/core/v1"
	policylisters "k8s.io/client-go/listers/policy/v1"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/klog/v2"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/validation"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	"k8s.io/kubernetes/pkg/scheduler/framework/preemption"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	"k8s.io/kubernetes/pkg/scheduler/util"
)

// Name of the plugin used in the plugin registry and configurations.
const Name = names.DefaultPreemption

// DefaultPreemption is a PostFilter plugin implements the preemption logic.
type DefaultPreemption struct {
	fh        framework.Handle
	args      config.DefaultPreemptionArgs
	podLister corelisters.PodLister
	pdbLister policylisters.PodDisruptionBudgetLister
}

var _ framework.PostFilterPlugin = &DefaultPreemption{}

// Name returns name of the plugin. It is used in logs, etc.
func (pl *DefaultPreemption) Name() string {
	return Name
}

// New initializes a new plugin and returns it.
func New(dpArgs runtime.Object, fh framework.Handle, fts feature.Features) (framework.Plugin, error) {
	args, ok := dpArgs.(*config.DefaultPreemptionArgs)
	if !ok {
		return nil, fmt.Errorf("got args of type %T, want *DefaultPreemptionArgs", dpArgs)
	}
	if err := validation.ValidateDefaultPreemptionArgs(nil, args); err != nil {
		return nil, err
	}
	pl := DefaultPreemption{
		fh:        fh,
		args:      *args,
		podLister: fh.SharedInformerFactory().Core().V1().Pods().Lister(),
		pdbLister: getPDBLister(fh.SharedInformerFactory()),
	}
	return &pl, nil
}

// PostFilter invoked at the postFilter extension point.
func (pl *DefaultPreemption) PostFilter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, m framework.NodeToStatusMap) (*framework.PostFilterResult, *framework.Status) {
	defer func() {
		metrics.PreemptionAttempts.Inc()
	}()

	pe := preemption.Evaluator{
		PluginName: names.DefaultPreemption,
		Handler:    pl.fh,
		PodLister:  pl.podLister,
		PdbLister:  pl.pdbLister,
		State:      state,
		Interface:  pl,
	}

	result, status := pe.Preempt(ctx, pod, m)
	if status.Message() != "" {
		return result, framework.NewStatus(status.Code(), "preemption: "+status.Message())
	}
	return result, status
}

// calculateNumCandidates returns the number of candidates the FindCandidates
// method must produce from dry running based on the constraints given by
// <minCandidateNodesPercentage> and <minCandidateNodesAbsolute>. The number of
// candidates returned will never be greater than <numNodes>.
func (pl *DefaultPreemption) calculateNumCandidates(numNodes int32) int32 {
	n := (numNodes * pl.args.MinCandidateNodesPercentage) / 100
	if n < pl.args.MinCandidateNodesAbsolute {
		n = pl.args.MinCandidateNodesAbsolute
	}
	if n > numNodes {
		n = numNodes
	}
	return n
}

// GetOffsetAndNumCandidates chooses a random offset and calculates the number
// of candidates that should be shortlisted for dry running preemption.
func (pl *DefaultPreemption) GetOffsetAndNumCandidates(numNodes int32) (int32, int32) {
	return rand.Int31n(numNodes), pl.calculateNumCandidates(numNodes)
}

// This function is not applicable for out-of-tree preemption plugins that exercise
// different preemption candidates on the same nominated node.
// 此功能不适用于在同一指定节点上执行不同抢占候选的树外抢占插件。

//
// CandidatesToVictimsMap
//  @Description:
//  	建立宿主机节点与其上的受害者Pod的映射关系
//  @receiver pl *DefaultPreemption 对象
//  @param candidates 候选者宿主机节点
//  @return map[string]*extenderv1.Victims 返回宿主机node和受害者节点Pod的映射关系map
//
func (pl *DefaultPreemption) CandidatesToVictimsMap(candidates []preemption.Candidate) map[string]*extenderv1.Victims {
	m := make(map[string]*extenderv1.Victims)
	for _, c := range candidates {
		m[c.Name()] = c.Victims()
	}
	return m
}

// SelectVictimsOnNode finds minimum set of pods on the given node that should be preempted in order to make enough room
// for "pod" to be scheduled.
// 在给定节点上找到应该被抢占的最小Pod集，以便为计划“Pod”留出足够的空间。

//
// SelectVictimsOnNode
//  @Description:
//  	在被抢占的宿主机上选择需要驱逐的Pod
//  @receiver pl
//  @param ctx
//  @param state
//  @param pod
//  @param nodeInfo
//  @param pdbs
//  @return []*v1.Pod
//  @return int
//  @return *framework.Status
//
func (pl *DefaultPreemption) SelectVictimsOnNode(
	ctx context.Context,
	state *framework.CycleState,
	pod *v1.Pod,
	nodeInfo *framework.NodeInfo,
	pdbs []*policy.PodDisruptionBudget) ([]*v1.Pod, int, *framework.Status) {
	var potentialVictims []*framework.PodInfo
	removePod := func(rpi *framework.PodInfo) error {
		if err := nodeInfo.RemovePod(rpi.Pod); err != nil {
			return err
		}
		status := pl.fh.RunPreFilterExtensionRemovePod(ctx, state, pod, rpi, nodeInfo)
		if !status.IsSuccess() {
			return status.AsError()
		}
		return nil
	}
	addPod := func(api *framework.PodInfo) error {
		nodeInfo.AddPodInfo(api)
		status := pl.fh.RunPreFilterExtensionAddPod(ctx, state, pod, api, nodeInfo)
		if !status.IsSuccess() {
			return status.AsError()
		}
		return nil
	}
	// As the first step, remove all the lower priority pods from the node and
	// check if the given pod can be scheduled.
	podPriority := corev1helpers.PodPriority(pod)
	for _, pi := range nodeInfo.Pods {
		if corev1helpers.PodPriority(pi.Pod) < podPriority {
			potentialVictims = append(potentialVictims, pi)
			if err := removePod(pi); err != nil {
				return nil, 0, framework.AsStatus(err)
			}
		}
	}

	// No potential victims are found, and so we don't need to evaluate the node again since its state didn't change.
	if len(potentialVictims) == 0 {
		message := fmt.Sprintf("No preemption victims found for incoming pod")
		return nil, 0, framework.NewStatus(framework.UnschedulableAndUnresolvable, message)
	}

	// If the new pod does not fit after removing all the lower priority pods,
	// we are almost done and this node is not suitable for preemption. The only
	// condition that we could check is if the "pod" is failing to schedule due to
	// inter-pod affinity to one or more victims, but we have decided not to
	// support this case for performance reasons. Having affinity to lower
	// priority pods is not a recommended configuration anyway.
	if status := pl.fh.RunFilterPluginsWithNominatedPods(ctx, state, pod, nodeInfo); !status.IsSuccess() {
		return nil, 0, status
	}
	var victims []*v1.Pod
	numViolatingVictim := 0
	sort.Slice(potentialVictims, func(i, j int) bool { return util.MoreImportantPod(potentialVictims[i].Pod, potentialVictims[j].Pod) })
	// Try to reprieve as many pods as possible. We first try to reprieve the PDB
	// violating victims and then other non-violating ones. In both cases, we start
	// from the highest priority victims.
	violatingVictims, nonViolatingVictims := filterPodsWithPDBViolation(potentialVictims, pdbs)
	reprievePod := func(pi *framework.PodInfo) (bool, error) {
		if err := addPod(pi); err != nil {
			return false, err
		}
		status := pl.fh.RunFilterPluginsWithNominatedPods(ctx, state, pod, nodeInfo)
		fits := status.IsSuccess()
		if !fits {
			if err := removePod(pi); err != nil {
				return false, err
			}
			rpi := pi.Pod
			victims = append(victims, rpi)
			klog.V(5).InfoS("Pod is a potential preemption victim on node", "pod", klog.KObj(rpi), "node", klog.KObj(nodeInfo.Node()))
		}
		return fits, nil
	}
	for _, p := range violatingVictims {
		if fits, err := reprievePod(p); err != nil {
			return nil, 0, framework.AsStatus(err)
		} else if !fits {
			numViolatingVictim++
		}
	}
	// Now we try to reprieve non-violating victims.
	for _, p := range nonViolatingVictims {
		if _, err := reprievePod(p); err != nil {
			return nil, 0, framework.AsStatus(err)
		}
	}
	return victims, numViolatingVictim, framework.NewStatus(framework.Success)
}

// PodEligibleToPreemptOthers returns one bool and one string. The bool
// indicates whether this pod should be considered for preempting other pods or
// not. The string includes the reason if this pod isn't eligible.
// If this pod has a preemptionPolicy of Never or has already preempted other
// pods and those are in their graceful termination period, it shouldn't be
// considered for preemption.
// We look at the node that is nominated for this pod and as long as there are
// terminating pods on the node, we don't consider this for preempting more pods.

// PodEligibleToPreemptOthers
//  @Description:
//  	检查当前pod是否需要进行抢占
//  	如果当前的pod已经提名了一个node节点且该节点有pod正在执行优雅退出（正常终止期），那么不应该执行抢占，而是等待原有pod退出后再正常占用
//  @receiver pl *DefaultPreemption 对象
//  @param pod 抢占者Pod
//  @param nominatedNodeStatus 被抢占的宿主机节点信息（用于判断该Pod是否已经抢占过其他宿主机节点）
//  @return bool 是否该考虑此Pod抢占其他Pod，即Pod当前提名节点满足/不满足Pod的抢占，而是否应该尝试抢占到其他宿主机上
//  @return string 抢占不成功的原因
func (pl *DefaultPreemption) PodEligibleToPreemptOthers(pod *v1.Pod, nominatedNodeStatus *framework.Status) (bool, string) {
	if pod.Spec.PreemptionPolicy != nil && *pod.Spec.PreemptionPolicy == v1.PreemptNever {
		return false, fmt.Sprint("not eligible due to preemptionPolicy=Never.")
	}
	nodeInfos := pl.fh.SnapshotSharedLister().NodeInfos()
	// 1.检查pod是否已经有提名node
	nomNodeName := pod.Status.NominatedNodeName
	// 2.如果该Pod已经指定了提名节点
	if len(nomNodeName) > 0 {
		// If the pod's nominated node is considered as UnschedulableAndUnresolvable by the filters,
		// then the pod should be considered for preempting again.
		// 2.1 如果pod的指定节点被过滤器认为是不可调度和不可解析的，则应该考虑再次为该Pod选择宿主机节点执行抢占
		if nominatedNodeStatus.Code() == framework.UnschedulableAndUnresolvable {
			return true, ""
		}
		// 2.2 获取该Pod提名宿主机节点下的所有Pod信息
		if nodeInfo, _ := nodeInfos.Get(nomNodeName); nodeInfo != nil {
			// 2.2.1 检查这个node中是否存在正处于terminating状态的pod，且优先级比当前pod低
			podPriority := corev1helpers.PodPriority(pod)
			// 2.2.2 遍历宿主机中的所有Pod，检查这个node中是否存在正处于terminating状态的pod，且优先级比抢占者pod低
			for _, p := range nodeInfo.Pods {
				if p.Pod.DeletionTimestamp != nil && corev1helpers.PodPriority(p.Pod) < podPriority {
					// There is a terminating pod on the nominated node.
					// 如果指定节点上有一个正在终止的pod，且优先级比抢占者Pod低，则直接就在该提名节点抢占，而不再尝试抢占其他宿主机
					return false, fmt.Sprint("not eligible due to a terminating pod on the nominated node.")
				}
			}
		}
	}
	// 3.如果该宿主机没有指定提名节点，则返回True，表示后续需要为该Pod过滤出一个适合的抢占宿主机节点
	return true, ""
}

// filterPodsWithPDBViolation groups the given "pods" into two groups of "violatingPods"
// and "nonViolatingPods" based on whether their PDBs will be violated if they are
// preempted.
// This function is stable and does not change the order of received pods. So, if it
// receives a sorted list, grouping will preserve the order of the input list.
func filterPodsWithPDBViolation(podInfos []*framework.PodInfo, pdbs []*policy.PodDisruptionBudget) (violatingPodInfos, nonViolatingPodInfos []*framework.PodInfo) {
	pdbsAllowed := make([]int32, len(pdbs))
	for i, pdb := range pdbs {
		pdbsAllowed[i] = pdb.Status.DisruptionsAllowed
	}

	for _, podInfo := range podInfos {
		pod := podInfo.Pod
		pdbForPodIsViolated := false
		// A pod with no labels will not match any PDB. So, no need to check.
		if len(pod.Labels) != 0 {
			for i, pdb := range pdbs {
				if pdb.Namespace != pod.Namespace {
					continue
				}
				selector, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
				if err != nil {
					// This object has an invalid selector, it does not match the pod
					continue
				}
				// A PDB with a nil or empty selector matches nothing.
				if selector.Empty() || !selector.Matches(labels.Set(pod.Labels)) {
					continue
				}

				// Existing in DisruptedPods means it has been processed in API server,
				// we don't treat it as a violating case.
				if _, exist := pdb.Status.DisruptedPods[pod.Name]; exist {
					continue
				}
				// Only decrement the matched pdb when it's not in its <DisruptedPods>;
				// otherwise we may over-decrement the budget number.
				pdbsAllowed[i]--
				// We have found a matching PDB.
				if pdbsAllowed[i] < 0 {
					pdbForPodIsViolated = true
				}
			}
		}
		if pdbForPodIsViolated {
			violatingPodInfos = append(violatingPodInfos, podInfo)
		} else {
			nonViolatingPodInfos = append(nonViolatingPodInfos, podInfo)
		}
	}
	return violatingPodInfos, nonViolatingPodInfos
}

func getPDBLister(informerFactory informers.SharedInformerFactory) policylisters.PodDisruptionBudgetLister {
	return informerFactory.Policy().V1().PodDisruptionBudgets().Lister()
}
