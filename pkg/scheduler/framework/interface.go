/*
Copyright 2019 The Kubernetes Authors.

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

// This file defines the scheduling framework plugin interfaces.

package framework

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/events"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/framework/parallelize"
)

// NodeScoreList declares a list of nodes and their scores.
type NodeScoreList []NodeScore

// NodeScore is a struct with node name and score.
type NodeScore struct {
	Name  string
	Score int64
}

// PluginToNodeScores declares a map from plugin name to its NodeScoreList.
type PluginToNodeScores map[string]NodeScoreList

// NodeToStatusMap declares map from node name to its status.
// 声明从节点名称到其状态的映射
type NodeToStatusMap map[string]*Status

// Code is the Status code/type which is returned from plugins.
type Code int

// These are predefined codes used in a Status.
const (
	// Success means that plugin ran correctly and found pod schedulable.
	// NOTE: A nil status is also considered as "Success".
	Success Code = iota
	// Error is used for internal plugin errors, unexpected input, etc.
	Error
	// Unschedulable is used when a plugin finds a pod unschedulable. The scheduler might attempt to
	// preempt other pods to get this pod scheduled. Use UnschedulableAndUnresolvable to make the
	// scheduler skip preemption.
	// The accompanying status message should explain why the pod is unschedulable.
	// 当插件发现一个pod不可调度时，使用Unschedulable。调度程序可能会尝试抢占其他pod以调度此pod。使用UnsetulableAndUnalyable使调度程序跳过抢占
	// 随附的状态消息应该解释为什么pod不可调度。
	Unschedulable
	// UnschedulableAndUnresolvable is used when a plugin finds a pod unschedulable and
	// preemption would not change anything. Plugins should return Unschedulable if it is possible
	// that the pod can get scheduled with preemption.
	// The accompanying status message should explain why the pod is unschedulable.
	// 当插件发现pod不可调度并且抢占不会更改任何内容时，使用UnschedulableAndUnresolvable。
	// 如果pod可以通过抢占进行调度，插件应该返回Unschedulable
	// 随附的状态消息应该解释为什么pod不可调度
	UnschedulableAndUnresolvable
	// Wait is used when a Permit plugin finds a pod scheduling should wait.
	Wait
	// Skip is used when a Bind plugin chooses to skip binding.
	Skip
)

// This list should be exactly the same as the codes iota defined above in the same order.
var codes = []string{"Success", "Error", "Unschedulable", "UnschedulableAndUnresolvable", "Wait", "Skip"}

// statusPrecedence defines a map from status to its precedence, larger value means higher precedent.
var statusPrecedence = map[Code]int{
	Error:                        3,
	UnschedulableAndUnresolvable: 2,
	Unschedulable:                1,
	// Any other statuses we know today, `Skip` or `Wait`, will take precedence over `Success`.
	Success: -1,
}

func (c Code) String() string {
	return codes[c]
}

const (
	// MaxNodeScore is the maximum score a Score plugin is expected to return.
	MaxNodeScore int64 = 100

	// MinNodeScore is the minimum score a Score plugin is expected to return.
	MinNodeScore int64 = 0

	// MaxTotalScore is the maximum total score.
	MaxTotalScore int64 = math.MaxInt64
)

// PodsToActivateKey is a reserved state key for stashing pods.
// If the stashed pods are present in unschedulablePods or backoffQ，they will be
// activated (i.e., moved to activeQ) in two phases:
// - end of a scheduling cycle if it succeeds (will be cleared from `PodsToActivate` if activated)
// - end of a binding cycle if it succeeds
var PodsToActivateKey StateKey = "kubernetes.io/pods-to-activate"

// PodsToActivate stores pods to be activated.
// 存储将要放到activeQ队列中的Pods
type PodsToActivate struct {
	sync.Mutex
	// Map is keyed with namespaced pod name, and valued with the pod.
	Map map[string]*v1.Pod
}

// Clone just returns the same state.
func (s *PodsToActivate) Clone() StateData {
	return s
}

// NewPodsToActivate instantiates a PodsToActivate object.
// 实例化一个PodsToActivate结构体对象
func NewPodsToActivate() *PodsToActivate {
	return &PodsToActivate{Map: make(map[string]*v1.Pod)}
}

// Status indicates the result of running a plugin. It consists of a code, a
// message, (optionally) an error, and a plugin name it fails by.
// When the status code is not Success, the reasons should explain why.
// And, when code is Success, all the other fields should be empty.
// NOTE: A nil Status is also considered as Success.
// 状态表示运行插件的结果。它由代码、消息、（可选）错误和它失败的插件名称组成。
// 当状态码不是成功时，原因应该解释为什么。
// 并且，当代码为成功时，所有其他字段都应该为空。
// 注意：nil状态也被视为成功。
type Status struct {
	code    Code
	reasons []string
	err     error
	// failedPlugin is an optional field that records the plugin name a Pod failed by.
	// It's set by the framework when code is Error, Unschedulable or UnschedulableAndUnresolvable.
	// FailedPlugin是一个可选字段，用于记录Pod失败的插件名称
	// 它是由框架在代码为错误、不可调度或不可调度和不可解析时设置的
	failedPlugin string
}

// Code returns code of the Status.
func (s *Status) Code() Code {
	if s == nil {
		return Success
	}
	return s.code
}

// Message returns a concatenated message on reasons of the Status.
func (s *Status) Message() string {
	if s == nil {
		return ""
	}
	return strings.Join(s.reasons, ", ")
}

// SetFailedPlugin sets the given plugin name to s.failedPlugin.
func (s *Status) SetFailedPlugin(plugin string) {
	s.failedPlugin = plugin
}

// WithFailedPlugin sets the given plugin name to s.failedPlugin,
// and returns the given status object.
func (s *Status) WithFailedPlugin(plugin string) *Status {
	s.SetFailedPlugin(plugin)
	return s
}

// FailedPlugin returns the failed plugin name.
func (s *Status) FailedPlugin() string {
	return s.failedPlugin
}

// Reasons returns reasons of the Status.
func (s *Status) Reasons() []string {
	return s.reasons
}

// AppendReason appends given reason to the Status.
func (s *Status) AppendReason(reason string) {
	s.reasons = append(s.reasons, reason)
}

// IsSuccess returns true if and only if "Status" is nil or Code is "Success".
func (s *Status) IsSuccess() bool {
	return s.Code() == Success
}

// IsWait returns true if and only if "Status" is non-nil and its Code is "Wait".
func (s *Status) IsWait() bool {
	return s.Code() == Wait
}

// IsSkip returns true if and only if "Status" is non-nil and its Code is "Skip".
func (s *Status) IsSkip() bool {
	return s.Code() == Skip
}

// IsUnschedulable returns true if "Status" is Unschedulable (Unschedulable or UnschedulableAndUnresolvable).
func (s *Status) IsUnschedulable() bool {
	code := s.Code()
	return code == Unschedulable || code == UnschedulableAndUnresolvable
}

// AsError returns nil if the status is a success or a wait; otherwise returns an "error" object
// with a concatenated message on reasons of the Status.
func (s *Status) AsError() error {
	if s.IsSuccess() || s.IsWait() || s.IsSkip() {
		return nil
	}
	if s.err != nil {
		return s.err
	}
	return errors.New(s.Message())
}

// Equal checks equality of two statuses. This is useful for testing with
// cmp.Equal.
func (s *Status) Equal(x *Status) bool {
	if s == nil || x == nil {
		return s.IsSuccess() && x.IsSuccess()
	}
	if s.code != x.code {
		return false
	}
	if s.code == Error {
		return cmp.Equal(s.err, x.err, cmpopts.EquateErrors())
	}
	return cmp.Equal(s.reasons, x.reasons)
}

// NewStatus makes a Status out of the given arguments and returns its pointer.
func NewStatus(code Code, reasons ...string) *Status {
	s := &Status{
		code:    code,
		reasons: reasons,
	}
	if code == Error {
		s.err = errors.New(s.Message())
	}
	return s
}

// AsStatus wraps an error in a Status.
func AsStatus(err error) *Status {
	return &Status{
		code:    Error,
		reasons: []string{err.Error()},
		err:     err,
	}
}

// PluginToStatus maps plugin name to status. Currently used to identify which Filter plugin
// returned which status.
type PluginToStatus map[string]*Status

// Merge merges the statuses in the map into one. The resulting status code have the following
// precedence: Error, UnschedulableAndUnresolvable, Unschedulable.
func (p PluginToStatus) Merge() *Status {
	if len(p) == 0 {
		return nil
	}

	finalStatus := NewStatus(Success)
	for _, s := range p {
		if s.Code() == Error {
			finalStatus.err = s.AsError()
		}
		if statusPrecedence[s.Code()] > statusPrecedence[finalStatus.code] {
			finalStatus.code = s.Code()
			// Same as code, we keep the most relevant failedPlugin in the returned Status.
			finalStatus.failedPlugin = s.FailedPlugin()
		}

		for _, r := range s.reasons {
			finalStatus.AppendReason(r)
		}
	}

	return finalStatus
}

// WaitingPod represents a pod currently waiting in the permit phase.
type WaitingPod interface {
	// GetPod returns a reference to the waiting pod.
	GetPod() *v1.Pod
	// GetPendingPlugins returns a list of pending Permit plugin's name.
	GetPendingPlugins() []string
	// Allow declares the waiting pod is allowed to be scheduled by the plugin named as "pluginName".
	// If this is the last remaining plugin to allow, then a success signal is delivered
	// to unblock the pod.
	Allow(pluginName string)
	// Reject declares the waiting pod unschedulable.
	Reject(pluginName, msg string)
}

// Plugin is the parent type for all the scheduling framework plugins.
// 是所有调度框架插件的父类型
type Plugin interface {
	Name() string
}

// LessFunc is the function to sort pod info
type LessFunc func(podInfo1, podInfo2 *QueuedPodInfo) bool

// QueueSortPlugin is an interface that must be implemented by "QueueSort" plugins.
// These plugins are used to sort pods in the scheduling queue. Only one queue sort
// plugin may be enabled at a time.
type QueueSortPlugin interface {
	Plugin
	// Less are used to sort pods in the scheduling queue.
	Less(*QueuedPodInfo, *QueuedPodInfo) bool
}

// EnqueueExtensions is an optional interface that plugins can implement to efficiently
// move unschedulable Pods in internal scheduling queues. Plugins
// that fail pod scheduling (e.g., Filter plugins) are expected to implement this interface.
type EnqueueExtensions interface {
	// EventsToRegister returns a series of possible events that may cause a Pod
	// failed by this plugin schedulable.
	// The events will be registered when instantiating the internal scheduling queue,
	// and leveraged to build event handlers dynamically.
	// Note: the returned list needs to be static (not depend on configuration parameters);
	// otherwise it would lead to undefined behavior.
	EventsToRegister() []ClusterEvent
}

// PreFilterExtensions is an interface that is included in plugins that allow specifying
// callbacks to make incremental updates to its supposedly pre-calculated
// state.
type PreFilterExtensions interface {
	// AddPod is called by the framework while trying to evaluate the impact
	// of adding podToAdd to the node while scheduling podToSchedule.
	AddPod(ctx context.Context, state *CycleState, podToSchedule *v1.Pod, podInfoToAdd *PodInfo, nodeInfo *NodeInfo) *Status
	// RemovePod is called by the framework while trying to evaluate the impact
	// of removing podToRemove from the node while scheduling podToSchedule.
	RemovePod(ctx context.Context, state *CycleState, podToSchedule *v1.Pod, podInfoToRemove *PodInfo, nodeInfo *NodeInfo) *Status
}

// PreFilterPlugin is an interface that must be implemented by "PreFilter" plugins.
// These plugins are called at the beginning of the scheduling cycle.
type PreFilterPlugin interface {
	Plugin
	// PreFilter is called at the beginning of the scheduling cycle. All PreFilter
	// plugins must return success or the pod will be rejected. PreFilter could optionally
	// return a PreFilterResult to influence which nodes to evaluate downstream. This is useful
	// for cases where it is possible to determine the subset of nodes to process in O(1) time.
	PreFilter(ctx context.Context, state *CycleState, p *v1.Pod) (*PreFilterResult, *Status)
	// PreFilterExtensions returns a PreFilterExtensions interface if the plugin implements one,
	// or nil if it does not. A Pre-filter plugin can provide extensions to incrementally
	// modify its pre-processed info. The framework guarantees that the extensions
	// AddPod/RemovePod will only be called after PreFilter, possibly on a cloned
	// CycleState, and may call those functions more than once before calling
	// Filter again on a specific node.
	PreFilterExtensions() PreFilterExtensions
}

// FilterPlugin is an interface for Filter plugins. These plugins are called at the
// filter extension point for filtering out hosts that cannot run a pod.
// This concept used to be called 'predicate' in the original scheduler.
// These plugins should return "Success", "Unschedulable" or "Error" in Status.code.
// However, the scheduler accepts other valid codes as well.
// Anything other than "Success" will lead to exclusion of the given host from
// running the pod.
// 是过滤器插件的接口。这些插件在过滤器扩展点调用，用于过滤掉无法运行pod的主机。
// 这个概念过去在原始调度程序中称为“谓词”。
// 这些插件应该在S中返回“成功”、“不可调度”或“错误”tatus.code.
// 但是，调度程序也接受其他有效代码。
// 除了“成功”之外的任何内容都将导致给定主机被排除在运行pod之外。
// 过滤器应该实现的接口
type FilterPlugin interface {
	//
	Plugin
	// Filter is called by the scheduling framework.
	// All FilterPlugins should return "Success" to declare that
	// the given node fits the pod. If Filter doesn't return "Success",
	// it will return "Unschedulable", "UnschedulableAndUnresolvable" or "Error".
	// For the node being evaluated, Filter plugins should look at the passed
	// nodeInfo reference for this particular node's information (e.g., pods
	// considered to be running on the node) instead of looking it up in the
	// NodeInfoSnapshot because we don't guarantee that they will be the same.
	// For example, during preemption, we may pass a copy of the original
	// nodeInfo object that has some pods removed from it to evaluate the
	// possibility of preempting them to schedule the target pod.
	// Filter由调度框架调用。
	// 所有FilterPlugins都应返回“成功”以声明给定节点适合pod。如果Filter不返回“成功”，它将返回“不可调度”、“不可调度”或“错误”。
	// 对于正在评估的节点，Filter插件应该查看传递的nodeInfo引用以获取该特定节点的信息（例如，被认为在节点上运行的pod），而不是在
	// NodeInfoSnapshot，因为我们不保证它们是相同的。
	// 例如，在抢占期间，我们可能会传递原始nodeInfo对象的副本，该对象已从其中删除了一些pod，以评估抢占它们以调度目标pod的可能性。
	Filter(ctx context.Context, state *CycleState, pod *v1.Pod, nodeInfo *NodeInfo) *Status
}

// PostFilterPlugin is an interface for "PostFilter" plugins. These plugins are called
// after a pod cannot be scheduled.
type PostFilterPlugin interface {
	Plugin
	// PostFilter is called by the scheduling framework.
	// A PostFilter plugin should return one of the following statuses:
	// - Unschedulable: the plugin gets executed successfully but the pod cannot be made schedulable.
	// - Success: the plugin gets executed successfully and the pod can be made schedulable.
	// - Error: the plugin aborts due to some internal error.
	//
	// Informational plugins should be configured ahead of other ones, and always return Unschedulable status.
	// Optionally, a non-nil PostFilterResult may be returned along with a Success status. For example,
	// a preemption plugin may choose to return nominatedNodeName, so that framework can reuse that to update the
	// preemptor pod's .spec.status.nominatedNodeName field.
	PostFilter(ctx context.Context, state *CycleState, pod *v1.Pod, filteredNodeStatusMap NodeToStatusMap) (*PostFilterResult, *Status)
}

// PreScorePlugin is an interface for "PreScore" plugin. PreScore is an
// informational extension point. Plugins will be called with a list of nodes
// that passed the filtering phase. A plugin may use this data to update internal
// state or to generate logs/metrics.
type PreScorePlugin interface {
	Plugin
	// PreScore is called by the scheduling framework after a list of nodes
	// passed the filtering phase. All prescore plugins must return success or
	// the pod will be rejected
	PreScore(ctx context.Context, state *CycleState, pod *v1.Pod, nodes []*v1.Node) *Status
}

// ScoreExtensions is an interface for Score extended functionality.
type ScoreExtensions interface {
	// NormalizeScore is called for all node scores produced by the same plugin's "Score"
	// method. A successful run of NormalizeScore will update the scores list and return
	// a success status.
	NormalizeScore(ctx context.Context, state *CycleState, p *v1.Pod, scores NodeScoreList) *Status
}

// ScorePlugin is an interface that must be implemented by "Score" plugins to rank
// nodes that passed the filtering phase.
type ScorePlugin interface {
	Plugin
	// Score is called on each filtered node. It must return success and an integer
	// indicating the rank of the node. All scoring plugins must return success or
	// the pod will be rejected.
	Score(ctx context.Context, state *CycleState, p *v1.Pod, nodeName string) (int64, *Status)

	// ScoreExtensions returns a ScoreExtensions interface if it implements one, or nil if does not.
	ScoreExtensions() ScoreExtensions
}

// ReservePlugin is an interface for plugins with Reserve and Unreserve
// methods. These are meant to update the state of the plugin. This concept
// used to be called 'assume' in the original scheduler. These plugins should
// return only Success or Error in Status.code. However, the scheduler accepts
// other valid codes as well. Anything other than Success will lead to
// rejection of the pod.
type ReservePlugin interface {
	Plugin
	// Reserve is called by the scheduling framework when the scheduler cache is
	// updated. If this method returns a failed Status, the scheduler will call
	// the Unreserve method for all enabled ReservePlugins.
	Reserve(ctx context.Context, state *CycleState, p *v1.Pod, nodeName string) *Status
	// Unreserve is called by the scheduling framework when a reserved pod was
	// rejected, an error occurred during reservation of subsequent plugins, or
	// in a later phase. The Unreserve method implementation must be idempotent
	// and may be called by the scheduler even if the corresponding Reserve
	// method for the same plugin was not called.
	Unreserve(ctx context.Context, state *CycleState, p *v1.Pod, nodeName string)
}

// PreBindPlugin is an interface that must be implemented by "PreBind" plugins.
// These plugins are called before a pod being scheduled.
type PreBindPlugin interface {
	Plugin
	// PreBind is called before binding a pod. All prebind plugins must return
	// success or the pod will be rejected and won't be sent for binding.
	PreBind(ctx context.Context, state *CycleState, p *v1.Pod, nodeName string) *Status
}

// PostBindPlugin is an interface that must be implemented by "PostBind" plugins.
// These plugins are called after a pod is successfully bound to a node.
type PostBindPlugin interface {
	Plugin
	// PostBind is called after a pod is successfully bound. These plugins are
	// informational. A common application of this extension point is for cleaning
	// up. If a plugin needs to clean-up its state after a pod is scheduled and
	// bound, PostBind is the extension point that it should register.
	PostBind(ctx context.Context, state *CycleState, p *v1.Pod, nodeName string)
}

// PermitPlugin is an interface that must be implemented by "Permit" plugins.
// These plugins are called before a pod is bound to a node.
type PermitPlugin interface {
	Plugin
	// Permit is called before binding a pod (and before prebind plugins). Permit
	// plugins are used to prevent or delay the binding of a Pod. A permit plugin
	// must return success or wait with timeout duration, or the pod will be rejected.
	// The pod will also be rejected if the wait timeout or the pod is rejected while
	// waiting. Note that if the plugin returns "wait", the framework will wait only
	// after running the remaining plugins given that no other plugin rejects the pod.
	Permit(ctx context.Context, state *CycleState, p *v1.Pod, nodeName string) (*Status, time.Duration)
}

// BindPlugin is an interface that must be implemented by "Bind" plugins. Bind
// plugins are used to bind a pod to a Node.
type BindPlugin interface {
	Plugin
	// Bind plugins will not be called until all pre-bind plugins have completed. Each
	// bind plugin is called in the configured order. A bind plugin may choose whether
	// or not to handle the given Pod. If a bind plugin chooses to handle a Pod, the
	// remaining bind plugins are skipped. When a bind plugin does not handle a pod,
	// it must return Skip in its Status code. If a bind plugin returns an Error, the
	// pod is rejected and will not be bound.
	Bind(ctx context.Context, state *CycleState, p *v1.Pod, nodeName string) *Status
}

// Framework manages the set of plugins in use by the scheduling framework.
// Configured plugins are called at specified points in a scheduling context.
// 管理调度框架正在使用的插件集，在调度上下文中的指定点调用配置的插件

//
// Framework
//  @Description:
//  	管理调度整个流程中的扩展点
//  	即：一个实现了该framework接口的结构体实例化对象，就是为某一个Pod指定的所有拓展点的具体插件
//  	该接口只是定义了有哪些拓展点，拓展点中的插件函数格式是什么样子的
//  	然后通过为不同场景Pod定义具体的不同的framework结构体，实现该接口中的所有抽象方法，来实现该接口，用于后续贯穿该Pod的整个调度流程
//
type Framework interface {
	// Handle
	// 提名管理、score函数、
	Handle
	// QueueSortFunc returns the function to sort pods in scheduling queue
	// 返回在调度队列中对pod进行排序的函数
	// Sort插件，Pod队列排序函数
	QueueSortFunc() LessFunc

	// RunPreFilterPlugins runs the set of configured PreFilter plugins. It returns
	// *Status and its code is set to non-success if any of the plugins returns
	// anything but Success. If a non-success status is returned, then the scheduling
	// cycle is aborted.
	// It also returns a PreFilterResult, which may influence what or how many nodes to
	// evaluate downstream.
	// 运行一组已配置的PreFilter插件。它返回*状态，如果任何插件返回除成功之外的任何内容，则其代码设置为非成功。
	// 如果返回非成功状态，则调度周期中止
	// 它还返回一个PreFilterResult，这可能会影响下游评估什么或多少节点
	// PreFilter插件，运行过滤节点前的函数
	RunPreFilterPlugins(ctx context.Context, state *CycleState, pod *v1.Pod) (*PreFilterResult, *Status)

	// RunPostFilterPlugins runs the set of configured PostFilter plugins.
	// PostFilter plugins can either be informational, in which case should be configured
	// to execute first and return Unschedulable status, or ones that try to change the
	// cluster state to make the pod potentially schedulable in a future scheduling cycle.
	// 运行一组配置的PostFilter插件。
	// 该插件可以是信息性的，在这种情况下应配置为首先执行并返回不可调度状态，或者尝试更改集群状态以使pod在未来的调度周期中具有潜在的可调度性。
	// PostFilter插件，运行过滤节点后的函数
	RunPostFilterPlugins(ctx context.Context, state *CycleState, pod *v1.Pod, filteredNodeStatusMap NodeToStatusMap) (*PostFilterResult, *Status)

	// RunPreBindPlugins runs the set of configured PreBind plugins. It returns
	// *Status and its code is set to non-success if any of the plugins returns
	// anything but Success. If the Status code is "Unschedulable", it is
	// considered as a scheduling check failure, otherwise, it is considered as an
	// internal error. In either case the pod is not going to be bound.
	// 运行配置的PreBind插件集。如果任何插件返回除成功之外的任何内容，它将返回*status，其代码设置为non-成功。
	// 如果状态代码为“不可调度”，则将其视为调度检查失败，否则将其视为内部错误。在任何一种情况下，pod都不会被绑定。
	// PreBind插件，执行绑定前的函数
	RunPreBindPlugins(ctx context.Context, state *CycleState, pod *v1.Pod, nodeName string) *Status

	// RunPostBindPlugins runs the set of configured PostBind plugins.
	// PostBind插件，运行已配置的PostBind插件集
	RunPostBindPlugins(ctx context.Context, state *CycleState, pod *v1.Pod, nodeName string)

	// RunReservePluginsReserve runs the Reserve method of the set of
	// configured Reserve plugins. If any of these calls returns an error, it
	// does not continue running the remaining ones and returns the error. In
	// such case, pod will not be scheduled.
	// 运行一组配置的保留插件的保留方法。如果这些调用中的任何一个返回错误，它不会继续运行其余的调用并返回错误。在这种情况下，将不会安排pod
	// Reserve插件，假性绑定，运行Reserve资源预留插件，在k8s-scheduler内存中将Pod和node进行绑定并对内存资源视图进行扣减
	RunReservePluginsReserve(ctx context.Context, state *CycleState, pod *v1.Pod, nodeName string) *Status

	// RunReservePluginsUnreserve runs the Unreserve method of the set of configured Reserve plugins.
	// 运行一组已配置的保留插件的取消保留方法
	// 绑定回滚，执行Unreserve插件，将资源视图回滚到假性绑定前的状态
	// 只要在Permit到Bind阶段有一个阶段出现问题，就将资源视图回滚到假性绑定前的状态
	RunReservePluginsUnreserve(ctx context.Context, state *CycleState, pod *v1.Pod, nodeName string)

	// RunPermitPlugins runs the set of configured Permit plugins. If any of these
	// plugins returns a status other than "Success" or "Wait", it does not continue
	// running the remaining plugins and returns an error. Otherwise, if any of the
	// plugins returns "Wait", then this function will create and add waiting pod
	// to a map of currently waiting pods and return status with "Wait" code.
	// Pod will remain waiting pod for the minimum duration returned by the Permit plugins.
	// 运行配置的Permit插件集。如果这些插件中的任何一个返回“成功”或“等待”以外的状态，它不会继续运行剩余的插件并返回错误。
	// 否则，如果任何插件返回“等待”，则此函数将创建等待pod并将其添加到当前等待pod的映射中，并使用“等待”代码返回状态。
	// Pod将在Permit插件返回的最短持续时间内保持等待pod。
	// Permit插件，实现一些阻止或延迟Pod与node绑定的操作，并根据条件对假性绑定关系返回allow、reject或wait
	RunPermitPlugins(ctx context.Context, state *CycleState, pod *v1.Pod, nodeName string) *Status

	// WaitOnPermit will block, if the pod is a waiting pod, until the waiting pod is rejected or allowed.
	// 如果pod是wait状态，则将阻塞，直到等待pod变成allow或reject状态
	WaitOnPermit(ctx context.Context, pod *v1.Pod) *Status

	// RunBindPlugins runs the set of configured Bind plugins. A Bind plugin may choose
	// whether or not to handle the given Pod. If a Bind plugin chooses to skip the
	// binding, it should return code=5("skip") status. Otherwise, it should return "Error"
	// or "Success". If none of the plugins handled binding, RunBindPlugins returns
	// code=5("skip") status.
	// 运行配置的Bind插件集。Bind插件可以选择是否处理给定的Pod。
	// 如果Bind插件选择跳过绑定，它应该返回code=5（“跳过”）状态。否则，它应该返回“错误”或“成功”。
	// 如果没有插件处理绑定，RunBindPlugins返回code=5（“跳过”）状态。
	// Bind插件，执行实际的绑定操作
	RunBindPlugins(ctx context.Context, state *CycleState, pod *v1.Pod, nodeName string) *Status

	// HasFilterPlugins returns true if at least one Filter plugin is defined.
	// 如果至少定义了一个Filter插件，则返回true
	HasFilterPlugins() bool

	// HasPostFilterPlugins returns true if at least one PostFilter plugin is defined.
	// 如果至少指定了一个PostFilter插件，则返回true
	HasPostFilterPlugins() bool

	// HasScorePlugins returns true if at least one Score plugin is defined.
	// 如果至少定义了一个Score插件，则返回true
	HasScorePlugins() bool

	// ListPlugins returns a map of extension point name to list of configured Plugins.
	// 扩展点名称与插件集的映射
	ListPlugins() *config.Plugins

	// ProfileName returns the profile name associated to this framework.
	// 返回与此框架关联的配置文件名称
	// KubeSchedulerProfile.SchedulerName = Framework.profileName
	ProfileName() string
}

// Handle provides data and some tools that plugins can use. It is
// passed to the plugin factories at the time of plugin initialization. Plugins
// must store and use this handle to call framework functions.
// 提供插件可以使用的数据和一些工具。
// 它在插件初始化时传递给插件工厂。插件必须存储并使用此句柄来调用框架函数
type Handle interface {
	// PodNominator abstracts operations to maintain nominated Pods.
	// Pod对宿主机node提名的管理
	PodNominator
	// PluginsRunner abstracts operations to run some plugins.
	// 定义一些过滤Filter和打分Score的插件
	PluginsRunner
	// SnapshotSharedLister returns listers from the latest NodeInfo Snapshot. The snapshot
	// is taken at the beginning of a scheduling cycle and remains unchanged until
	// a pod finishes "Permit" point. There is no guarantee that the information
	// remains unchanged in the binding phase of scheduling, so plugins in the binding
	// cycle (pre-bind/bind/post-bind/un-reserve plugin) should not use it,
	// otherwise a concurrent read/write error might occur, they should use scheduler
	// cache instead.
	// 返回来自最新NodeInfo快照的列表。
	// 快照是在调度周期开始时拍摄的，并且保持不变，直到pod完成“许可”点。
	// 不能保证信息在调度的绑定阶段保持不变，因此绑定周期中的插件（pre-bind/bind/post-bind/un-储备插件）不应使用它，否则可能会发生并发读/写错误，它们应该使用调度程序缓存。
	// 所有宿主机节点的快照列表
	SnapshotSharedLister() SharedLister

	// IterateOverWaitingPods acquires a read lock and iterates over the WaitingPods map.
	// 获取读取锁并遍历WaitingPods映射
	// 获取下一个waiting状态的Pod
	IterateOverWaitingPods(callback func(WaitingPod))

	// GetWaitingPod returns a waiting pod given its UID.
	// 返回给定UID的waiting的pod
	GetWaitingPod(uid types.UID) WaitingPod

	// RejectWaitingPod rejects a waiting pod given its UID.
	// The return value indicates if the pod is waiting or not.
	// 拒绝给定UID的waiting状态的pod。
	// 返回值指示pod是否正在等待。
	RejectWaitingPod(uid types.UID) bool

	// ClientSet returns a kubernetes clientSet.
	// 返回k8s客户端配置
	ClientSet() clientset.Interface

	// KubeConfig returns the raw kube config.
	// 返回原始k8s配置
	KubeConfig() *restclient.Config

	// EventRecorder returns an event recorder.
	// 返回事件记录器
	EventRecorder() events.EventRecorder

	SharedInformerFactory() informers.SharedInformerFactory

	// RunFilterPluginsWithNominatedPods runs the set of configured filter plugins for nominated pod on the given node.
	// 为给定宿主机节点上的指定pod运行一组配置的过滤器插件
	RunFilterPluginsWithNominatedPods(ctx context.Context, state *CycleState, pod *v1.Pod, info *NodeInfo) *Status

	// Extenders returns registered scheduler extenders.
	// 返回已注册的调度程序扩展器
	Extenders() []Extender

	// Parallelizer returns a parallelizer holding parallelism for scheduler.
	// 返回一个并行程序，该并行程序保持调度程序的并行性
	Parallelizer() parallelize.Parallelizer
}

// PreFilterResult wraps needed info for scheduler framework to act upon PreFilter phase.
type PreFilterResult struct {
	// The set of nodes that should be considered downstream; if nil then
	// all nodes are eligible.
	NodeNames sets.String
}

func (p *PreFilterResult) AllNodes() bool {
	return p == nil || p.NodeNames == nil
}

func (p *PreFilterResult) Merge(in *PreFilterResult) *PreFilterResult {
	if p.AllNodes() && in.AllNodes() {
		return nil
	}

	r := PreFilterResult{}
	if p.AllNodes() {
		r.NodeNames = sets.NewString(in.NodeNames.UnsortedList()...)
		return &r
	}
	if in.AllNodes() {
		r.NodeNames = sets.NewString(p.NodeNames.UnsortedList()...)
		return &r
	}

	r.NodeNames = p.NodeNames.Intersection(in.NodeNames)
	return &r
}

type NominatingMode int

const (
	ModeNoop NominatingMode = iota
	ModeOverride
)

type NominatingInfo struct {
	NominatedNodeName string
	NominatingMode    NominatingMode
}

// PostFilterResult wraps needed info for scheduler framework to act upon PostFilter phase.
type PostFilterResult struct {
	*NominatingInfo
}

func NewPostFilterResultWithNominatedNode(name string) *PostFilterResult {
	return &PostFilterResult{
		NominatingInfo: &NominatingInfo{
			NominatedNodeName: name,
			NominatingMode:    ModeOverride,
		},
	}
}

func (ni *NominatingInfo) Mode() NominatingMode {
	if ni == nil {
		return ModeNoop
	}
	return ni.NominatingMode
}

// PodNominator abstracts operations to maintain nominated Pods.
// 抽象维护指定Pods的操作
// Pod对节点的提名管理
type PodNominator interface {
	// AddNominatedPod adds the given pod to the nominator or updates it if it already exists.
	// 增，将给定的pod添加到提名器或更新它（如果它已经存在）
	AddNominatedPod(pod *PodInfo, nominatingInfo *NominatingInfo)
	// DeleteNominatedPodIfExists deletes nominatedPod from internal cache. It's a no-op if it doesn't exist.
	// 删，从内部缓存中删除已完成提名的Pod。如果它不存在，则为无操作。
	DeleteNominatedPodIfExists(pod *v1.Pod)
	// UpdateNominatedPod updates the <oldPod> with <newPod>.
	// 改，更新缓存中该Pod的提名宿主机
	UpdateNominatedPod(oldPod *v1.Pod, newPodInfo *PodInfo)
	// NominatedPodsForNode returns nominatedPods on the given node.
	// 查，查询Pod宿主机上所有的提名Pod
	NominatedPodsForNode(nodeName string) []*PodInfo
}

// PluginsRunner abstracts operations to run some plugins.
// This is used by preemption PostFilter plugins when evaluating the feasibility of
// scheduling the pod on nodes when certain running pods get evicted.
// 运行一些过滤或者打分的插件
type PluginsRunner interface {
	// RunPreScorePlugins runs the set of configured PreScore plugins. If any
	// of these plugins returns any status other than "Success", the given pod is rejected.
	// 运行一组配置的PreScore插件。如果这些插件中的任何一个返回“成功”以外的任何状态，则拒绝给定的pod
	// PreScore插件，打分前的准备
	RunPreScorePlugins(context.Context, *CycleState, *v1.Pod, []*v1.Node) *Status
	// RunScorePlugins runs the set of configured Score plugins. It returns a map that
	// stores for each Score plugin name the corresponding NodeScoreList(s).
	// It also returns *Status, which is set to non-success if any of the plugins returns
	// a non-success status.
	// 运行一组配置的Score插件。它返回一个映射，为每个Score插件名称存储相应的NodeScoreList。
	// 它还返回*status，如果任何插件返回非成功状态，则将其设置为非成功。
	// Score插件，打分操作
	RunScorePlugins(context.Context, *CycleState, *v1.Pod, []*v1.Node) (PluginToNodeScores, *Status)
	// RunFilterPlugins runs the set of configured Filter plugins for pod on
	// the given node. Note that for the node being evaluated, the passed nodeInfo
	// reference could be different from the one in NodeInfoSnapshot map (e.g., pods
	// considered to be running on the node could be different). For example, during
	// preemption, we may pass a copy of the original nodeInfo object that has some pods
	// removed from it to evaluate the possibility of preempting them to
	// schedule the target pod.
	// 在给定节点上为pod运行一组配置的Filter插件。
	// 请注意，对于正在评估的节点，传递的nodeInfo引用可能与NodeInfoSnapshot映射中的引用不同（例如，被认为在节点上运行的pod可能不同）。
	// 例如，在抢占期间，我们可能会传递原始nodeInfo对象的副本，该对象已从其中删除了一些pod，以评估抢占它们以调度目标pod的可能性。
	// Filter插件，执行真正的过滤流程
	RunFilterPlugins(context.Context, *CycleState, *v1.Pod, *NodeInfo) PluginToStatus
	// RunPreFilterExtensionAddPod calls the AddPod interface for the set of configured
	// PreFilter plugins. It returns directly if any of the plugins return any
	// status other than Success.
	// 在执行PreFilter插件过程中添加Pod
	RunPreFilterExtensionAddPod(ctx context.Context, state *CycleState, podToSchedule *v1.Pod, podInfoToAdd *PodInfo, nodeInfo *NodeInfo) *Status
	// RunPreFilterExtensionRemovePod calls the RemovePod interface for the set of configured
	// PreFilter plugins. It returns directly if any of the plugins return any
	// status other than Success.
	// 为一组配置的PreFilter插件调用RemovePod接口。如果任何插件返回成功以外的任何状态，它将直接返回
	// 在执行PreFilter插件过程中移除Pod
	RunPreFilterExtensionRemovePod(ctx context.Context, state *CycleState, podToSchedule *v1.Pod, podInfoToRemove *PodInfo, nodeInfo *NodeInfo) *Status
}
