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

package runtime

import (
	"fmt"
	"sync"
	"time"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// waitingPodsMap a thread-safe map used to maintain pods waiting in the permit phase.
// waitingPodMap主要是存储Permit阶段插件设置的需要Wait等待的pod
// 即时经过之前的优选后，这里面的pod也可能会被某些插件给拒绝掉
type waitingPodsMap struct {
	// 通过pod的uid保存一个map映射
	pods map[types.UID]*waitingPod
	// 通过读写锁来进行数据保护
	mu sync.RWMutex
}

// newWaitingPodsMap returns a new waitingPodsMap.
func newWaitingPodsMap() *waitingPodsMap {
	return &waitingPodsMap{
		pods: make(map[types.UID]*waitingPod),
	}
}

// add a new WaitingPod to the map.
func (m *waitingPodsMap) add(wp *waitingPod) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pods[wp.GetPod().UID] = wp
}

// remove a WaitingPod from the map.
func (m *waitingPodsMap) remove(uid types.UID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.pods, uid)
}

// get a WaitingPod from the map.
func (m *waitingPodsMap) get(uid types.UID) *waitingPod {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.pods[uid]
}

// iterate acquires a read lock and iterates over the WaitingPods map.
func (m *waitingPodsMap) iterate(callback func(framework.WaitingPod)) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, v := range m.pods {
		callback(v)
	}
}

// waitingPod represents a pod waiting in the permit phase.
// 一个具体的pod的等待实例，其内部通过pendingPlugins保存插件的定义的timer等待时间
// 对外通过chan *status来接受当前pod的状态，并通过读写锁来进行串行化
type waitingPod struct {
	pod            *v1.Pod
	pendingPlugins map[string]*time.Timer
	s              chan *framework.Status
	mu             sync.RWMutex
}

var _ framework.WaitingPod = &waitingPod{}

// newWaitingPod returns a new waitingPod instance.

//
// newWaitingPod
//  @Description:
//  	构建waitingPod与计时器
//  	会根据每个plugin的wait等待时间构建N个timer, 如果任一的timer到期，则就拒绝
//  @param pod
//  @param pluginsMaxWaitTime
//  @return *waitingPod
//
func newWaitingPod(pod *v1.Pod, pluginsMaxWaitTime map[string]time.Duration) *waitingPod {
	// 1.新建waitingPod
	wp := &waitingPod{
		pod: pod,
		// Allow() and Reject() calls are non-blocking. This property is guaranteed
		// by using non-blocking send to this channel. This channel has a buffer of size 1
		// to ensure that non-blocking send will not be ignored - possible situation when
		// receiving from this channel happens after non-blocking send.
		// Allow()和拒绝Reject()调用是非阻塞的。此属性通过对该通道使用非阻塞发送来保证。
		// 此通道有一个大小为1的缓冲区，以确保不会忽略非阻塞发送-从该通道接收时可能发生的情况发生在非阻塞发送之后。
		s: make(chan *framework.Status, 1),
	}

	wp.pendingPlugins = make(map[string]*time.Timer, len(pluginsMaxWaitTime))
	// The time.AfterFunc calls wp.Reject which iterates through pendingPlugins map. Acquire the
	// lock here so that time.AfterFunc can only execute after newWaitingPod finishes.
	wp.mu.Lock()
	defer wp.mu.Unlock()
	// 根据插件的等待时间来构建timer，如果有任一timer到期，还未曾有任何plugin Allow则会进行Rejectj
	for k, v := range pluginsMaxWaitTime {
		plugin, waitTime := k, v
		wp.pendingPlugins[plugin] = time.AfterFunc(waitTime, func() {
			msg := fmt.Sprintf("rejected due to timeout after waiting %v at plugin %v",
				waitTime, plugin)
			wp.Reject(plugin, msg)
		})
	}

	return wp
}

// GetPod returns a reference to the waiting pod.
func (w *waitingPod) GetPod() *v1.Pod {
	return w.pod
}

// GetPendingPlugins returns a list of pending permit plugin's name.
func (w *waitingPod) GetPendingPlugins() []string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	plugins := make([]string, 0, len(w.pendingPlugins))
	for p := range w.pendingPlugins {
		plugins = append(plugins, p)
	}

	return plugins
}

// Allow declares the waiting pod is allowed to be scheduled by plugin pluginName.
// If this is the last remaining plugin to allow, then a success signal is delivered
// to unblock the pod.
// 声明等待pod允许由plugin pluginName调度。
// 如果这是最后一个允许的插件，则传递成功信号以解除屏蔽pod。

//
// Allow
//  @Description:
//  	发送允许调度操作
//  	允许操作必须等待所有的plugin都Allow后，才能发送允许事件
//  @receiver w
//  @param pluginName
//
func (w *waitingPod) Allow(pluginName string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if timer, exist := w.pendingPlugins[pluginName]; exist {
		// 停止当前plugin的定时器
		timer.Stop()
		delete(w.pendingPlugins, pluginName)
	}

	// Only signal success status after all plugins have allowed
	// 只有在所有插件都允许后才发出成功状态的信号
	if len(w.pendingPlugins) != 0 {
		return
	}

	// The select clause works as a non-blocking send.
	// If there is no receiver, it's a no-op (default case).
	// Select子句用作非阻塞发送。
	// 如果没有接收器，则为no-op（默认情况）。
	// 只有当所有的plugin都允许，才会发生成功允许事件
	select {
	case w.s <- framework.NewStatus(framework.Success, ""):
	default:
	}
}

// Reject declares the waiting pod unschedulable.

//
// Reject
//  @Description:
//  	声明等待pod不可调度
//  	停止定时器并发送拒绝事件
//  	任一一个plugin的定时器到期，或者plugin主动发起reject操作，则都会暂停所有的定时器，并进行消息广播
//  @receiver w *waitingPod 对象
//  @param pluginName
//  @param msg
//
func (w *waitingPod) Reject(pluginName, msg string) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	// 停止所有的timer
	for _, timer := range w.pendingPlugins {
		timer.Stop()
	}

	// The select clause works as a non-blocking send.
	// If there is no receiver, it's a no-op (default case).
	// 通过管道发送拒绝事件
	select {
	case w.s <- framework.NewStatus(framework.Unschedulable, msg).WithFailedPlugin(pluginName):
	default:
	}
}
