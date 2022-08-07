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

package framework

import (
	"errors"
	"sync"
)

var (
	// ErrNotFound is the not found error message.
	ErrNotFound = errors.New("not found")
)

// StateData is a generic type for arbitrary data stored in CycleState.
// 是存储在CycleState中的任意数据的泛型类型
type StateData interface {
	// Clone is an interface to make a copy of StateData. For performance reasons,
	// clone should make shallow copies for members (e.g., slices or maps) that are not
	// impacted by PreFilter's optional AddPod/RemovePod methods.
	Clone() StateData
}

// StateKey is the type of keys stored in CycleState.
type StateKey string

// CycleState provides a mechanism for plugins to store and retrieve arbitrary data.
// StateData stored by one plugin can be read, altered, or deleted by another plugin.
// CycleState does not provide any data protection, as all plugins are assumed to be
// trusted.
// Note: CycleState uses a sync.Map to back the storage. It's aimed to optimize for the "write once and read many times" scenarios.
// It is the recommended pattern used in all in-tree plugins - plugin-specific state is written once in PreFilter/PreScore and afterwards read many times in Filter/Score.
// CycleState为插件提供了一种存储和检索任意数据的机制
// 一个插件存储的StateData可以被另一个插件读取、更改或删除
// CycleState不提供任何数据保护，因为所有插件都被假定为信任
// 注意：CycleState使用同步。映射来备份存储。它旨在针对“一次写入多次读取”场景进行优化。
// 它是所有树内插件中使用的推荐模式-插件特定状态在PreFilter/PreScore中写入一次，然后在Filter/Score中多次读取。
// 每一个pod的调度绑定流程，都会关联一个CycleState对象，它用于存储当前pod整个调度绑定流程的所有数据，在每一个扩展点中，所有的plugin都可以拿到这个对象，可以从该对象读取或写入一些必要的信息
type CycleState struct {
	// storage is keyed with StateKey, and valued with StateData.
	// 使用StateKey进行键控，并使用StateData进行估值
	storage sync.Map
	// if recordPluginMetrics is true, PluginExecutionDuration will be recorded for this cycle.
	recordPluginMetrics bool
}

// NewCycleState initializes a new CycleState and returns its pointer.
// 初始化一个新的CycleState并返回它的指针。
func NewCycleState() *CycleState {
	return &CycleState{}
}

// ShouldRecordPluginMetrics returns whether PluginExecutionDuration metrics should be recorded.
func (c *CycleState) ShouldRecordPluginMetrics() bool {
	if c == nil {
		return false
	}
	return c.recordPluginMetrics
}

// SetRecordPluginMetrics sets recordPluginMetrics to the given value.
func (c *CycleState) SetRecordPluginMetrics(flag bool) {
	if c == nil {
		return
	}
	c.recordPluginMetrics = flag
}

// Clone creates a copy of CycleState and returns its pointer. Clone returns
// nil if the context being cloned is nil.
func (c *CycleState) Clone() *CycleState {
	if c == nil {
		return nil
	}
	copy := NewCycleState()
	c.storage.Range(func(k, v interface{}) bool {
		copy.storage.Store(k, v.(StateData).Clone())
		return true
	})
	copy.recordPluginMetrics = c.recordPluginMetrics

	return copy
}

// Read retrieves data with the given "key" from CycleState. If the key is not
// present an error is returned.
// This function is thread safe by using sync.Map.
func (c *CycleState) Read(key StateKey) (StateData, error) {
	if v, ok := c.storage.Load(key); ok {
		return v.(StateData), nil
	}
	return nil, ErrNotFound
}

// Write stores the given "val" in CycleState with the given "key".
// This function is thread safe by using sync.Map.
func (c *CycleState) Write(key StateKey, val StateData) {
	c.storage.Store(key, val)
}

// Delete deletes data with the given key from CycleState.
// This function is thread safe by using sync.Map.
func (c *CycleState) Delete(key StateKey) {
	c.storage.Delete(key)
}
