package rpchelper

import (
	"sync"
	"time"
)

type GenericSyncMap[K comparable, V any] struct {
	inner sync.Map
}

// Store 存储键值对
func (m *GenericSyncMap[K, V]) Store(key K, value V) {
	m.inner.Store(key, value)
}

// Load 加载值
func (m *GenericSyncMap[K, V]) Load(key K) (V, bool) {
	value, ok := m.inner.Load(key)
	if !ok {
		var zero V
		return zero, false
	}
	return value.(V), true
}

// LoadOrStore 加载或存储
func (m *GenericSyncMap[K, V]) LoadOrStore(key K, value V) (V, bool) {
	actual, loaded := m.inner.LoadOrStore(key, value)
	return actual.(V), loaded
}

// Delete 删除键值对
func (m *GenericSyncMap[K, V]) Delete(key K) {
	m.inner.Delete(key)
}

// Range 遍历map
func (m *GenericSyncMap[K, V]) Range(f func(key K, value V) bool) {
	m.inner.Range(func(key, value interface{}) bool {
		return f(key.(K), value.(V))
	})
}

type FilterType int

const (
	HeaderFilterType FilterType = iota
	PendingTxFilterType
	LogsFilterType
)

type FilterInfo struct {
	sync.Mutex
	Type       FilterType
	updateTime time.Time
}

func NewFilterInfo(filterType FilterType) *FilterInfo {
	return &FilterInfo{
		Type:       filterType,
		updateTime: time.Now(),
	}
}

func (f *FilterInfo) Update() {
	f.Lock()
	defer f.Unlock()
	f.updateTime = time.Now()
}

func (f *FilterInfo) Expired() bool {
	f.Lock()
	defer f.Unlock()
	return time.Since(f.updateTime) > time.Minute
}
