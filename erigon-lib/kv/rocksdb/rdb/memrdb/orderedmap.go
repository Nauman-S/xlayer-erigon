package memrdb

import (
	"errors"
	"github.com/ledgerwatch/erigon-lib/kv/rocksdb/rdb/common"
)

// ascend order map
type OrderedMap struct {
	data       map[string]*common.DBValue
	sortedKeys []string
}

type OrderedMapIterator struct {
	omap *OrderedMap

	beginPrefix []byte
	endPrefix   []byte

	valid   bool
	current int
	err     error
}

func NewOrderedMap() *OrderedMap {
	return &OrderedMap{
		data:       make(map[string]*common.DBValue),
		sortedKeys: make([]string, 0),
	}
}

func (m *OrderedMap) Put(key []byte, value *common.DBValue) {
	sKey := string(key)

	if _, exists := m.data[sKey]; exists {
		m.data[sKey] = value
		// key has existed, don't need to insert new key
		return
	}

	m.data[sKey] = value

	// insert new key
	idx, _ := m.sortedSeek(sKey)
	m.sortedKeys = append(m.sortedKeys, "")
	copy(m.sortedKeys[idx+1:], m.sortedKeys[idx:]) // move back
	m.sortedKeys[idx] = sKey                       // insert
}

func (m *OrderedMap) sortedSeek(key string) (int, bool) {
	return common.SortedSeek(m.sortedKeys, key, func(k1 string, k2 string) bool {
		return k1 >= k2
	})
}

func (m *OrderedMap) Get(key []byte) (*common.DBValue, bool) {
	sKey := string(key)

	val, ok := m.data[sKey]
	return val, ok
}

func (m *OrderedMap) Delete(key []byte) {
	sKey := string(key)

	if _, ok := m.data[sKey]; ok {
		delete(m.data, sKey)
		for i, k := range m.sortedKeys {
			if k == sKey {
				m.sortedKeys = append(m.sortedKeys[:i], m.sortedKeys[i+1:]...)
				break
			}
		}
	}
}

func (m *OrderedMap) NewIterator(beginPrefix, endPrefix []byte) *OrderedMapIterator {
	iter := &OrderedMapIterator{
		omap:        m,
		beginPrefix: beginPrefix,
		endPrefix:   endPrefix,
	}
	iter.setInvalid("init")

	return iter
}

func (iter *OrderedMapIterator) Valid() bool {
	return iter.valid
}

func (iter *OrderedMapIterator) SeekToFirst() {
	if iter.beginPrefix == nil {
		if len(iter.omap.sortedKeys) == 0 {
			iter.setInvalid("SeekToFirst: no keys")
			return
		}
		iter.setValid(0)
		return
	}

	iter.setInvalid("SeekToFirst: don't find begin prefix")
	beginPrefix := string(iter.beginPrefix)
	for i, key := range iter.omap.sortedKeys {
		if key >= beginPrefix {
			iter.setValid(i)
			break
		}
	}
}

func (iter *OrderedMapIterator) SeekToLast() {
	if iter.endPrefix == nil {
		if len(iter.omap.sortedKeys) == 0 {
			iter.setInvalid("SeekToLast: no keys")
			return
		}
		iter.setValid(len(iter.omap.sortedKeys) - 1)
		return
	}

	iter.setInvalid("SeekToLast: don't find end prefix")
	endPrefix := string(iter.endPrefix)
	for i := len(iter.omap.sortedKeys) - 1; i >= 0; i-- {
		key := iter.omap.sortedKeys[i]
		if key < endPrefix {
			iter.setValid(i)
			break
		}
	}
}

func (iter *OrderedMapIterator) Next() {
	if !iter.Valid() {
		return
	}

	iter.current++
	if iter.current >= len(iter.omap.sortedKeys) {
		iter.setInvalid("Next: no more items")
		return
	}

	curKey := iter.currentKey()
	if curKey >= string(iter.endPrefix) {
		iter.setInvalid("Next: exceed end prefix")
		return
	}
}

func (iter *OrderedMapIterator) Prev() {
	if !iter.Valid() {
		return
	}

	iter.current--
	if iter.current < 0 {
		iter.setInvalid("Prev: no more items")
		return
	}

	curKey := iter.currentKey()
	if curKey < string(iter.beginPrefix) {
		iter.setInvalid("Prev: exceed begin prefix")
		return
	}
}

func (iter *OrderedMapIterator) Seek(key []byte) {
	iter.SeekToFirst()
	if !iter.Valid() {
		return
	}

	seekKey := string(key)
	idx, ok := iter.omap.sortedSeek(seekKey)
	if !ok {
		iter.setInvalid("Seek: dont find seek key")
		return
	}

	iter.setValid(idx)
}

func (iter *OrderedMapIterator) Key() []byte {
	if !iter.Valid() {
		return nil
	}

	curKey := iter.currentKey()
	return []byte(curKey)
}

func (iter *OrderedMapIterator) Value() *common.DBValue {
	if !iter.Valid() {
		return nil
	}

	curKey := iter.omap.sortedKeys[iter.current]
	return iter.omap.data[curKey]
}

func (iter *OrderedMapIterator) Close() {
	// do nothing
}

func (iter *OrderedMapIterator) Err() error {
	return iter.err
}

func (iter *OrderedMapIterator) currentKey() string {
	return iter.omap.sortedKeys[iter.current]
}

func (iter *OrderedMapIterator) setInvalid(err string) {
	iter.valid = false
	iter.current = -1
	iter.err = errors.New(err)
}

func (iter *OrderedMapIterator) setValid(current int) {
	iter.valid = true
	iter.current = current
	iter.err = nil
}
