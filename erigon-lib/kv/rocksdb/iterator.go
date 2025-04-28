package rocksdb

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/rocksdb/rdb"
	"github.com/ledgerwatch/erigon-lib/kv/rocksdb/rdb/common"
	"github.com/ledgerwatch/log/v3"
)

type pairCache struct {
	key   []byte
	value []byte

	err error
}

func makeInvalidPair(err error) pairCache {
	if err == nil {
		err = errors.New("invalid pair")
	}
	return pairCache{nil, nil, err}
}

func makePairCache(k, v []byte, err error) pairCache {
	return pairCache{k, v, err}
}

func (p pairCache) isValid() bool {
	return p.err == nil
}

type iterCache struct {
	key   []byte
	value *common.DBValueIterator
}

func invalidIterCache() iterCache {
	return iterCache{nil, nil}
}

func (ic iterCache) isValid() bool {
	return ic.value != nil
}

// the order of rocksdb's iterator is inverse with order of mdbx's cursor,
// so we have to wrap a iterator to make the order consist with mdbx.
type alwaysValidRDBIterator struct {
	tx rdb.RDBTransaction

	it           rdb.RDBIterator
	currentKey   []byte
	currentValue *common.DBValue

	table string
	// beginPrefix and endPrefix come from table.
	// beginPrefix is included but endPrefix is excluded.
	beginPrefix []byte
	endPrefix   []byte
}

func newalwaysValidRDBIterator(tx rdb.RDBTransaction, table string, beginPrefix, endPrefix []byte) *alwaysValidRDBIterator {
	avit := &alwaysValidRDBIterator{
		tx: tx,

		it:           nil,
		currentKey:   nil,
		currentValue: nil,

		table:       table,
		beginPrefix: beginPrefix,
		endPrefix:   endPrefix,
	}
	avit.reCreate()

	return avit
}

func (avit *alwaysValidRDBIterator) InvalidCurrent() {
	avit.currentKey = nil
	avit.currentValue = nil
}

func (avit *alwaysValidRDBIterator) Current() ([]byte, *common.DBValue, bool) {
	return avit.currentKey, avit.currentValue, avit.currentKey != nil
}

func (avit *alwaysValidRDBIterator) SeekToFirst() ([]byte, *common.DBValue, bool) {
	avit.it.SeekToFirst()
	if !avit.it.Valid() {
		return nil, nil, false
	}
	avit.currentKey = avit.it.Key()
	avit.currentValue = avit.it.Value()

	return avit.currentKey, avit.currentValue, true
}

func (avit *alwaysValidRDBIterator) SeekToLast() ([]byte, *common.DBValue, bool) {
	avit.it.SeekToLast()
	if !avit.it.Valid() {
		return nil, nil, false
	}
	avit.currentKey = avit.it.Key()
	avit.currentValue = avit.it.Value()

	return avit.currentKey, avit.currentValue, true
}

func (avit *alwaysValidRDBIterator) Next() ([]byte, *common.DBValue, bool) {
	avit.it.Next()
	if !avit.it.Valid() {
		avit.reCreate()
		return nil, nil, false
	}
	avit.currentKey = avit.it.Key()
	avit.currentValue = avit.it.Value()

	return avit.currentKey, avit.currentValue, true
}

func (avit *alwaysValidRDBIterator) Prev() ([]byte, *common.DBValue, bool) {
	avit.it.Prev()
	if !avit.it.Valid() {
		avit.reCreate()
		return nil, nil, false
	}
	avit.currentKey = avit.it.Key()
	avit.currentValue = avit.it.Value()

	return avit.currentKey, avit.currentValue, true
}

func (avit *alwaysValidRDBIterator) Seek(key []byte) ([]byte, *common.DBValue, bool) {
	avit.it.Seek(key)
	if !avit.it.Valid() {
		avit.reCreate()
		return nil, nil, false
	}
	avit.currentKey = avit.it.Key()
	avit.currentValue = avit.it.Value()

	return avit.currentKey, avit.currentValue, true
}

func (avit *alwaysValidRDBIterator) Clone() *alwaysValidRDBIterator {
	return newalwaysValidRDBIterator(avit.tx, avit.table, avit.beginPrefix, avit.endPrefix)
}

func (avit *alwaysValidRDBIterator) Close() {
	avit.it.Close()
	avit.tx = nil
	avit.it = nil
	avit.currentKey = nil
	avit.currentValue = nil
}

func (avit *alwaysValidRDBIterator) reCreate() {
	if avit.it != nil {
		avit.it.Close()
	}

	avit.it = avit.tx.NewIterator(avit.beginPrefix, avit.endPrefix)

	// seek to old position and initialize currentValue
	if avit.currentKey != nil {
		avit.it.Seek(avit.currentKey)
		if !avit.it.Valid() {
			log.Warn(fmt.Sprintf("seek to %x failed. may be deleted", avit.currentKey))
			avit.InvalidCurrent()
		} else {
			avit.currentValue = avit.it.Value()
		}
	} else {
		avit.it.SeekToFirst()
		if avit.it.Valid() {
			avit.currentKey = avit.it.Key()
			avit.currentValue = avit.it.Value()
		} else {
			avit.InvalidCurrent()
		}
	}

}

type RocksDbIterator struct {
	tx rdb.RDBTransaction

	// iterator of rocksdb.
	// when iterate values, must iterate dbvIter first if dbvIter is not null
	it *alwaysValidRDBIterator

	table string
	// beginPrefix and endPrefix come from table.
	// beginPrefix is included but endPrefix is excluded.
	beginPrefix []byte
	endPrefix   []byte

	current iterCache
}

func NewRocksDbIterator(tx rdb.RDBTransaction, table string) *RocksDbIterator {
	beginPrefix := common.MergeKey(table, []byte{})
	endPrefix, _ := kv.NextSubtree(beginPrefix)

	it := newalwaysValidRDBIterator(tx, table, beginPrefix, endPrefix)

	k, dbv, valid := it.Current()
	current := invalidIterCache()
	if valid {
		current = createCacheWithPriorFirst(k, dbv)
	}

	return &RocksDbIterator{
		tx: tx,

		table:       table,
		beginPrefix: beginPrefix,
		endPrefix:   endPrefix,
		it:          it,
		current:     current,
	}
}

func (iter *RocksDbIterator) UpdateSnapshot() {
	// create a new iterator will use the latest snapshot
	iter.it.reCreate()
}

func (iter *RocksDbIterator) Close() {
	if iter.it != nil {
		iter.it.Close()
		iter.it = nil
	}
}

func (iter *RocksDbIterator) First() ([]byte, []byte, error) {
	k, dbv, valid := iter.it.SeekToFirst()
	if !valid {
		return nil, nil, common.ErrInvalidIter
	}

	iter.current = createCacheWithFirstValueIsCurrent(k, dbv)
	v, _ := iter.current.value.Current()
	return iter.current.key, v, nil
}

func (iter *RocksDbIterator) Last() ([]byte, []byte, error) {
	k, dbv, valid := iter.it.SeekToLast()
	if !valid {
		return nil, nil, common.ErrInvalidIter
	}

	iter.current = createCacheWithFirstValueIsCurrent(k, dbv)
	v, ok := iter.current.value.SeekToLast()
	if !ok {
		panic("must have a value for Last")
	}
	return iter.current.key, v, nil
}

func (iter *RocksDbIterator) Current() ([]byte, []byte, error) {
	if iter.current.isValid() {
		if v, ok := iter.current.value.Current(); ok {
			return iter.current.key, v, nil
		}
	}

	return nil, nil, common.ErrInvalidIter
}

func (iter *RocksDbIterator) NextKey() error {
	k, dbv, valid := iter.it.Next()
	if !valid {
		return common.ErrInvalidIter
	}
	iter.current = createCacheWithFirstValueIsCurrent(k, dbv)
	return nil
}

func (iter *RocksDbIterator) Count() (uint64, error) {
	it := iter.it.Clone()
	defer it.Close()

	count := uint64(0)
	for k, dbv, valid := it.SeekToFirst(); valid; k, dbv, valid = it.Next() {
		count += createCacheWithFirstValueIsCurrent(k, dbv).value.Count()
	}

	return count, nil
}

func (iter *RocksDbIterator) Seek(key []byte) ([]byte, []byte, error) {
	return iter.SeekWithValue(key, nil)
}

func (iter *RocksDbIterator) SeekWithValue(key, seekValue []byte) (k []byte, v []byte, err error) {
	defer func() {
		if err != nil {
			iter.current = invalidIterCache()
		}
	}()

	k, dbv, valid := iter.it.Seek(common.MergeKey(iter.table, key))
	if !valid {
		return nil, nil, common.ErrNotFound
	}
	iter.current = createCacheWithFirstValueIsCurrent(k, dbv)
	v, err = iter.current.value.Seek(seekValue)
	return iter.current.key, v, err
}

func (iter *RocksDbIterator) SeekExact(key []byte) ([]byte, []byte, error) {
	return iter.SeekExactKeyWithGeValue(key, nil)
}

// seek to exact key and value >= seekV
func (iter *RocksDbIterator) SeekExactKeyWithGeValue(seekK, seekV []byte) (k []byte, v []byte, err error) {
	return iter.seekExactKeyWithValueSeekFunc(seekK, func(valueIter *common.DBValueIterator) ([]byte, error) {
		return valueIter.Seek(seekV)
	})
}

func (iter *RocksDbIterator) SeekExactKeyAndValue(seekK, seekV []byte) (k []byte, v []byte, err error) {
	// we call SeekExactKeyWithGeValue here because SeekExactKeyAndValue must
	// keep `current` consist with `SeekExactKeyWithGeValue`, even the value is mismatch.
	k, v, err = iter.SeekExactKeyWithGeValue(seekK, seekV)
	if err != nil {
		return nil, nil, err
	}
	if !bytes.Equal(v, seekV) {
		return nil, nil, common.ErrNotFound
	}
	return k, v, err
}

func (iter *RocksDbIterator) Next() ([]byte, []byte, error) {
	if iter.current.isValid() {
		if nextV, valid := iter.current.value.Next(); valid {
			return iter.current.key, nextV, nil
		}
	}

	nextK, nextDbv, valid := iter.it.Next()
	if !valid {
		return nil, nil, common.ErrInvalidIter
	}

	iter.current = createCacheWithFirstValueIsCurrent(nextK, nextDbv)
	v, _ := iter.current.value.Current()
	return iter.current.key, v, nil
}

func (iter *RocksDbIterator) Prev() ([]byte, []byte, error) {
	if iter.current.isValid() {
		if nextV, valid := iter.current.value.Prev(); valid {
			return iter.current.key, nextV, nil
		}
	}

	prevK, prevDbv, valid := iter.it.Prev()
	if !valid {
		return nil, nil, common.ErrInvalidIter
	}

	iter.current = createCacheWithFirstValueIsCurrent(prevK, prevDbv)
	v, _ := iter.current.value.SeekToLast()
	return iter.current.key, v, nil
}

func (iter *RocksDbIterator) LastDup() ([]byte, []byte, error) {
	if iter.current.isValid() {
		if v, ok := iter.current.value.SeekToLast(); ok {
			return iter.current.key, v, nil
		}
	}

	nextK, nextDbv, valid := iter.it.Next()
	if !valid {
		return nil, nil, common.ErrInvalidIter
	}

	iter.current = createCacheWithFirstValueIsCurrent(nextK, nextDbv)
	v, _ := iter.current.value.SeekToLast()
	return iter.current.key, v, nil
}

func (iter *RocksDbIterator) FirstDup() ([]byte, []byte, error) {
	if iter.current.isValid() {
		if v, ok := iter.current.value.SeekToFirst(); ok {
			return iter.current.key, v, nil
		}
	}

	nextK, nextDbv, valid := iter.it.Next()
	if !valid {
		return nil, nil, common.ErrInvalidIter
	}

	iter.current = createCacheWithFirstValueIsCurrent(nextK, nextDbv)
	v, _ := iter.current.value.SeekToFirst()
	return iter.current.key, v, nil
}

func (iter *RocksDbIterator) NextDup() ([]byte, []byte, error) {
	if iter.current.isValid() {
		if nextV, valid := iter.current.value.Next(); valid {
			return iter.current.key, nextV, nil
		}
	}
	return nil, nil, common.ErrNotFound
}

func (iter *RocksDbIterator) PrevDup() ([]byte, []byte, error) {
	if iter.current.isValid() {
		if nextV, valid := iter.current.value.Prev(); valid {
			return iter.current.key, nextV, nil
		}
	}
	return nil, nil, common.ErrNotFound
}

func (iter *RocksDbIterator) seekExactKeyWithValueSeekFunc(key []byte, valueSeekFunc func(valueIter *common.DBValueIterator) ([]byte, error)) (k []byte, v []byte, err error) {
	k, dbv, valid := iter.it.Seek(common.MergeKey(iter.table, key))
	if !valid {
		iter.invalidCurrent()
		return nil, nil, common.ErrNotFound
	}
	iter.current = createCacheWithFirstValueIsCurrent(k, dbv)

	if !bytes.Equal(k, common.MergeKey(iter.table, key)) {
		return nil, nil, common.ErrNotFound
	}

	v, err = valueSeekFunc(iter.current.value)
	if err != nil {
		iter.invalidCurrent()
		return nil, nil, err
	}
	return iter.current.key, v, err
}

func (iter *RocksDbIterator) currentKeyAndValueStamp() ([]byte, common.DBValueStamp, error) {
	if !iter.current.isValid() {
		return nil, common.DBValueStamp{}, common.ErrInvalidIter
	}
	return iter.current.key, iter.current.value.CurrentStamp(), nil
}

func (iter *RocksDbIterator) mustSeekToKeyValue(key, value []byte) {
	k, dbv, valid := iter.it.Seek(common.MergeKey(iter.table, key))
	if !valid {
		panic("seek to key must success")
	}

	iter.current = createCacheWithFirstValueIsCurrent(k, dbv)
	iter.current.value.MustSeekToValue(value)
}

func (iter *RocksDbIterator) invalidCurrent() {
	iter.current = invalidIterCache()
	iter.it.InvalidCurrent()
}

func (iter *RocksDbIterator) deleteCurrent() {
	if !iter.current.isValid() {
		panic("deleteCurrent: current is invalid")
	}
	iter.current.value.DeleteCurrent()

	if !iter.current.value.IsEmpty() {
		return
	}

	// current cache is empty, we must valid it
	if k, dbv, valid := iter.it.Next(); valid {
		iter.current = createCacheWithFirstValueIsCurrent(k, dbv)
		// todo: it's not appropriate to set nextIsCurrent flag here
		iter.current.value.SetNextIsCurrent()
	} else {
		if k, dbv, valid = iter.it.Prev(); valid {
			iter.current = createCacheWithAfterLast(k, dbv)
		} else {
			//  both Next and Prev is invalid
			iter.invalidCurrent()
		}
	}

}

func createCacheWithFirstValueIsCurrent(key []byte, value *common.DBValue) iterCache {
	cache := createCacheWithPriorFirst(key, value)

	// the current of DBValueIterator is invalid when it is created,
	// so we should call Next to make the current get valid (which is the first one)
	_, valid := cache.value.Next()
	if !valid {
		panic(fmt.Sprintf("invalid DBValue: %v. %v", cache.value, value))
	}
	return cache
}

func createCacheWithPriorFirst(key []byte, value *common.DBValue) iterCache {
	_, key = common.SplitKey(key)
	valueIter := common.NewDBValueIterator(value)
	return iterCache{
		key:   key,
		value: valueIter,
	}
}

func createCacheWithAfterLast(key []byte, value *common.DBValue) iterCache {
	_, key = common.SplitKey(key)
	valueIter := common.NewDBValueIteratorOverLast(value)
	return iterCache{
		key:   key,
		value: valueIter,
	}
}
