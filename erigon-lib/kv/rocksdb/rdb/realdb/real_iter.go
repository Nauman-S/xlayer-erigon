package realdb

import (
	"github.com/ledgerwatch/erigon-lib/kv/rocksdb/rdb/common"
	"github.com/linxGnu/grocksdb"
)

type RealIterator struct {
	ropts *grocksdb.ReadOptions
	it    *grocksdb.Iterator
}

func newRealIterator(ropts *grocksdb.ReadOptions, it *grocksdb.Iterator) *RealIterator {
	return &RealIterator{
		ropts: ropts,
		it:    it,
	}
}

func (iter *RealIterator) Valid() bool {
	return iter.it.Valid()
}

func (iter *RealIterator) SeekToFirst() {
	iter.it.SeekToFirst()
}

func (iter *RealIterator) SeekToLast() {
	iter.it.SeekToLast()
}

func (iter *RealIterator) Next() {
	iter.it.Next()
}

func (iter *RealIterator) Prev() {
	iter.it.Prev()
}

func (iter *RealIterator) Seek(key []byte) {
	iter.it.Seek(key)
}

func (iter *RealIterator) Key() []byte {
	k := iter.it.Key()
	if !k.Exists() {
		return nil
	}
	return common.MoveSliceToBytes(k)
}

func (iter *RealIterator) Value() *common.DBValue {
	v := iter.it.Value()
	if !v.Exists() {
		return nil
	}
	return common.DeserializeDBValue(common.MoveSliceToBytes(v))
}

func (iter *RealIterator) Close() {
	iter.it.Close()
	iter.ropts.Destroy()
}

func (iter *RealIterator) Err() error {
	return iter.it.Err()
}
