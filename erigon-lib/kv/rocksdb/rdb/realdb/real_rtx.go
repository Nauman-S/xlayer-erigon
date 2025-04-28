package realdb

import (
	"github.com/ledgerwatch/erigon-lib/kv/rocksdb/rdb"
	"github.com/ledgerwatch/erigon-lib/kv/rocksdb/rdb/common"
	"github.com/linxGnu/grocksdb"
)

type RealRtx struct {
	tx *grocksdb.Transaction

	latestSnapshotCreator func() *grocksdb.Snapshot
	snapshots             []*grocksdb.Snapshot
}

func newRealRtx(tx *grocksdb.Transaction, latestSnapshotCreator func() *grocksdb.Snapshot) *RealRtx {
	return &RealRtx{tx: tx, latestSnapshotCreator: latestSnapshotCreator}
}

func (rtx *RealRtx) Get(opts *grocksdb.ReadOptions, key []byte) (*common.DBValue, error) {
	s, err := rtx.tx.Get(opts, key)
	if err != nil {
		return nil, err
	}
	if !s.Exists() {
		return nil, common.ErrKeyNotExist
	}

	return common.DeserializeDBValue(common.MoveSliceToBytes(s)), nil
}

func (rtx *RealRtx) Put(key []byte, value *common.DBValue) error {
	return rtx.tx.Put(key, value.Serialize())
}

func (rtx *RealRtx) Delete(key []byte) error {
	return rtx.tx.Delete(key)
}

func (rtx *RealRtx) Commit() error {
	return rtx.tx.Commit()
}

func (rtx *RealRtx) Rollback() error {
	return rtx.tx.Rollback()
}

func (rtx *RealRtx) NewIterator(beginPrefix, endPrefix []byte) rdb.RDBIterator {
	// todo: combinedb need update snapshot in some situation.
	//   if combinedb isn't used, code here could ignore call `ropts.SetSnapshot`
	ss := rtx.latestSnapshotCreator()
	rtx.snapshots = append(rtx.snapshots, ss)

	ropts := grocksdb.NewDefaultReadOptions()
	ropts.SetIterateLowerBound(beginPrefix)
	ropts.SetIterateUpperBound(endPrefix)
	ropts.SetSnapshot(ss)

	return newRealIterator(ropts, rtx.tx.NewIterator(ropts))
}

func (rtx *RealRtx) Destroy() {
	rtx.tx.Destroy()
	for _, ss := range rtx.snapshots {
		ss.Destroy()
	}
	rtx.snapshots = nil
}
