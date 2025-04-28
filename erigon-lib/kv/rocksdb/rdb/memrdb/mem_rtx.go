package memrdb

import (
	"github.com/ledgerwatch/erigon-lib/kv/rocksdb/rdb"
	"github.com/ledgerwatch/erigon-lib/kv/rocksdb/rdb/common"
	"github.com/linxGnu/grocksdb"
)

type MemoryRTX struct {
	db *MemoryRDB
}

func NewMemoryRTX(db *MemoryRDB) *MemoryRTX {
	return &MemoryRTX{db: db}
}

func (rtx *MemoryRTX) Get(opts *grocksdb.ReadOptions, key []byte) (*common.DBValue, error) {
	return rtx.db.get(key)
}

func (rtx *MemoryRTX) Put(key []byte, value *common.DBValue) error {
	// todo: should store data in MemoryRTX first
	return rtx.db.put(key, value)
}

func (rtx *MemoryRTX) Delete(key []byte) error {
	rtx.db.storage.Delete(key)
	return nil
}

func (rtx *MemoryRTX) Commit() error {
	return nil
}

func (rtx *MemoryRTX) Rollback() error {
	// do nothing
	return nil
}

func (rtx *MemoryRTX) NewIterator(beginPrefix, endPrefix []byte) rdb.RDBIterator {
	return rtx.db.storage.NewIterator(beginPrefix, endPrefix)
}

func (rtx *MemoryRTX) Destroy() {
	// do nothing
}
