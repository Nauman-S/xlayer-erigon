package memrdb

import (
	"github.com/ledgerwatch/erigon-lib/kv/rocksdb/rdb"
	"github.com/ledgerwatch/erigon-lib/kv/rocksdb/rdb/common"
	"github.com/linxGnu/grocksdb"
)

type MemoryRDB struct {
	storage *OrderedMap
}

func NewMemoryRDB() *MemoryRDB {
	return &MemoryRDB{
		storage: NewOrderedMap(),
	}
}

func (db *MemoryRDB) Get(key []byte) (*common.DBValue, error) {
	dbv, ok := db.storage.Get(key)
	if !ok {
		return nil, common.ErrKeyNotExist
	}
	return dbv, nil
}

func (db *MemoryRDB) Close() {
	// do nothing
}

func (db *MemoryRDB) TransactionBegin(opts *grocksdb.WriteOptions, transactionOpts *grocksdb.TransactionOptions, oldTransaction *grocksdb.Transaction) rdb.RDBTransaction {
	return NewMemoryRTX(db)
}

func (db *MemoryRDB) GetMemStorage() map[string]*common.DBValue {
	return db.storage.data
}

func (db *MemoryRDB) put(key []byte, value *common.DBValue) error {
	db.storage.Put(key, value)
	return nil
}

func (db *MemoryRDB) get(key []byte) (*common.DBValue, error) {
	value, ok := db.storage.Get(key)
	if !ok {
		return nil, common.ErrKeyNotExist
	}
	return value, nil
}
