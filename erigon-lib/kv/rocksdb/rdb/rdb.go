package rdb

import (
	"github.com/ledgerwatch/erigon-lib/kv/rocksdb/rdb/common"
	"github.com/linxGnu/grocksdb"
)

type RDB interface {
	Get(key []byte) (*common.DBValue, error)
	TransactionBegin(opts *grocksdb.WriteOptions, transactionOpts *grocksdb.TransactionOptions, oldTransaction *grocksdb.Transaction) RDBTransaction
	Close()
}

type RDBTransaction interface {
	Get(opts *grocksdb.ReadOptions, key []byte) (*common.DBValue, error)
	Put(key []byte, value *common.DBValue) error
	Delete(key []byte) error
	Commit() error
	Rollback() error
	NewIterator(beginPrefix, endPrefix []byte) RDBIterator
	Destroy()
}

type RDBIterator interface {
	Valid() bool
	SeekToFirst()
	SeekToLast()
	Next()
	Prev()
	Seek(key []byte)
	Key() []byte
	Value() *common.DBValue
	Close()
	Err() error
}
