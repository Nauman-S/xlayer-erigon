package rocksdb

import (
	"fmt"
	"github.com/ledgerwatch/erigon-lib/kv/rocksdb/rdb"
	"github.com/ledgerwatch/erigon-lib/kv/rocksdb/rdb/memrdb"
	"github.com/ledgerwatch/erigon-lib/kv/rocksdb/rdb/realdb"
	"github.com/linxGnu/grocksdb"
)

type RDBType int

const (
	MemRDB RDBType = iota
	RealRDB
)

func (rt RDBType) NewRDB(opts *grocksdb.Options, txopts *grocksdb.TransactionDBOptions, dbPath string) (rdb.RDB, error) {
	switch rt {
	case MemRDB:
		return memrdb.NewMemoryRDB(), nil
	case RealRDB:
		return realdb.NewRealRDB(opts, txopts, dbPath)
	default:
		panic(fmt.Sprintf("unknown RDB type: %v", rt))
	}
}
