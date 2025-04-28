package realdb

import (
	"github.com/ledgerwatch/erigon-lib/kv/rocksdb/rdb"
	"github.com/ledgerwatch/erigon-lib/kv/rocksdb/rdb/common"
	"github.com/linxGnu/grocksdb"
)

type RealRDB struct {
	db *grocksdb.TransactionDB
}

func NewRealRDB(opts *grocksdb.Options, txopts *grocksdb.TransactionDBOptions, dbPath string) (*RealRDB, error) {
	db, err := grocksdb.OpenTransactionDb(opts, txopts, dbPath)
	return &RealRDB{db: db}, err
}

func (db *RealRDB) TransactionBegin(opts *grocksdb.WriteOptions, transactionOpts *grocksdb.TransactionOptions, oldTransaction *grocksdb.Transaction) rdb.RDBTransaction {
	return newRealRtx(db.db.TransactionBegin(opts, transactionOpts, oldTransaction), func() *grocksdb.Snapshot {
		return db.db.NewSnapshot()
	})

}

func (db *RealRDB) Close() {
	db.db.Close()
	db.db = nil
}

func (db *RealRDB) Get(k []byte) (*common.DBValue, error) {
	ropts := grocksdb.NewDefaultReadOptions()
	defer ropts.Destroy()
	s, err := db.db.Get(ropts, k)
	if err != nil {
		return nil, err
	}
	defer s.Free()
	return common.DeserializeDBValue(common.MoveSliceToBytes(s)), nil
}
