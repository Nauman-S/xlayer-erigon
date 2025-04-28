package combinedb

import (
	"context"
	"fmt"
	"path"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/kv/rocksdb"
)

var commitLock sync.RWMutex

type CombineDB struct {
	mdbx    kv.RwDB
	rocksdb kv.RwDB

	logger *combineLogger
}

var dbCounter atomic.Uint64

func NewCombinDB(ctx context.Context, opts mdbx.MdbxOpts, tableCfg kv.TableCfg, enableLog bool) (db kv.RwDB, err error) {
	dbDir := opts.GetPath()

	opts = opts.Path(path.Join(dbDir, "mdbx"))
	opts.GetLogger().Info("Set mdbx path", "new path", opts.GetPath())
	mdbx, err := opts.Open(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			mdbx.Close()
		}
	}()

	rocksdbDir := path.Join(dbDir, "rocksdb")
	opts.GetLogger().Info("Set rocksdb path", "new path", rocksdbDir)
	rocksdb, err := rocksdb.NewRocksDB(rocksdbDir, opts.GetLogger(), tableCfg, opts.GetLabel(), opts.GetRoTxsLimiter(), opts.IsReadonly(), rocksdb.RealRDB)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			rocksdb.Close()
		}
	}()

	return &CombineDB{
		mdbx:    mdbx,
		rocksdb: rocksdb,

		logger: newCombinLogger(enableLog, fmt.Sprintf("combinedb dbid=%d", dbCounter.Add(1))),
	}, nil
}

func (db *CombineDB) Close() {
	db.logger.Info("Close")
	defer db.logger.Info("Close done")
	db.mdbx.Close()
	db.mdbx = nil

	db.rocksdb.Close()
	db.rocksdb = nil
}

func (db *CombineDB) ReadOnly() (b bool) {
	db.logger.Info("ReadOnly")
	defer db.logger.Infof("ReadOnly done. b=%v", b)

	b1 := db.mdbx.ReadOnly()
	b2 := db.rocksdb.ReadOnly()

	assertEqualF(db.logger, b1, b2, "ReadOnly mismatch: mdbx: %v. rocksdb: %v", b1, b2)
	return b1
}

func (db *CombineDB) View(ctx context.Context, f func(tx kv.Tx) error) (err error) {
	commitLock.RLock()
	defer commitLock.RUnlock()

	db.logger.Info("View")
	defer db.logger.Infof("View done. err=%v", err)

	return db.mdbx.View(ctx, func(mdbxTx kv.Tx) error {
		return db.rocksdb.View(ctx, func(rocksdbTx kv.Tx) error {
			ctx := newCombineTx(db.logger, mdbxTx, rocksdbTx)
			return f(ctx)
		})
	})
}

func (db *CombineDB) BeginRo(ctx context.Context) (t kv.Tx, err error) {
	commitLock.RLock()
	defer commitLock.RUnlock()

	db.logger.Info("BeginRo")
	defer db.logger.Infof("BeginRo done. err=%v", err)

	mdbxTx, err1 := db.mdbx.BeginRo(ctx)
	rocksdbTx, err2 := db.rocksdb.BeginRo(ctx)
	if err := assertError(db.logger, err1, err2, "BeginRo"); err != nil {
		return nil, err
	}

	return newCombineTx(db.logger, mdbxTx, rocksdbTx), nil
}

func (db *CombineDB) AllTables() (tables kv.TableCfg) {
	db.logger.Info("AllTables")
	defer db.logger.Infof("AllTables done. tables=%v", tables)

	mdbxTables := db.mdbx.AllTables()
	rocksdbTables := db.rocksdb.AllTables()

	assertEqualF(db.logger, mdbxTables, rocksdbTables, "AllTables mismatch: mdbx table: %v\nrocksdb table: %v", mdbxTables, rocksdbTables)
	return mdbxTables
}

func (db *CombineDB) PageSize() (s uint64) {
	db.logger.Info("PageSize")
	defer db.logger.Infof("PageSize done. s=%d", s)

	mdbxPageSize := db.mdbx.PageSize()
	rocksdbPageSize := db.rocksdb.PageSize()

	assertEqualF(db.logger, mdbxPageSize, mdbxPageSize, "PageSize mismatch: mdbx.PageSize=%v\nrocksdb.PageSize=%v", mdbxPageSize, rocksdbPageSize)
	return mdbxPageSize
}

// Pointer to the underlying C environment handle, if applicable (e.g. *C.MDBX_env)
func (db *CombineDB) CHandle() unsafe.Pointer {
	db.logger.Info("CHandle")
	defer db.logger.Info("CHandle done")

	mdbxH := db.mdbx.CHandle()
	rocksdbH := db.rocksdb.CHandle()
	assertEqualF(db.logger, mdbxH, rocksdbH, "CHandle mismatch: mdbx: %v\nrocksdb: %v", mdbxH, rocksdbH)
	return mdbxH
}

func (db *CombineDB) Update(ctx context.Context, f func(tx kv.RwTx) error) (err error) {
	commitLock.RLock()
	defer commitLock.RUnlock()

	db.logger.Info("Update")
	defer db.logger.Infof("Update done. err=%v", err)

	return db.mdbx.Update(ctx, func(mdbxTx kv.RwTx) error {
		return db.rocksdb.Update(ctx, func(rocksdbTx kv.RwTx) error {
			ctx := newCombineRwTx(db.logger, mdbxTx, rocksdbTx)
			return f(ctx)
		})
	})
}

func (db *CombineDB) UpdateNosync(ctx context.Context, f func(tx kv.RwTx) error) (err error) {
	commitLock.RLock()
	defer commitLock.RUnlock()

	db.logger.Info("UpdateNosync")
	defer db.logger.Infof("UpdateNosync done. err=%v", err)

	return db.mdbx.UpdateNosync(ctx, func(mdbxTx kv.RwTx) error {
		return db.rocksdb.Update(ctx, func(rocksdbTx kv.RwTx) error {
			ctx := newCombineRwTx(db.logger, mdbxTx, rocksdbTx)
			return f(ctx)
		})
	})
}

func (db *CombineDB) BeginRw(ctx context.Context) (t kv.RwTx, err error) {
	commitLock.RLock()
	defer commitLock.RUnlock()

	db.logger.Info("BeginRw")
	defer db.logger.Infof("BeginRw done. err=%v", err)

	mdbxTx, err1 := db.mdbx.BeginRw(ctx)
	rocksdbTx, err2 := db.rocksdb.BeginRw(ctx)
	if err := assertError(db.logger, err1, err2, "BeginRw"); err != nil {
		return nil, err
	}

	return newCombineRwTx(db.logger, mdbxTx, rocksdbTx), nil
}

func (db *CombineDB) BeginRwNosync(ctx context.Context) (t kv.RwTx, err error) {
	commitLock.RLock()
	defer commitLock.RUnlock()

	db.logger.Info("BeginRwNosync")
	defer db.logger.Infof("BeginRwNosync done. err=%v", err)

	mdbxTx, err1 := db.mdbx.BeginRwNosync(ctx)
	rocksdbTx, err2 := db.rocksdb.BeginRwNosync(ctx)
	if err := assertError(db.logger, err1, err2, "BeginRwNosync"); err != nil {
		return nil, err
	}

	return newCombineRwTx(db.logger, mdbxTx, rocksdbTx), nil
}
