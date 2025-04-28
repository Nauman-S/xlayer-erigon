package rocksdb

import (
	"context"
	"fmt"
	"runtime"
	"unsafe"

	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/rocksdb/rdb"
	"github.com/ledgerwatch/erigon-lib/kv/rocksdb/rdb/common"
	"github.com/ledgerwatch/erigon-lib/kv/rocksdb/rdb/memrdb"
	"github.com/ledgerwatch/log/v3"
	"github.com/linxGnu/grocksdb"
	"golang.org/x/sync/semaphore"
)

type RocksDB struct {
	db       rdb.RDB
	lruCache *grocksdb.Cache
	bbto     *grocksdb.BlockBasedTableOptions
	opts     *grocksdb.Options
	txopts   *grocksdb.TransactionDBOptions

	closeGuard *CloseGuard

	readOnly  bool // todo: not used
	tablesCfg kv.TableCfg
	label     kv.Label // marker to distinct db instances - one process may open many databases. for example to collect metrics of only 1 database

	readTxLimiter  *semaphore.Weighted
	writeTxLimiter *semaphore.Weighted
	logger         log.Logger
	leakDetector   *dbg.LeakDetector
}

func NewRocksDB(dbPath string, logger log.Logger, tablesCfg kv.TableCfg, label kv.Label, readTxLimiter *semaphore.Weighted, readOnly bool, rdbType RDBType) (kv.RwDB, error) {
	if readTxLimiter == nil {
		targetSemCount := int64(runtime.GOMAXPROCS(-1)) - 1
		readTxLimiter = semaphore.NewWeighted(targetSemCount) // 1 less than max to allow unlocking to happen
	}
	writeTxLimiter := semaphore.NewWeighted(int64(runtime.GOMAXPROCS(-1)) - 1) // 1 less than max to allow unlocking to happen

	lruCache := grocksdb.NewLRUCache(3 << 30)

	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(lruCache)

	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetBlockBasedTableFactory(bbto)

	txopts := grocksdb.NewDefaultTransactionDBOptions()

	db, err := rdbType.NewRDB(opts, txopts, dbPath)
	if err != nil {
		return nil, err
	}

	return &RocksDB{
		db:             db,
		lruCache:       lruCache,
		bbto:           bbto,
		opts:           opts,
		txopts:         txopts,
		closeGuard:     newCloseGuard(),
		readOnly:       readOnly,
		tablesCfg:      tablesCfg,
		label:          label,
		readTxLimiter:  readTxLimiter,
		writeTxLimiter: writeTxLimiter,
		logger:         logger,
	}, nil
}

func (db *RocksDB) GetMemStorage() map[string]*common.DBValue {
	return db.db.(*memrdb.MemoryRDB).GetMemStorage()
}

// impl Closer interface
func (db *RocksDB) Close() {
	firstClose := db.closeGuard.close()
	if firstClose {
		db.db.Close()
		db.db = nil

		db.lruCache.Destroy()
		db.lruCache = nil

		db.bbto.Destroy()
		db.bbto = nil

		db.opts.Destroy()
		db.opts = nil

		db.txopts.Destroy()
		db.txopts = nil
	}
}

func (db *RocksDB) Get(table string, k []byte) (*common.DBValue, error) {
	return db.db.Get(common.MergeKey(table, k))
}

// impl Ro interface
func (db *RocksDB) ReadOnly() bool {
	return db.readOnly
}

func (db *RocksDB) View(ctx context.Context, f func(tx kv.Tx) error) error {
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	return f(tx)
}

func (db *RocksDB) BeginRo(ctx context.Context) (tx kv.Tx, err error) {
	return db.beginTx(ctx, db.readTxLimiter)
}

func (db *RocksDB) AllTables() kv.TableCfg {
	return db.tablesCfg
}

func (db *RocksDB) PageSize() uint64 {
	// note: no avaliable call to PageSize for now
	panic("not supported")
}

func (db *RocksDB) CHandle() unsafe.Pointer {
	// note: not support for RocksDB
	panic("not supported")
}

// impl RwDB interface
func (db *RocksDB) Update(ctx context.Context, f func(tx kv.RwTx) error) error {
	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	if err = f(tx); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}

	return nil
}

func (db *RocksDB) UpdateNosync(ctx context.Context, f func(tx kv.RwTx) error) error {
	tx, err := db.BeginRwNosync(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	if err = f(tx); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}

	return nil
}

func (db *RocksDB) BeginRw(ctx context.Context) (tx kv.RwTx, err error) {
	return db.beginTx(ctx, db.writeTxLimiter)
}

func (db *RocksDB) BeginRwNosync(ctx context.Context) (kv.RwTx, error) {
	// note: yztodo: there isn't a real `no sync` in rocksdb
	return db.BeginRw(ctx)
}

func (db *RocksDB) beginTx(ctx context.Context, txLimiter *semaphore.Weighted) (tx kv.RwTx, err error) {
	// don't try to acquire if the context is already done
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// otherwise carry on
	}

	// will return nil err if context is cancelled (may appear to acquire the semaphore)
	if semErr := txLimiter.Acquire(ctx, 1); semErr != nil {
		return nil, fmt.Errorf("rocksdb.BeginTx: tx limiter error %w", semErr)
	}
	defer func() {
		if tx == nil {
			txLimiter.Release(1)
		}
	}()

	if !db.closeGuard.reference() {
		return nil, fmt.Errorf("db closed")
	}
	defer func() {
		if tx == nil {
			db.closeGuard.deReference()
		}
	}()

	id := db.leakDetector.Add()
	// todo: yztodo: not a real read only tx yet
	return newRocksDbTx(db, ctx, func() {
		db.closeGuard.deReference()
		txLimiter.Release(1)
		db.leakDetector.Del(id)
	})
}
