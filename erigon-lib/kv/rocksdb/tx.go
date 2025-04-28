package rocksdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"
	"unsafe"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/ledgerwatch/erigon-lib/kv/rocksdb/rdb"
	"github.com/ledgerwatch/erigon-lib/kv/rocksdb/rdb/common"
	"github.com/linxGnu/grocksdb"
)

type RocksDbTx struct {
	db    *RocksDB
	tx    rdb.RDBTransaction
	ropts *grocksdb.ReadOptions
	wopts *grocksdb.WriteOptions
	txopt *grocksdb.TransactionOptions

	id  uint64 // set only if TRACE_TX=true
	ctx context.Context

	cursors  map[uint64]kv.Closer
	cursorID uint64
	streams  map[int]kv.Closer
	streamID int

	statelessCursors map[string]kv.RwCursor

	closed        bool
	closeCallback func()
}

func newRocksDbTx(db *RocksDB, ctx context.Context, closeCallback func()) (*RocksDbTx, error) {
	wopts := grocksdb.NewDefaultWriteOptions()
	txopt := grocksdb.NewDefaultTransactionOptions()
	tx := db.db.TransactionBegin(wopts, txopt, nil)

	return &RocksDbTx{
		db:    db,
		tx:    tx,
		ropts: grocksdb.NewDefaultReadOptions(),
		wopts: wopts,
		txopt: txopt,

		ctx: ctx,

		closeCallback: closeCallback,
	}, nil
}

func (rtx *RocksDbTx) UpdateSnapshot() {
	for _, c := range rtx.statelessCursors {
		c.(*RocksDbCursor).UpdateSnapshot()
	}
	for _, c := range rtx.cursors {
		c.(*RocksDbCursor).UpdateSnapshot()
	}
}

// impl kv.Has interface
func (rtx *RocksDbTx) Has(table string, key []byte) (bool, error) {
	_, err := rtx.tx.Get(rtx.ropts, common.MergeKey(table, key))
	if err != nil {
		if errors.Is(err, common.ErrKeyNotExist) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

// impl kv.Getter interface
func (rtx *RocksDbTx) GetOne(table string, key []byte) (val []byte, err error) {
	c, err := rtx.statelessCursor(table)
	if err != nil {
		return nil, err
	}
	_, v, err := c.SeekExact(key)
	return v, err
}

// ForEach iterates over entries with keys greater or equal to fromPrefix.
// walker is called for each eligible entry.
// If walker returns an error:
//   - implementations of local db - stop
//   - implementations of remote db - do not handle this error and may finish (send all entries to client) before error happen.
func (rtx *RocksDbTx) ForEach(table string, fromPrefix []byte, walker func(k, v []byte) error) error {
	return rtx.iterWithStop(table, fromPrefix, func(k, v []byte) (error, bool) {
		return walker(k, v), false
	})
}

func (rtx *RocksDbTx) ForPrefix(table string, prefix []byte, walker func(k, v []byte) error) error {
	return rtx.iterWithStop(table, prefix, func(k, v []byte) (error, bool) {
		if !bytes.HasPrefix(k, prefix) {
			return nil, true
		}
		return walker(k, v), false
	})
}

func (rtx *RocksDbTx) ForAmount(table string, prefix []byte, amount uint32, walker func(k, v []byte) error) error {
	if amount <= 0 {
		return nil
	}

	return rtx.iterWithStop(table, prefix, func(k, v []byte) (error, bool) {
		err := walker(k, v)
		if err != nil {
			return err, false
		}

		amount--
		return nil, amount <= 0
	})
}

// Put will append `v` to the exist value if `k` has exist
func (rtx *RocksDbTx) Put(table string, k, v []byte) error {
	c, err := rtx.statelessCursor(table)
	if err != nil {
		return err
	}
	return c.Put(k, v)
}

// impl kv.Deleter interface
// Delete removes a single entry.
func (rtx *RocksDbTx) Delete(table string, k []byte) error {
	c, err := rtx.statelessCursor(table)
	if err != nil {
		return err
	}
	return c.Delete(k)
}

// impl kv.StatelessReadTx interface
func (rtx *RocksDbTx) Commit() error { // Commit all the operations of a transaction into the database.
	if rtx.closed {
		return nil
	}
	rtx.CollectMetrics()

	now := time.Now()
	if err := rtx.close(rtx.tx.Commit); err != nil {
		return fmt.Errorf("label: %s, %w", rtx.db.label, err)
	}

	if rtx.db.label == kv.ChainDB {
		kv.DbCommitTotal.Observe(time.Since(now).Seconds())
	}
	return nil
}

func (rtx *RocksDbTx) Rollback() { // Rollback - abandon all the operations of the transaction instead of saving them.
	if rtx.closed {
		return
	}
	if err := rtx.close(rtx.tx.Rollback); err != nil {
		panic(fmt.Sprintf("rocksdb tx: rollback failed: %v", err))
	}
}

// ReadSequence - allows to create a linear sequence of unique positive integers for each table.
// Can be called for a read transaction to retrieve the current sequence value, and the increment must be zero.
// Sequence changes become visible outside the current write transaction after it is committed, and discarded on abort.
// Starts from 0.
func (rtx *RocksDbTx) ReadSequence(table string) (uint64, error) {
	dbv, err := rtx.get(kv.Sequence, []byte(table))
	notExist := errors.Is(err, common.ErrKeyNotExist)
	if err != nil && !notExist {
		return 0, err
	}

	var currentV uint64
	if !notExist {
		data := dbv.First()
		currentV = binary.BigEndian.Uint64(data)
	}

	return currentV, nil
}

// impl kv.BucketMigratorRO interface
func (rtx *RocksDbTx) ListBuckets() ([]string, error) {
	// yztodo
	panic("not implemented")
}

// impl kv.Tx interface
// ID returns the identifier associated with this transaction. For a
// read-only transaction, this corresponds to the snapshot being read;
// concurrent readers will frequently have the same transaction ID.
func (rtx *RocksDbTx) ViewID() uint64 {
	// todo: yztodo seems not important
	return 0
}

// Cursor - creates cursor object on top of given bucket. Type of cursor - depends on bucket configuration.
// If bucket was created with mdbx.DupSort flag, then cursor with interface CursorDupSort created
// Otherwise - object of interface Cursor created
//
// Cursor, also provides a grain of magic - it can use a declarative configuration - and automatically break
// long keys into DupSort key/values. See docs for `bucket.go:TableCfgItem`
func (rtx *RocksDbTx) Cursor(table string) (cur kv.Cursor, err error) {
	return rtx.RwCursor(table)
}

func (rtx *RocksDbTx) CursorDupSort(table string) (kv.CursorDupSort, error) { // CursorDupSort - can be used if bucket has mdbx.DupSort flag
	return rtx.RwCursorDupSort(table)
}

func (rtx *RocksDbTx) DBSize() (uint64, error) {
	panic("yztodo not implemented(and no valiable call)")
}

// --- High-Level methods: 1request -> stream of server-side pushes ---

// Range [from, to)
// Range(from, nil) means [from, EndOfTable)
// Range(nil, to)   means [StartOfTable, to)
func (rtx *RocksDbTx) Range(table string, fromPrefix, toPrefix []byte) (iter.KV, error) {
	return rtx.RangeAscend(table, fromPrefix, toPrefix, -1)
}

// Stream is like Range, but for requesting huge data (Example: full table scan). Client can't stop it.
// Stream(table string, fromPrefix, toPrefix []byte) (iter.KV, error)
// RangeAscend - like Range [from, to) but also allow pass Limit parameters
// Limit -1 means Unlimited
func (rtx *RocksDbTx) RangeAscend(table string, fromPrefix, toPrefix []byte, limit int) (iter.KV, error) {
	return rtx.rangeOrderLimit(table, fromPrefix, toPrefix, order.Asc, limit)
}

// StreamAscend(table string, fromPrefix, toPrefix []byte, limit int) (iter.KV, error)
// RangeDescend - is like Range [from, to), but expecing `from`<`to`
// example: RangeDescend("Table", "B", "A", -1)
func (rtx *RocksDbTx) RangeDescend(table string, fromPrefix, toPrefix []byte, limit int) (iter.KV, error) {
	return rtx.rangeOrderLimit(table, fromPrefix, toPrefix, order.Desc, limit)
}

// StreamDescend(table string, fromPrefix, toPrefix []byte, limit int) (kv.iter.KV, error)
// Prefix - is exactly Range(Table, prefix, kv.NextSubtree(prefix))
func (rtx *RocksDbTx) Prefix(table string, prefix []byte) (iter.KV, error) {
	nextPrefix, ok := kv.NextSubtree(prefix)
	if !ok {
		return rtx.Range(table, prefix, nil)
	}
	return rtx.Range(table, prefix, nextPrefix)
}

// RangeDupSort - like Range but for fixed single key and iterating over range of values
func (rtx *RocksDbTx) RangeDupSort(table string, key []byte, fromPrefix, toPrefix []byte, asc order.By, limit int) (iter.KV, error) {
	s := &cursorDup2iter{ctx: rtx.ctx, tx: rtx, key: key, fromPrefix: fromPrefix, toPrefix: toPrefix, orderAscend: bool(asc), limit: int64(limit), id: rtx.streamID}
	rtx.streamID++
	if rtx.streams == nil {
		rtx.streams = map[int]kv.Closer{}
	}
	rtx.streams[s.id] = s
	return s.init(table, rtx)
}

// --- High-Level methods: 1request -> 1page of values in response -> send next page request ---
// Paginate(table string, fromPrefix, toPrefix []byte) (PairsStream, error)

// --- High-Level deprecated methods ---

// Pointer to the underlying C transaction handle (e.g. *C.MDBX_txn)
func (rtx *RocksDbTx) CHandle() unsafe.Pointer {
	panic("yztodo: not supported")
}
func (rtx *RocksDbTx) BucketSize(table string) (uint64, error) {
	panic("yztodo: not implemented for now")
}

// impl
/*
// if need N id's:
baseId, err := tx.IncrementSequence(bucket, N)

	if err != nil {
		return err
	}

for i := 0; i < N; i++ {    // if N == 0, it will work as expected

		id := baseId + i
		// use id
	}

// or if need only 1 id:
id, err := tx.IncrementSequence(bucket, 1)

	if err != nil {
		return err
	}

// use id
*/
func (rtx *RocksDbTx) IncrementSequence(table string, amount uint64) (uint64, error) {
	currentV, err := rtx.ReadSequence(table)
	if err != nil {
		return 0, err
	}

	newV := currentV + amount
	newVBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(newVBytes, newV)
	dbv := common.DBValueWithOneValue(newVBytes)

	return currentV, rtx.putOverwrite(kv.Sequence, []byte(table), dbv)
}

func (rtx *RocksDbTx) Append(table string, k, v []byte) error {
	c, err := rtx.statelessCursor(table)
	if err != nil {
		return err
	}
	return c.Append(k, v)
}

func (rtx *RocksDbTx) AppendDup(table string, k, v []byte) error {
	c, err := rtx.statelessCursor(table)
	if err != nil {
		return err
	}
	return c.(*RocksDbDupSortCursor).AppendDup(k, v)
}

// impl BucketMigrator interface
func (rtx *RocksDbTx) DropBucket(table string) error {
	if cfg, ok := rtx.db.tablesCfg[table]; !(ok && cfg.IsDeprecated) {
		return fmt.Errorf("%w, bucket: %s", kv.ErrAttemptToDeleteNonDeprecatedBucket, table)
	}

	return rtx.dropEvenIfBucketIsNotDeprecated(table)
}
func (rtx *RocksDbTx) CreateBucket(string) error {
	// do nothing, for bucket( or table) is just a prefix of key in rocksdb
	return nil
}
func (rtx *RocksDbTx) ExistsBucket(string) (bool, error) {
	// yztodo
	panic("not supported")
}
func (rtx *RocksDbTx) ClearBucket(table string) error {
	beginPrefix := common.MergeKey(table, []byte{})
	endPrefix, _ := kv.NextSubtree(beginPrefix)

	iterateBatch := func() ([][]byte, bool) {
		it := rtx.tx.NewIterator(beginPrefix, endPrefix)
		defer it.Close()
		keyBatch := make([][]byte, 0)
		for it.SeekToFirst(); it.Valid(); it.Next() {
			keyBatch = append(keyBatch, it.Key())
			if len(keyBatch) >= 1000000 {
				return keyBatch, false
			}
		}
		return keyBatch, true
	}
	deleteBatch := func(keyBatch [][]byte) {
		for _, key := range keyBatch {
			if err := rtx.tx.Delete(key); err != nil {
				panic(fmt.Errorf("failed to delete key %x when ClearBucket: %v", key, err))
			}
		}
	}

	for {
		keyBatch, isExhaust := iterateBatch()
		deleteBatch(keyBatch)
		if isExhaust {
			break
		}
	}

	return nil
}

// impl RwTx interface
func (rtx *RocksDbTx) RwCursor(table string) (c kv.RwCursor, err error) {
	b := rtx.db.tablesCfg[table]
	if b.AutoDupSortKeysConversion {
		return rtx.stdCursor(table)
	}

	if b.Flags&kv.DupSort != 0 {
		return rtx.RwCursorDupSort(table)
	}

	return rtx.stdCursor(table)
}

func (rtx *RocksDbTx) SpaceDirty() (uint64, uint64, error) {
	// todo: not found appropriate function to get those infos,
	// so if `SpaceDirty` is important, we should compute it by ourself
	return 0, 0, nil
}

func (rtx *RocksDbTx) close(action func() error) error {
	if rtx.closed {
		return nil
	}

	defer func() {
		rtx.closeCursors()
		rtx.closeCallback()

		rtx.tx.Destroy()
		rtx.tx = nil

		rtx.ropts.Destroy()
		rtx.ropts = nil

		rtx.wopts.Destroy()
		rtx.wopts = nil

		rtx.txopt.Destroy()
		rtx.txopt = nil

		rtx.closed = true
	}()

	return action()
}

func (rtx *RocksDbTx) closeCursors() {
	for _, c := range rtx.cursors {
		if c != nil {
			c.Close()
		}
	}
	rtx.cursors = nil

	for _, c := range rtx.streams {
		if c != nil {
			c.Close()
		}
	}
	rtx.streams = nil

	for _, c := range rtx.statelessCursors {
		if c != nil {
			c.Close()
		}
	}
	rtx.statelessCursors = nil
}

func (rtx *RocksDbTx) statelessCursor(bucket string) (kv.RwCursor, error) {
	if rtx.statelessCursors == nil {
		rtx.statelessCursors = make(map[string]kv.RwCursor)
	}
	c, ok := rtx.statelessCursors[bucket]
	if !ok {
		var err error
		c, err = rtx.RwCursor(bucket)
		if err != nil {
			return nil, err
		}
		rtx.statelessCursors[bucket] = c
	}
	return c, nil
}

func (rtx *RocksDbTx) stdCursor(table string) (kv.RwCursor, error) {
	b := rtx.db.tablesCfg[table]
	c, err := newRocksDbCursorRW(table, b, rtx, rtx.cursorID)
	if err != nil {
		return nil, err
	}

	rtx.cursorID++

	// add to auto-cleanup on end of transactions
	if rtx.cursors == nil {
		rtx.cursors = make(map[uint64]kv.Closer)
	}
	rtx.cursors[c.id] = c
	return c, nil
}

func (rtx *RocksDbTx) RwCursorDupSort(table string) (c kv.RwCursorDupSort, err error) {
	basicCursor, err := rtx.stdCursor(table)
	if err != nil {
		return nil, err
	}
	return &RocksDbDupSortCursor{RocksDbCursor: basicCursor.(*RocksDbCursor)}, nil
}

// CollectMetrics - does collect all DB-related and Tx-related metrics
// this method exists only in RwTx to avoid concurrency
func (rtx *RocksDbTx) CollectMetrics() {
	// yztodo: not implemented
}

func (rtx *RocksDbTx) get(table string, k []byte) (*common.DBValue, error) {
	dbv, err := rtx.tx.Get(rtx.ropts, common.MergeKey(table, k))
	if err != nil {
		return nil, err
	}

	//yztodo: cache this not dirty dbv?
	return dbv, nil
}

func (rtx *RocksDbTx) putSorted(table string, k, v []byte) error {
	dbv, err := rtx.get(table, k)
	notExist := errors.Is(err, common.ErrKeyNotExist)
	if err != nil && !notExist {
		return err
	}

	if notExist {
		dbv = common.DBValueWithOneValue(v)
	} else {
		dbv.SortedInsert(v)
	}
	return rtx.putOverwrite(table, k, dbv)
}

// putOverwrite will overwrite the key if it has exist
func (rtx *RocksDbTx) putOverwrite(table string, k []byte, v *common.DBValue) error {
	return rtx.tx.Put(common.MergeKey(table, k), v)
}

// iterWithStop iterate data until `walker` return error or true
func (rtx *RocksDbTx) iterWithStop(table string, fromPrefix []byte, walker func(k, v []byte) (error, bool)) error {
	c, err := rtx.Cursor(table)
	if err != nil {
		return err
	}
	defer c.Close()

	for k, v, err := c.Seek(fromPrefix); k != nil; k, v, err = c.Next() {
		if err != nil {
			return err
		}
		err, isStop := walker(k, v)
		if err != nil {
			return err
		}
		if isStop {
			break
		}
	}
	return nil
}

func (rtx *RocksDbTx) rangeOrderLimit(table string, fromPrefix, toPrefix []byte, orderAscend order.By, limit int) (*cursor2iter, error) {
	s := &cursor2iter{ctx: rtx.ctx, tx: rtx, fromPrefix: fromPrefix, toPrefix: toPrefix, orderAscend: orderAscend, limit: int64(limit), id: rtx.streamID}
	rtx.streamID++
	if rtx.streams == nil {
		rtx.streams = map[int]kv.Closer{}
	}
	rtx.streams[s.id] = s
	return s.init(table, rtx)
}

func (rtx *RocksDbTx) dropEvenIfBucketIsNotDeprecated(name string) error {
	panic("not supported")
}

func (rtx *RocksDbTx) delete(table string, k []byte) error {
	return rtx.tx.Delete(common.MergeKey(table, k))
}

type cursor2iter struct {
	c  kv.Cursor
	id int
	tx *RocksDbTx

	fromPrefix, toPrefix, nextK, nextV []byte
	orderAscend                        order.By
	limit                              int64
	ctx                                context.Context
}

func (s *cursor2iter) init(table string, tx kv.Tx) (*cursor2iter, error) {
	if s.orderAscend && s.fromPrefix != nil && s.toPrefix != nil && bytes.Compare(s.fromPrefix, s.toPrefix) >= 0 {
		return s, fmt.Errorf("tx.Dual: %x must be lexicographicaly before %x", s.fromPrefix, s.toPrefix)
	}
	if !s.orderAscend && s.fromPrefix != nil && s.toPrefix != nil && bytes.Compare(s.fromPrefix, s.toPrefix) <= 0 {
		return s, fmt.Errorf("tx.Dual: %x must be lexicographicaly before %x", s.toPrefix, s.fromPrefix)
	}
	c, err := tx.Cursor(table)
	if err != nil {
		return s, err
	}
	s.c = c

	if s.fromPrefix == nil { // no initial position
		if s.orderAscend {
			s.nextK, s.nextV, err = s.c.First()
		} else {
			s.nextK, s.nextV, err = s.c.Last()
		}
		return s, err
	}

	if s.orderAscend {
		s.nextK, s.nextV, err = s.c.Seek(s.fromPrefix)
		return s, err
	} else {
		// seek exactly to given key or previous one
		s.nextK, s.nextV, err = s.c.SeekExact(s.fromPrefix)
		if err != nil {
			return s, err
		}
		if s.nextK != nil { // go to last value of this key
			if casted, ok := s.c.(kv.CursorDupSort); ok {
				s.nextV, err = casted.LastDup()
			}
		} else { // key not found, go to prev one
			s.nextK, s.nextV, err = s.c.Prev()
		}
		return s, err
	}
}

func (s *cursor2iter) advance() (err error) {
	if s.orderAscend {
		s.nextK, s.nextV, err = s.c.Next()
	} else {
		s.nextK, s.nextV, err = s.c.Prev()
	}
	return err
}

func (s *cursor2iter) Close() {
	if s.c != nil {
		s.c.Close()
		delete(s.tx.streams, s.id)
		s.c = nil
	}
}

func (s *cursor2iter) HasNext() bool {
	if s.limit == 0 { // limit reached
		return false
	}
	if s.nextK == nil { // EndOfTable
		return false
	}
	if s.toPrefix == nil { // s.nextK == nil check is above
		return true
	}

	//Asc:  [from, to) AND from > to
	//Desc: [from, to) AND from < to
	cmp := bytes.Compare(s.nextK, s.toPrefix)
	return (bool(s.orderAscend) && cmp < 0) || (!bool(s.orderAscend) && cmp > 0)
}

func (s *cursor2iter) Next() (k, v []byte, err error) {
	select {
	case <-s.ctx.Done():
		return nil, nil, s.ctx.Err()
	default:
	}
	s.limit--
	k, v = s.nextK, s.nextV
	if err = s.advance(); err != nil {
		return nil, nil, err
	}
	return k, v, nil
}

type cursorDup2iter struct {
	c  kv.CursorDupSort
	id int
	tx *RocksDbTx

	key                         []byte
	fromPrefix, toPrefix, nextV []byte
	orderAscend                 bool
	limit                       int64
	ctx                         context.Context
}

func (s *cursorDup2iter) init(table string, tx kv.Tx) (*cursorDup2iter, error) {
	if s.orderAscend && s.fromPrefix != nil && s.toPrefix != nil && bytes.Compare(s.fromPrefix, s.toPrefix) >= 0 {
		return s, fmt.Errorf("tx.Dual: %x must be lexicographicaly before %x", s.fromPrefix, s.toPrefix)
	}
	if !s.orderAscend && s.fromPrefix != nil && s.toPrefix != nil && bytes.Compare(s.fromPrefix, s.toPrefix) <= 0 {
		return s, fmt.Errorf("tx.Dual: %x must be lexicographicaly before %x", s.toPrefix, s.fromPrefix)
	}
	c, err := tx.CursorDupSort(table)
	if err != nil {
		return s, err
	}
	s.c = c
	k, _, err := c.SeekExact(s.key)
	if err != nil {
		return s, err
	}
	if k == nil {
		return s, nil
	}

	if s.fromPrefix == nil { // no initial position
		if s.orderAscend {
			s.nextV, err = s.c.FirstDup()
		} else {
			s.nextV, err = s.c.LastDup()
		}
		return s, err
	}

	if s.orderAscend {
		s.nextV, err = s.c.SeekBothRange(s.key, s.fromPrefix)
		return s, err
	} else {
		// seek exactly to given key or previous one
		_, s.nextV, err = s.c.SeekBothExact(s.key, s.fromPrefix)
		if s.nextV == nil { // no such key
			_, s.nextV, err = s.c.PrevDup()
		}
		return s, err
	}
}

func (s *cursorDup2iter) advance() (err error) {
	if s.orderAscend {
		_, s.nextV, err = s.c.NextDup()
	} else {
		_, s.nextV, err = s.c.PrevDup()
	}
	return err
}

func (s *cursorDup2iter) Close() {
	if s.c != nil {
		s.c.Close()
		delete(s.tx.streams, s.id)
		s.c = nil
	}
}
func (s *cursorDup2iter) HasNext() bool {
	if s.limit == 0 { // limit reached
		return false
	}
	if s.nextV == nil { // EndOfTable
		return false
	}
	if s.toPrefix == nil { // s.nextK == nil check is above
		return true
	}

	//Asc:  [from, to) AND from > to
	//Desc: [from, to) AND from < to
	cmp := bytes.Compare(s.nextV, s.toPrefix)
	return (s.orderAscend && cmp < 0) || (!s.orderAscend && cmp > 0)
}
func (s *cursorDup2iter) Next() (k, v []byte, err error) {
	select {
	case <-s.ctx.Done():
		return nil, nil, s.ctx.Err()
	default:
	}
	s.limit--
	v = s.nextV
	if err = s.advance(); err != nil {
		return nil, nil, err
	}
	return s.key, v, nil
}
