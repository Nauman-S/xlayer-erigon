package combinedb

import (
	"fmt"
	"sync/atomic"

	"github.com/ledgerwatch/erigon-lib/kv"
)

type CombineCursor struct {
	mdbxCursor    kv.Cursor
	rocksdbCursor kv.Cursor

	logger *combineLogger
}

type CombineRwCursor struct {
	*CombineCursor

	mdbxCursor    kv.RwCursor
	rocksdbCursor kv.RwCursor
}

type CombineCursorDupSort struct {
	*CombineCursor

	mdbxCursor    kv.CursorDupSort
	rocksdbCursor kv.CursorDupSort
}

type CombineRwCursorDupSort struct {
	*CombineCursorDupSort
	*CombineRwCursor

	mdbxCursor    kv.RwCursorDupSort
	rocksdbCursor kv.RwCursorDupSort
}

var cursorCounter atomic.Uint64

func newCombineCursor(parentLogger *combineLogger, mdbxCursor, rocksdbCursor kv.Cursor, table string) *CombineCursor {
	logger := newCombinLogger(parentLogger.isEnable(), fmt.Sprintf("%s cursorid=%d table=%s", parentLogger.getPrefix(), cursorCounter.Add(1), table))
	logger.Info("create combine cursor", "table", table)
	return &CombineCursor{
		mdbxCursor:    mdbxCursor,
		rocksdbCursor: rocksdbCursor,
		logger:        logger,
	}
}
func newCombineRwCursor(parentLogger *combineLogger, mdbxCursor, rocksdbCursor kv.RwCursor, table string) *CombineRwCursor {
	return &CombineRwCursor{
		CombineCursor: newCombineCursor(parentLogger, mdbxCursor, rocksdbCursor, table),
		mdbxCursor:    mdbxCursor,
		rocksdbCursor: rocksdbCursor,
	}
}

func newCombineCursorDupSort(parentLogger *combineLogger, mdbxCursor, rocksdbCursor kv.CursorDupSort, table string) *CombineCursorDupSort {
	return &CombineCursorDupSort{
		CombineCursor: newCombineCursor(parentLogger, mdbxCursor, rocksdbCursor, table),
		mdbxCursor:    mdbxCursor,
		rocksdbCursor: rocksdbCursor,
	}
}

func newCombineRwCursorDupSort(parentLogger *combineLogger, mdbxCursor, rocksdbCursor kv.RwCursorDupSort, table string) kv.RwCursorDupSort {
	return &CombineRwCursorDupSort{
		CombineCursorDupSort: newCombineCursorDupSort(parentLogger, mdbxCursor, rocksdbCursor, table),
		CombineRwCursor:      newCombineRwCursor(parentLogger, mdbxCursor, rocksdbCursor, table),

		mdbxCursor:    mdbxCursor,
		rocksdbCursor: rocksdbCursor,
	}
}

func (c *CombineCursor) First() (k []byte, v []byte, err error) {
	c.logger.Info("First()")
	defer c.logger.Infof("First() done. k=%x, v=%x, err=%v", k, v, err)

	k1, v1, err1 := c.mdbxCursor.First()
	k2, v2, err2 := c.rocksdbCursor.First()
	if err := assertError(c.logger, err1, err2, "First"); err != nil {
		return nil, nil, err
	}

	assertEqualF(c.logger, k1, k2, "First key mismatch. mdbx: %x. rocksdb: %x", k1, k2)
	assertEqualF(c.logger, v1, v2, "First value mismatch. mdbx: %x. rocksdb: %x", v1, v2)
	return k1, v2, nil
}

func (c *CombineCursor) Seek(key []byte) (k []byte, v []byte, err error) {
	c.logger.Infof("Seek(key=%x)", key)
	defer c.logger.Infof("Seek(key=%x) done. k=%x, v=%x, err=%v", key, k, v, err)

	k1, v1, err1 := c.mdbxCursor.Seek(key)
	k2, v2, err2 := c.rocksdbCursor.Seek(key)
	if err := assertError(c.logger, err1, err2, "Seek"); err != nil {
		return nil, nil, err
	}

	assertEqualF(c.logger, k1, k2, "Seek key mismatch. mdbx: %x. rocksdb: %x", k1, k2)
	assertEqualF(c.logger, v1, v2, "Seek value mismatch. mdbx: %x. rocksdb: %x", v1, v2)
	return k1, v2, nil
}

func (c *CombineCursor) SeekExact(key []byte) (k []byte, v []byte, err error) {
	c.logger.Infof("SeekExact(key=%x)", key)
	defer c.logger.Infof("SeekExact(key=%x) done. k=%x, v=%x, err=%v", key, k, v, err)

	k1, v1, err1 := c.mdbxCursor.SeekExact(key)
	k2, v2, err2 := c.rocksdbCursor.SeekExact(key)
	if err := assertError(c.logger, err1, err2, "SeekExact"); err != nil {
		return nil, nil, err
	}

	assertEqualF(c.logger, k1, k2, "SeekExact key mismatch. mdbx: %x. rocksdb: %x", k1, k2)
	assertEqualF(c.logger, v1, v2, "SeekExact value mismatch. mdbx: %x. rocksdb: %x", v1, v2)
	return k1, v2, nil
}

func (c *CombineCursor) Next() (k []byte, v []byte, err error) {
	c.logger.Info("Next()")
	defer c.logger.Infof("Next() done. k=%x, v=%x, err=%v", k, v, err)

	k1, v1, err1 := c.mdbxCursor.Next()
	k2, v2, err2 := c.rocksdbCursor.Next()
	if err := assertError(c.logger, err1, err2, "Next"); err != nil {
		return nil, nil, err
	}

	assertEqualF(c.logger, k1, k2, "Next key mismatch. mdbx: %x. rocksdb: %x", k1, k2)
	assertEqualF(c.logger, v1, v2, "Next value mismatch. mdbx: %x. rocksdb: %x", v1, v2)
	return k1, v2, nil
}

func (c *CombineCursor) Prev() (k []byte, v []byte, err error) {
	c.logger.Info("Prev()")
	defer c.logger.Infof("Prev() done. k=%x, v=%x, err=%v", k, v, err)

	k1, v1, err1 := c.mdbxCursor.Prev()
	k2, v2, err2 := c.rocksdbCursor.Prev()
	if err := assertError(c.logger, err1, err2, "Prev"); err != nil {
		return nil, nil, err
	}

	assertEqualF(c.logger, k1, k2, "Prev key mismatch. mdbx: %x. rocksdb: %x", k1, k2)
	assertEqualF(c.logger, v1, v2, "Prev value mismatch. mdbx: %x. rocksdb: %x", k1, k2)
	return k1, v2, nil
}

func (c *CombineCursor) Last() (k []byte, v []byte, err error) {
	c.logger.Info("Last()")
	defer c.logger.Infof("Last() done. k=%x, v=%x, err=%v", k, v, err)

	k1, v1, err1 := c.mdbxCursor.Last()
	k2, v2, err2 := c.rocksdbCursor.Last()
	if err := assertError(c.logger, err1, err2, "Last"); err != nil {
		return nil, nil, err
	}

	assertEqualF(c.logger, k1, k2, "Last key mismatch. mdbx: %x. rocksdb: %x", k1, k2)
	assertEqualF(c.logger, v1, v2, "Last value mismatch. mdbx: %x. rocksdb: %x", v1, v2)
	return k1, v2, nil
}

func (c *CombineCursor) Current() (k []byte, v []byte, err error) {
	c.logger.Info("Current()")
	defer c.logger.Infof("Current() done. k=%x, v=%x, err=%v", k, v, err)

	k1, v1, err1 := c.mdbxCursor.Current()
	k2, v2, err2 := c.rocksdbCursor.Current()
	if err := assertError(c.logger, err1, err2, "Current"); err != nil {
		return nil, nil, err
	}

	assertEqualF(c.logger, k1, k2, "Current key mismatch. mdbx: %x. rocksdb: %x", k1, k2)
	assertEqualF(c.logger, v1, v2, "Current value mismatch. mdbx: %x. rocksdb: %x", v1, v2)
	return k1, v2, nil
}

func (c *CombineCursor) Count() (cnt uint64, err error) {
	c.logger.Info("Count()")
	defer c.logger.Infof("Count() done. cnt=%d, err=%v", c, err)

	v1, err1 := c.mdbxCursor.Count()
	v2, err2 := c.rocksdbCursor.Count()
	if err := assertError(c.logger, err1, err2, "Count"); err != nil {
		return 0, err
	}

	assertEqualF(c.logger, v1, v2, "Count mismatch. mdbx: %x. rocksdb: %x", v1, v2)
	return v1, nil
}

func (c *CombineCursor) Close() {
	c.logger.Info("Close()")
	defer c.logger.Info("Close() done")

	c.mdbxCursor.Close()
	c.rocksdbCursor.Close()
}

func (c *CombineRwCursor) Put(k, v []byte) (err error) {
	c.logger.Infof("Put(k=%x, v=%x)", k, v)
	defer c.logger.Infof("Put(k=%x, v=%x) done. error=%v", k, v, err)

	err1 := c.mdbxCursor.Put(k, v)
	err2 := c.rocksdbCursor.Put(k, v)
	return assertError(c.logger, err1, err2, "Put")
}

func (c *CombineRwCursor) Append(k []byte, v []byte) (err error) {
	c.logger.Infof("Append(k=%x, v=%x)", k, v)
	defer c.logger.Infof("Append(k=%x, v=%x) done. err=%v", k, v, err)

	err1 := c.mdbxCursor.Append(k, v)
	err2 := c.rocksdbCursor.Append(k, v)
	return assertError(c.logger, err1, err2, "Append")
}

func (c *CombineRwCursor) Delete(k []byte) (err error) {
	c.logger.Infof("Delete(k=%x)", k)
	defer c.logger.Infof("Delete(k=%x) done. err=%v", k, err)

	err1 := c.mdbxCursor.Delete(k)
	err2 := c.rocksdbCursor.Delete(k)
	return assertError(c.logger, err1, err2, "Delete")
}

func (c *CombineRwCursor) DeleteCurrent() (err error) {
	c.logger.Info("DeleteCurrent()")
	defer c.logger.Infof("DeleteCurrent() done. err=%v", err)

	k1, v1, err1 := c.mdbxCursor.Current()
	k2, v2, err2 := c.rocksdbCursor.Current()
	assertEqualF(c.logger, k1, k2, "DeleteCurrent key mismatch. mdbx: %x. rocksdb: %x", k1, k2)
	assertEqualF(c.logger, v1, v2, "DeleteCurrent value mismatch. mdbx: %x. rocksdb: %x", v1, v2)

	err1 = c.mdbxCursor.DeleteCurrent()
	err2 = c.rocksdbCursor.DeleteCurrent()
	if err := assertError(c.logger, err1, err2, "DeleteCurrent"); err != nil {
		return err
	}

	// make sure the current value is still same after delete
	k1, v1, _ = c.mdbxCursor.Current()
	k2, v2, _ = c.rocksdbCursor.Current()
	assertEqualF(c.logger, k1, k2, "DeleteCurrent after delete key mismatch. mdbx: %x. rocksdb: %x", k1, k2)
	assertEqualF(c.logger, v1, v2, "DeleteCurrent after delete value mismatch. mdbx: %x. rocksdb: %x", v1, v2)

	return nil
}

func (c *CombineCursorDupSort) SeekBothExact(key, value []byte) (k []byte, v []byte, err error) {
	c.logger.Infof("SeekBothExact(key=%x, value=%x)", key, value)
	defer c.logger.Infof("SeekBothExact(key=%x, value=%x) done. k=%x, v=%x, err=%v", key, value, k, v, err)

	k1, v1, err1 := c.mdbxCursor.SeekBothExact(key, value)
	k2, v2, err2 := c.rocksdbCursor.SeekBothExact(key, value)
	if err := assertError(c.logger, err1, err2, "SeekBothExact"); err != nil {
		return nil, nil, err
	}

	assertEqualF(c.logger, k1, k2, "SeekBothExact key mismatch. mdbx: %x. rocksdb: %x", k1, k2)
	assertEqualF(c.logger, v1, v2, "SeekBothExact value mismatch. mdbx: %x. rocksdb: %x", v1, v2)
	return k1, v1, nil
}

func (c *CombineCursorDupSort) SeekBothRange(key, value []byte) (v []byte, err error) {
	c.logger.Infof("SeekBothRange(key=%x, value=%x)", key, value)
	defer c.logger.Infof("SeekBothRange(key=%x, value=%x) done. v=%x, err=%v", key, value, v, err)

	v1, err1 := c.mdbxCursor.SeekBothRange(key, value)
	v2, err2 := c.rocksdbCursor.SeekBothRange(key, value)
	if err := assertError(c.logger, err1, err2, "SeekBothRange"); err != nil {
		return nil, err
	}

	assertEqualF(c.logger, v1, v2, "SeekBothRange mismatch. mdbx: %x. rocksdb: %x", v1, v2)
	return v1, nil
}

func (c *CombineCursorDupSort) FirstDup() (v []byte, err error) {
	c.logger.Info("FirstDup()")
	defer c.logger.Infof("FirstDup() done. v=%x, err=%v", v, err)

	v1, err1 := c.mdbxCursor.FirstDup()
	v2, err2 := c.rocksdbCursor.FirstDup()
	if err := assertError(c.logger, err1, err2, "FirstDup"); err != nil {
		return nil, err
	}

	assertEqualF(c.logger, v1, v2, "FirstDup mismatch. mdbx: %x. rocksdb: %x", v1, v2)
	return v1, nil
}

func (c *CombineCursorDupSort) NextDup() (k []byte, v []byte, err error) {
	c.logger.Info("NextDup()")
	defer c.logger.Infof("NextDup() done. k=%x, v=%x, err=%v", k, v, err)

	k1, v1, err1 := c.mdbxCursor.NextDup()
	k2, v2, err2 := c.rocksdbCursor.NextDup()
	if err := assertError(c.logger, err1, err2, "NextDup"); err != nil {
		return nil, nil, err
	}

	assertEqualF(c.logger, k1, k2, "NextDup key mismatch. mdbx: %x. rocksdb: %x", k1, k2)
	assertEqualF(c.logger, v1, v2, "NextDup value mismatch. mdbx: %x. rocksdb: %x", v1, v2)
	return k1, v1, nil
}

func (c *CombineCursorDupSort) NextNoDup() (k []byte, v []byte, err error) {
	c.logger.Info("NextNoDup()")
	defer c.logger.Infof("NextNoDup() done. k=%x, v=%x, err=%v", k, v, err)

	k1, v1, err1 := c.mdbxCursor.NextNoDup()
	k2, v2, err2 := c.rocksdbCursor.NextNoDup()
	if err := assertError(c.logger, err1, err2, "NextNoDup"); err != nil {
		return nil, nil, err
	}

	assertEqualF(c.logger, k1, k2, "NextNoDup key mismatch. mdbx: %x. rocksdb: %x", k1, k2)
	assertEqualF(c.logger, v1, v2, "NextNoDup value mismatch. mdbx: %x. rocksdb: %x", v1, v2)
	return k1, v1, nil
}

func (c *CombineCursorDupSort) PrevDup() (k []byte, v []byte, err error) {
	c.logger.Info("PrevDup()")
	defer c.logger.Infof("PrevDup() done. k=%x, v=%x, err=%v", k, v, err)

	k1, v1, err1 := c.mdbxCursor.PrevDup()
	k2, v2, err2 := c.rocksdbCursor.PrevDup()
	if err := assertError(c.logger, err1, err2, "PrevDup"); err != nil {
		return nil, nil, err
	}

	assertEqualF(c.logger, k1, k2, "PrevDup key mismatch. mdbx: %x. rocksdb: %x", k1, k2)
	assertEqualF(c.logger, v1, v2, "PrevDup value mismatch. mdbx: %x. rocksdb: %x", v1, v2)
	return k1, v1, nil
}

func (c *CombineCursorDupSort) PrevNoDup() (k []byte, v []byte, err error) {
	c.logger.Info("PrevNoDup()")
	defer c.logger.Infof("PrevNoDup() done. k=%x, v=%x, err=%v", k, v, err)

	k1, v1, err1 := c.mdbxCursor.PrevNoDup()
	k2, v2, err2 := c.rocksdbCursor.PrevNoDup()
	if err := assertError(c.logger, err1, err2, "PrevNoDup"); err != nil {
		return nil, nil, err
	}

	assertEqualF(c.logger, k1, k2, "PrevNoDup key mismatch. mdbx: %x. rocksdb: %x", k1, k2)
	assertEqualF(c.logger, v1, v2, "PrevNoDup value mismatch. mdbx: %x. rocksdb: %x", v1, v2)
	return k1, v1, nil
}

func (c *CombineCursorDupSort) LastDup() (v []byte, err error) {
	c.logger.Info("LastDup()")
	defer c.logger.Infof("LastDup() done. v=%x, err=%v", v, err)

	v1, err1 := c.mdbxCursor.LastDup()
	v2, err2 := c.mdbxCursor.LastDup()
	if err := assertError(c.logger, err1, err2, "LastDup"); err != nil {
		return nil, err
	}

	assertEqualF(c.logger, v1, v2, "LastDup mismatch. mdbx: %x. rocksdb: %x", v1, v2)
	return v1, nil
}

func (c *CombineCursorDupSort) CountDuplicates() (v uint64, err error) {
	c.logger.Info("CountDuplicates()")
	defer c.logger.Infof("CountDuplicates() done. v=%x, err=%v", v, err)

	v1, err1 := c.mdbxCursor.CountDuplicates()
	v2, err2 := c.rocksdbCursor.CountDuplicates()
	if err := assertError(c.logger, err1, err2, "CountDuplicates"); err != nil {
		return 0, err
	}

	assertEqualF(c.logger, v1, v2, "CountDuplicates mismatch. mdbx: %d. rocksdb: %d", v1, v2)
	return v1, nil
}

func (c *CombineRwCursorDupSort) PutNoDupData(key, value []byte) (err error) {
	c.CombineCursorDupSort.logger.Infof("PutNoDupData(key=%x, value=%x)", key, value)
	defer c.CombineCursorDupSort.logger.Infof("PutNoDupData(key=%x, value=%x) done. error=%v", key, value, err)

	err1 := c.mdbxCursor.PutNoDupData(key, value)
	err2 := c.rocksdbCursor.PutNoDupData(key, value)
	return assertError(c.CombineCursorDupSort.logger, err1, err2, "PutNoDupData")
}

func (c *CombineRwCursorDupSort) DeleteCurrentDuplicates() (err error) {
	c.CombineCursorDupSort.logger.Info("DeleteCurrentDuplicates()")
	defer c.CombineCursorDupSort.logger.Infof("DeleteCurrentDuplicates() done. error=%v", err)

	err1 := c.mdbxCursor.DeleteCurrentDuplicates()
	err2 := c.rocksdbCursor.DeleteCurrentDuplicates()
	return assertError(c.CombineCursorDupSort.logger, err1, err2, "DeleteCurrentDuplicates")
}

func (c *CombineRwCursorDupSort) DeleteExact(k, v []byte) (err error) {
	c.CombineCursorDupSort.logger.Infof("DeleteExact(key=%x, value=%x)", k, v)
	defer c.CombineCursorDupSort.logger.Infof("DeleteExact(key=%x, value=%x) done. error=%v", k, v, err)

	err1 := c.mdbxCursor.DeleteExact(k, v)
	err2 := c.rocksdbCursor.DeleteExact(k, v)
	return assertError(c.CombineCursorDupSort.logger, err1, err2, "DeleteExact")
}

func (c *CombineRwCursorDupSort) AppendDup(key, value []byte) (err error) {
	c.CombineCursorDupSort.logger.Infof("AppendDup(key=%x, value=%x)", key, value)
	defer c.CombineCursorDupSort.logger.Infof("AppendDup(key=%x, value=%x) done. error=%v", key, value, err)

	err1 := c.mdbxCursor.AppendDup(key, value)
	err2 := c.rocksdbCursor.AppendDup(key, value)
	return assertError(c.CombineCursorDupSort.logger, err1, err2, "AppendDup")
}

func (c *CombineRwCursorDupSort) First() ([]byte, []byte, error) {
	return c.CombineCursorDupSort.First()
}

func (c *CombineRwCursorDupSort) Seek(seek []byte) ([]byte, []byte, error) {
	return c.CombineCursorDupSort.Seek(seek)
}

func (c *CombineRwCursorDupSort) SeekExact(key []byte) ([]byte, []byte, error) {
	return c.CombineCursorDupSort.SeekExact(key)
}

func (c *CombineRwCursorDupSort) Next() ([]byte, []byte, error) {
	return c.CombineCursorDupSort.Next()
}

func (c *CombineRwCursorDupSort) Prev() ([]byte, []byte, error) {
	return c.CombineCursorDupSort.Prev()
}

func (c *CombineRwCursorDupSort) Last() ([]byte, []byte, error) {
	return c.CombineCursorDupSort.Last()
}

func (c *CombineRwCursorDupSort) Current() ([]byte, []byte, error) {
	return c.CombineCursorDupSort.Current()
}

func (c *CombineRwCursorDupSort) Count() (uint64, error) {
	return c.CombineCursorDupSort.Count()
}

func (c *CombineRwCursorDupSort) Close() {
	c.CombineCursorDupSort.Close()
}
