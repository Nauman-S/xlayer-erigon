package rocksdb

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/erigontech/mdbx-go/mdbx"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/rocksdb/rdb/common"
)

type RocksDbCursor struct {
	table    string
	tableCfg kv.TableCfgItem

	rtx *RocksDbTx
	id  uint64

	it *RocksDbIterator
}

func newRocksDbCursorRW(table string, tableCfg kv.TableCfgItem, rtx *RocksDbTx, id uint64) (*RocksDbCursor, error) {
	return &RocksDbCursor{
		table:    table,
		tableCfg: tableCfg,

		rtx: rtx,
		it:  NewRocksDbIterator(rtx.tx, table),

		id: id,
	}, nil
}

func (c *RocksDbCursor) UpdateSnapshot() {
	c.it.UpdateSnapshot()
}

// impl Cursor interface

func (c *RocksDbCursor) First() ([]byte, []byte, error) { // First - position at first key/data item
	return c.Seek(nil)
}

func (c *RocksDbCursor) Seek(seek []byte) (k []byte, v []byte, err error) { // Seek - position at first key greater than or equal to specified key
	if c.tableCfg.AutoDupSortKeysConversion {
		return c.seekDupSort(seek)
	}

	if len(seek) == 0 {
		k, v, err = c.it.First()
	} else {
		k, v, err = c.setRange(seek)
	}
	if err != nil {
		if errors.Is(err, common.ErrNotFound) || errors.Is(err, common.ErrInvalidIter) {
			return nil, nil, nil
		}
		err = fmt.Errorf("failed rocksdb cursor.Seek(): %w, bucket: %s,  key: %x", err, c.table, seek)
		return []byte{}, nil, err
	}

	return k, v, nil
}

func (c *RocksDbCursor) SeekExact(key []byte) ([]byte, []byte, error) { // SeekExact - position at exact matching key if exists
	b := c.tableCfg
	if b.AutoDupSortKeysConversion && len(key) == b.DupFromLen {
		from, to := b.DupFromLen, b.DupToLen
		v, err := c.getBothRange(key[:to], key[to:])
		if err != nil {
			if errors.Is(err, common.ErrNotFound) {
				return nil, nil, nil
			}
			return []byte{}, nil, err
		}
		if !bytes.Equal(key[to:], v[:from-to]) {
			return nil, nil, nil
		}
		return key[:to], v[from-to:], nil
	}

	k, v, err := c.set(key)
	if err != nil {
		if errors.Is(err, common.ErrNotFound) {
			return nil, nil, nil
		}
		return []byte{}, nil, err
	}
	return k, v, nil
}

func (c *RocksDbCursor) Next() (k []byte, v []byte, err error) { // Next - position at next key/value (can iterate over DupSort key/values automatically)
	k, v, err = c.it.Next()
	if err != nil {
		if errors.Is(err, common.ErrInvalidIter) {
			return nil, nil, nil
		}
		return []byte{}, nil, fmt.Errorf("failed rocksdb cursor.Next(): %w", err)
	}

	b := c.tableCfg
	if b.AutoDupSortKeysConversion && len(k) == b.DupToLen {
		keyPart := b.DupFromLen - b.DupToLen
		if len(v) == 0 {
			return nil, nil, fmt.Errorf("key with empty value: k=%x, len(k)=%d, v=%x", k, len(k), v)
		}
		k = append(k, v[:keyPart]...)
		v = v[keyPart:]
	}

	return k, v, nil
}

func (c *RocksDbCursor) Prev() (k []byte, v []byte, err error) { // Prev - position at previous key
	k, v, err = c.it.Prev()
	if err != nil {
		if errors.Is(err, common.ErrInvalidIter) {
			return nil, nil, nil
		}
		return []byte{}, nil, fmt.Errorf("failed MdbxKV cursor.Prev(): %w", err)
	}

	b := c.tableCfg
	if b.AutoDupSortKeysConversion && len(k) == b.DupToLen {
		keyPart := b.DupFromLen - b.DupToLen
		k = append(k, v[:keyPart]...)
		v = v[keyPart:]
	}

	return k, v, nil

}
func (c *RocksDbCursor) Last() ([]byte, []byte, error) { // Last - position at last key and last possible value
	k, v, err := c.it.Last()
	if err != nil {
		if errors.Is(err, common.ErrInvalidIter) {
			return nil, nil, nil
		}
		err = fmt.Errorf("failed MdbxKV cursor.Last(): %w, bucket: %s", err, c.table)
		return []byte{}, nil, err
	}

	b := c.tableCfg
	if b.AutoDupSortKeysConversion && len(k) == b.DupToLen {
		keyPart := b.DupFromLen - b.DupToLen
		k = append(k, v[:keyPart]...)
		v = v[keyPart:]
	}

	return k, v, nil

}
func (c *RocksDbCursor) Current() ([]byte, []byte, error) { // Current - return key/data at current cursor position
	k, v, err := c.it.Current()
	if err != nil {
		return []byte{}, nil, err
	}

	b := c.tableCfg
	if b.AutoDupSortKeysConversion && len(k) == b.DupToLen {
		keyPart := b.DupFromLen - b.DupToLen
		k = append(k, v[:keyPart]...)
		v = v[keyPart:]
	}

	return k, v, nil
}

func (c *RocksDbCursor) Count() (uint64, error) { // Count - fast way to calculate amount of keys in bucket. It counts all keys even if Prefix was set.
	return c.it.Count()
}

func (c *RocksDbCursor) Close() {
	if c.it != nil {
		c.it.Close()
		delete(c.rtx.cursors, c.id)
		c.it = nil
	}
}

// Put - based on order
func (c *RocksDbCursor) Put(k, v []byte) error {
	b := c.tableCfg
	if b.AutoDupSortKeysConversion {
		if err := c.putDupSort(k, v); err != nil {
			return fmt.Errorf("table: %s, err: %w", c.table, err)
		}
		return nil
	}
	if err := c.put(k, v); err != nil {
		return fmt.Errorf("table: %s, err: %w", c.table, err)
	}
	return nil
}

// Append - append the given key/data pair to the end of the database. This option allows fast bulk loading when keys are already known to be in the correct order.
func (c *RocksDbCursor) Append(k []byte, v []byte) error {
	if c.tableCfg.AutoDupSortKeysConversion {
		b := c.tableCfg
		from, to := b.DupFromLen, b.DupToLen
		if len(k) != from && len(k) >= to {
			return fmt.Errorf("append dupsort bucket: %s, can have keys of len==%d and len<%d. key: %x,%d", c.table, from, to, k, len(k))
		}

		if len(k) == from {
			v = append(k[to:], v...)
			k = k[:to]
		}
	}

	if c.tableCfg.Flags&mdbx.DupSort != 0 {
		if err := c.putAppendDup(k, v); err != nil {
			return fmt.Errorf("bucket: %s, %w", c.table, err)
		}
		return nil
	}

	if err := c.putAppend(k, v); err != nil {
		return fmt.Errorf("bucket: %s, %w", c.table, err)
	}
	return nil
}

// Delete - short version of SeekExact+DeleteCurrent or SeekBothExact+DeleteCurrent
func (c *RocksDbCursor) Delete(k []byte) error {
	if c.tableCfg.AutoDupSortKeysConversion {
		return c.deleteDupSort(k)
	}

	_, _, err := c.set(k)
	if err != nil {
		if errors.Is(err, common.ErrNotFound) {
			return nil
		}
		return err
	}

	if c.tableCfg.Flags&mdbx.DupSort != 0 {
		return c.delAllDupData()
	}

	return c.delCurrent()
}

func (c *RocksDbCursor) getBothRange(searchKey, searchV []byte) ([]byte, error) {
	// mdbx:
	// _, v, err := c.c.Get(k, v, mdbx.GetBothRange)
	// return v, err

	_, v, err := c.it.SeekExactKeyWithGeValue(searchKey, searchV)
	return v, err
}

func (c *RocksDbCursor) getBoth(k, v []byte) ([]byte, error) {
	//_, v, err := c.c.Get(k, v, mdbx.GetBoth)
	//return v, err

	_, v, err := c.it.SeekExactKeyAndValue(k, v)
	return v, err
}

func (c *RocksDbCursor) delCurrent() error {
	// mdbx
	// return c.c.Del(mdbx.Current)
	curK, valueStamp, err := c.it.currentKeyAndValueStamp()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			c.it.mustSeekToKeyValue(curK, valueStamp.Value)
		}
	}()

	// must delete current of iterator first,
	// for if delete data in db, the iterator may can't iterate data as expected.
	c.it.deleteCurrent()

	dbv, err := c.rtx.get(c.table, curK)
	if err != nil {
		return err
	}
	dbv.Delete(valueStamp)

	if dbv.IsEmpty() {
		if err := c.rtx.delete(c.table, curK); err != nil {
			return err
		}
	} else {
		if err := c.rtx.putOverwrite(c.table, curK, dbv); err != nil {
			return err
		}
	}

	return nil
}

// DeleteCurrent This function deletes the key/data pair to which the cursor refers.
// This does not invalidate the cursor, so operations such as MDB_NEXT
// can still be used on it.
// Both MDB_NEXT and MDB_GET_CURRENT will return the same record after
// this operation.
func (c *RocksDbCursor) DeleteCurrent() error {
	return c.delCurrent()
}

func (c *RocksDbCursor) putDupSort(key []byte, value []byte) error {
	b := c.tableCfg
	from, to := b.DupFromLen, b.DupToLen
	if len(key) != from && len(key) >= to {
		return fmt.Errorf("table: %s, can have keys of len==%d and len<%d. key: %x,%d", c.table, from, to, key, len(key))
	}

	if len(key) != from {
		err := c.putNoOverwrite(key, value)
		if err != nil {
			if errors.Is(err, common.ErrKeyExist) {
				return c.putCurrent(key, value)
			}
			return fmt.Errorf("putNoOverwrite, table: %s, key: %x, val: %x, err: %w", c.table, key, value, err)
		}
		return nil
	}

	value = append(key[to:], value...)
	key = key[:to]
	v, err := c.getBothRange(key, value[:from-to])
	if err != nil { // if key not found, or found another one - then just insert
		if errors.Is(err, common.ErrNotFound) {
			return c.put(key, value)
		}
		return err
	}

	if bytes.Equal(v[:from-to], value[:from-to]) {
		if len(v) == len(value) { // in DupSort case mdbx.Current works only with values of same length
			return c.putCurrent(key, value)
		}
		err = c.delCurrent()
		if err != nil {
			return err
		}
	}

	return c.put(key, value)
}

func (c *RocksDbCursor) putCurrent(k, v []byte) error {
	// mdbx:
	// return c.c.Put(k, v, mdbx.Current)
	curK, valueStamp, err := c.it.currentKeyAndValueStamp()
	if err != nil {
		return err
	}
	if !bytes.Equal(curK, k) {
		return common.ErrKeyMismatch
	}

	dbv, err := c.rtx.get(c.table, curK)
	if err != nil {
		return err
	}
	dbv.Replace(valueStamp, v)

	err = c.rtx.putOverwrite(c.table, k, dbv)
	if err != nil {
		return err
	}

	c.it.mustSeekToKeyValue(k, v)
	return nil
}

func (c *RocksDbCursor) putNoOverwrite(k, v []byte) error {
	// mdbx:
	// return c.c.Put(k, v, mdbx.NoOverwrite)

	_, _, err := c.it.SeekExact(k)
	if err == nil {
		return common.ErrKeyExist
	}

	return c.put(k, v)
}

func (c *RocksDbCursor) put(k, v []byte) error {
	var err error
	if c.tableCfg.Flags&kv.DupSort != 0 {
		err = c.rtx.putSorted(c.table, k, v)
	} else {
		err = c.rtx.putOverwrite(c.table, k, common.DBValueWithOneValue(v))
	}
	if err != nil {
		return err
	}

	c.it.mustSeekToKeyValue(k, v)
	return nil
}

func (c *RocksDbCursor) seekDupSort(seek []byte) (k, v []byte, err error) {
	b := c.tableCfg
	from, to := b.DupFromLen, b.DupToLen
	if len(seek) == 0 {
		k, v, err = c.it.First()
		if err != nil {
			if errors.Is(err, common.ErrNotFound) {
				return nil, nil, nil
			}
			return []byte{}, nil, err
		}

		if len(k) == to {
			k2 := make([]byte, 0, len(k)+from-to)
			k2 = append(append(k2, k...), v[:from-to]...)
			v = v[from-to:]
			k = k2
		}
		return k, v, nil
	}

	var seek1, seek2 []byte
	if len(seek) > to {
		seek1, seek2 = seek[:to], seek[to:]
	} else {
		seek1 = seek
	}
	k, v, err = c.setRange(seek1)
	if err != nil {
		if errors.Is(err, common.ErrNotFound) {
			return nil, nil, nil
		}

		return []byte{}, nil, err
	}

	if seek2 != nil && bytes.Equal(seek1, k) {
		v, err = c.getBothRange(seek1, seek2)
		if err != nil && errors.Is(err, common.ErrNotFound) {
			k, v, err = c.it.Next()
			if err != nil {
				if errors.Is(err, common.ErrInvalidIter) {
					return nil, nil, nil
				}
				return []byte{}, nil, err
			}
		} else if err != nil {
			return []byte{}, nil, err
		}
	}
	if len(k) == to {
		k2 := make([]byte, 0, len(k)+from-to)
		k2 = append(append(k2, k...), v[:from-to]...)
		v = v[from-to:]
		k = k2
	}

	return k, v, nil
}

func (c *RocksDbCursor) setRange(k []byte) ([]byte, []byte, error) {
	// return c.c.Get(k, nil, mdbx.SetRange)
	return c.it.Seek(k)
}
func (c *RocksDbCursor) set(k []byte) ([]byte, []byte, error) {
	// return c.c.Get(k, nil, mdbx.Set)
	return c.it.SeekExact(k)
}

func (c *RocksDbCursor) putAppendDup(k, v []byte) (err error) {
	curK, curV, curErr := c.it.Current()
	defer func() {
		if err != nil {
			if curErr != nil {
				c.it.invalidCurrent()
			} else {
				c.it.mustSeekToKeyValue(curK, curV)
			}
		}
	}()

	_, _, err = c.it.SeekExact(k)
	if err != nil {
		if errors.Is(err, common.ErrNotFound) {
			return c.put(k, v)
		}
		return err
	}

	_, lastV, err := c.it.LastDup()
	if err != nil {
		return err
	}
	if bytes.Compare(lastV, v) >= 0 {
		return common.ErrValueLeLatest
	}

	return c.put(k, v)
}

func (c *RocksDbCursor) putAppend(k, v []byte) (err error) {
	curK, curV, curErr := c.it.Current()
	defer func() {
		if err != nil {
			if curErr != nil {
				c.it.invalidCurrent()
			} else {
				c.it.mustSeekToKeyValue(curK, curV)
			}
		}
	}()

	lastK, _, err := c.it.Last()
	if err != nil {
		if errors.Is(err, common.ErrInvalidIter) {
			// this is the first k/v in the db, insert it.
			return c.put(k, v)
		}
		return err
	}
	if bytes.Compare(k, lastK) <= 0 {
		return common.ErrKeyMismatch
	}

	return c.putNoOverwrite(k, v)
}

func (c *RocksDbCursor) deleteDupSort(key []byte) error {
	b := c.tableCfg
	from, to := b.DupFromLen, b.DupToLen
	if len(key) != from && len(key) >= to {
		return fmt.Errorf("delete from dupsort bucket: %s, can have keys of len==%d and len<%d. key: %x,%d", c.table, from, to, key, len(key))
	}

	if len(key) == from {
		v, err := c.getBothRange(key[:to], key[to:])
		if err != nil { // if key not found, or found another one - then nothing to delete
			if errors.Is(err, common.ErrNotFound) {
				return nil
			}
			return err
		}
		if !bytes.Equal(v[:from-to], key[to:]) {
			return nil
		}
		return c.delCurrent()
	}

	_, _, err := c.set(key)
	if err != nil {
		if errors.Is(err, common.ErrNotFound) {
			return nil
		}
		return err
	}

	return c.delCurrent()
}

func (c *RocksDbCursor) delAllDupData() (err error) {
	// return c.c.Del(mdbx.AllDups)
	k, v, err := c.it.Current()
	if err != nil {
		return err
	}
	if err := c.it.NextKey(); err != nil {
		if !errors.Is(err, common.ErrInvalidIter) {
			return err
		}
		c.it.invalidCurrent()
	}
	defer func() {
		if err != nil {
			c.it.mustSeekToKeyValue(k, v)
		}
	}()

	if err := c.rtx.delete(c.table, k); err != nil {
		return err
	}

	return nil
}

func (c *RocksDbCursor) lastDup() ([]byte, error) {
	// _, v, err := c.c.Get(nil, nil, mdbx.LastDup)
	// return v, err

	_, v, err := c.it.LastDup()
	return v, err
}

func (c *RocksDbCursor) firstDup() ([]byte, error) {
	//_, v, err := c.c.Get(nil, nil, mdbx.FirstDup)
	//return v, err

	_, v, err := c.it.FirstDup()
	return v, err
}

func (c *RocksDbCursor) nextDup() ([]byte, []byte, error) {
	// return c.c.Get(nil, nil, mdbx.NextDup)

	return c.it.NextDup()
}

func (c *RocksDbCursor) nextNoDup() ([]byte, []byte, error) {
	// return c.c.Get(nil, nil, mdbx.NextNoDup)
	if err := c.it.NextKey(); err != nil {
		if errors.Is(err, common.ErrInvalidIter) {
			return nil, nil, common.ErrNotFound
		}
		return nil, nil, err
	}

	k, v, err := c.it.Current()
	if err != nil {
		if errors.Is(err, common.ErrInvalidIter) {
			return nil, nil, common.ErrNotFound
		}
	}
	return k, v, nil
}

func (c *RocksDbCursor) prevDup() ([]byte, []byte, error) {
	// return c.c.Get(nil, nil, mdbx.PrevDup)
	return c.it.PrevDup()
}
