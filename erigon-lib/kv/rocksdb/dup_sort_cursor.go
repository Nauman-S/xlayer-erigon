package rocksdb

import (
	"errors"
	"fmt"
	"github.com/ledgerwatch/erigon-lib/kv/rocksdb/rdb/common"
)

type RocksDbDupSortCursor struct {
	*RocksDbCursor
}

// DeleteExact - delete 1 value from given key
func (c *RocksDbDupSortCursor) DeleteExact(k1, k2 []byte) error {
	panic("yztodo: not implemented")
}

// SeekBothExact -
// second parameter can be nil only if searched key has no duplicates, or return error
func (c *RocksDbDupSortCursor) SeekBothExact(key, value []byte) ([]byte, []byte, error) {
	v, err := c.getBoth(key, value)
	if err != nil {
		if errors.Is(err, common.ErrNotFound) {
			return nil, nil, nil
		}
		return []byte{}, nil, fmt.Errorf("in SeekBothExact: %w", err)
	}
	return key, v, nil
}

// SeekBothRange - exact match of the key, but range match of the value
func (c *RocksDbDupSortCursor) SeekBothRange(key, value []byte) ([]byte, error) {
	v, err := c.getBothRange(key, value)
	if err != nil {
		if errors.Is(err, common.ErrNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("in SeekBothRange, table=%s: %w", c.table, err)
	}
	return v, nil
}

// FirstDup - position at first data item of current key
func (c *RocksDbDupSortCursor) FirstDup() ([]byte, error) {
	v, err := c.firstDup()
	if err != nil {
		if errors.Is(err, common.ErrNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("in FirstDup: %w", err)
	}
	return v, nil
}

// NextDup - position at next data item of current key
func (c *RocksDbDupSortCursor) NextDup() ([]byte, []byte, error) {
	k, v, err := c.nextDup()
	if err != nil {
		if errors.Is(err, common.ErrNotFound) {
			return nil, nil, nil
		}
		return []byte{}, nil, fmt.Errorf("in NextDup: %w", err)
	}
	return k, v, nil
}

// NextNoDup - position at first data item of next key
func (c *RocksDbDupSortCursor) NextNoDup() ([]byte, []byte, error) {
	k, v, err := c.nextNoDup()
	if err != nil {
		if errors.Is(err, common.ErrNotFound) {
			return nil, nil, nil
		}
		return []byte{}, nil, fmt.Errorf("in NextNoDup: %w", err)
	}
	return k, v, nil
}

func (c *RocksDbDupSortCursor) PrevDup() ([]byte, []byte, error) {
	k, v, err := c.prevDup()
	if err != nil {
		if errors.Is(err, common.ErrNotFound) {
			return nil, nil, nil
		}
		return []byte{}, nil, fmt.Errorf("in PrevDup: %w", err)
	}
	return k, v, nil
}

func (c *RocksDbDupSortCursor) PrevNoDup() ([]byte, []byte, error) {
	panic("yztodo: not implemented")
}

func (c *RocksDbDupSortCursor) LastDup() ([]byte, error) {
	v, err := c.lastDup()
	if err != nil {
		if errors.Is(err, common.ErrNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("in LastDup: %w", err)
	}
	return v, nil
}

// Append - append the given key/data pair to the end of the database. This option allows fast bulk loading when keys are already known to be in the correct order.
func (c *RocksDbDupSortCursor) Append(k []byte, v []byte) error {
	panic("yztodo: not implemented")
}

// AppendDup - same as Append, but for sorted dup data
func (c *RocksDbDupSortCursor) AppendDup(key, value []byte) error {
	if err := c.putAppendDup(key, value); err != nil {
		return fmt.Errorf("label: %s, in AppendDup: bucket=%s, %w", c.rtx.db.label, c.table, err)
	}
	return nil
}

// PutNoDupData - inserts key without dupsort
func (c *RocksDbDupSortCursor) PutNoDupData(key, value []byte) error {
	panic("yztodo: not implemented")
}

// DeleteCurrentDuplicates - deletes all of the data items for the current key
func (c *RocksDbDupSortCursor) DeleteCurrentDuplicates() error {
	panic("yztodo: not implemented")
}

// CountDuplicates - number of duplicates for the current key
func (c *RocksDbDupSortCursor) CountDuplicates() (uint64, error) {
	panic("yztodo: not implemented")
}
