package common

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type DBValueStamp struct {
	index int
	Value []byte
}

type DBValue struct {
	// values must be always sorted
	values [][]byte
}

func DBValueWithOneValue(v []byte) *DBValue {
	return &DBValue{values: [][]byte{v}}
}

func DeserializeDBValue(buf []byte) *DBValue {
	if buf == nil {
		return &DBValue{values: nil}
	}

	count := binary.LittleEndian.Uint32(buf)
	buf = buf[4:]

	lenSliceSize := count * 4
	lenSlice := buf[:lenSliceSize]
	buf = buf[lenSliceSize:]

	values := make([][]byte, count)
	valueOffset := 0
	for i := 0; i < int(count); i++ {
		l := binary.LittleEndian.Uint32(lenSlice[i*4:])
		values[i] = buf[valueOffset : valueOffset+int(l)]
		valueOffset += int(l)
	}

	return &DBValue{values: values}
}

func (dbv *DBValue) Serialize() []byte {
	// yztodo: make sure values is nil behave like mdbx
	if dbv.values == nil {
		return nil
	}

	bufLen := 4 + 4*len(dbv.values)
	for _, buf := range dbv.values {
		bufLen += len(buf)
	}

	buf := make([]byte, bufLen)
	pos := 0

	count := uint32(len(dbv.values))
	binary.LittleEndian.PutUint32(buf[pos:], count)
	pos += 4

	for _, v := range dbv.values {
		binary.LittleEndian.PutUint32(buf[pos:], uint32(len(v)))
		pos += 4
	}

	for _, v := range dbv.values {
		copy(buf[pos:], v)
		pos += len(v)
	}

	return buf
}

func (dbv *DBValue) First() []byte {
	return NewDBValueIterator(dbv).First()
}

func (dbv *DBValue) SortedInsert(v []byte) {
	idx, _ := dbv.sortedSeek(v)

	dbv.values = append(dbv.values, nil)
	copy(dbv.values[idx+1:], dbv.values[idx:]) // move back
	dbv.values[idx] = v                        // insert
}

func (dbv *DBValue) sortedSeek(v []byte) (int, bool) {
	return SortedSeek(dbv.values, v, func(v1 []byte, v2 []byte) bool {
		return bytes.Compare(v1, v2) >= 0
	})
}

func (dbv *DBValue) Replace(valueStamp DBValueStamp, newV []byte) {
	if valueStamp.index < 0 || valueStamp.index >= len(dbv.values) {
		panic("index out of range for DBValue.Replace")
	}
	if oldV := dbv.values[valueStamp.index]; !bytes.Equal(oldV, valueStamp.Value) {
		panic("value is different with value stamp when Replace")
	}
	dbv.values[valueStamp.index] = newV
}

func (dbv *DBValue) Delete(valueStamp DBValueStamp) {
	if valueStamp.index < 0 || valueStamp.index >= len(dbv.values) {
		panic("index out of range for DBValue.Delete")
	}
	if oldV := dbv.values[valueStamp.index]; !bytes.Equal(oldV, valueStamp.Value) {
		panic("value is different with value stamp when Delete")
	}
	dbv.values = append(dbv.values[:valueStamp.index], dbv.values[valueStamp.index+1:]...)
}

func (dbv *DBValue) IsEmpty() bool {
	return len(dbv.values) == 0
}

type DBValueIterator struct {
	dbv           *DBValue
	current       int
	nextIsCurrent bool
}

func NewDBValueIterator(dbv *DBValue) *DBValueIterator {
	return &DBValueIterator{dbv: dbv, current: -1}
}

func NewDBValueIteratorOverLast(dbv *DBValue) *DBValueIterator {
	return &DBValueIterator{dbv: dbv, current: len(dbv.values)}
}

func (it *DBValueIterator) SetNextIsCurrent() {
	it.nextIsCurrent = true
}

func (it *DBValueIterator) IsEmpty() bool {
	return it.dbv.IsEmpty()
}

func (it *DBValueIterator) First() []byte {
	if len(it.dbv.values) == 0 {
		return nil
	}

	return it.dbv.values[0]
}

func (it *DBValueIterator) Last() []byte {
	if len(it.dbv.values) == 0 {
		return nil
	}

	return it.dbv.values[len(it.dbv.values)-1]
}

func (it *DBValueIterator) Seek(seekV []byte) ([]byte, error) {
	it.nextIsCurrent = false

	if seekV == nil {
		// if seek value is nil, it sould seek to the first one
		it.SeekToFirst()
		return it.First(), nil
	}

	index, ok := it.dbv.sortedSeek(seekV)
	if !ok {
		it.SeekToLast()
		return nil, ErrNotFound
	}

	it.setCurrent(index)
	v, ok := it.Current()
	if !ok {
		panic("current must be exist")
	}
	return v, nil
}

func (it *DBValueIterator) SeekExact(seekV []byte) ([]byte, error) {
	it.nextIsCurrent = false

	v, err := it.Seek(seekV)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(v, seekV) {
		it.SeekToLast()
		return nil, ErrNotFound
	}

	return v, nil
}

// Next return false if it iterated past either the first or the last value
func (it *DBValueIterator) Next() ([]byte, bool) {
	if it.nextIsCurrent {
		it.nextIsCurrent = false
		return it.Current()
	}

	if it.current < -1 || it.current >= len(it.dbv.values)-1 {
		return nil, false
	}

	it.current++
	v := it.dbv.values[it.current]
	return v, true
}

func (it *DBValueIterator) Prev() ([]byte, bool) {
	it.nextIsCurrent = false

	if it.current <= 0 || it.current > len(it.dbv.values) {
		return nil, false
	}

	it.current--
	v := it.dbv.values[it.current]
	return v, true
}

func (it *DBValueIterator) Current() ([]byte, bool) {
	it.nextIsCurrent = false

	if it.current < 0 || it.current >= len(it.dbv.values) {
		return nil, false
	}

	return it.dbv.values[it.current], true
}

func (it *DBValueIterator) CurrentStamp() DBValueStamp {
	return DBValueStamp{
		index: it.current,
		Value: it.dbv.values[it.current],
	}
}

func (it *DBValueIterator) Count() uint64 {
	return uint64(len(it.dbv.values))
}

func (it *DBValueIterator) SeekToFirst() ([]byte, bool) {
	it.nextIsCurrent = false

	if len(it.dbv.values) == 0 {
		return nil, false
	}

	it.current = 0
	v := it.dbv.values[it.current]
	return v, true
}

func (it *DBValueIterator) SeekToLast() ([]byte, bool) {
	it.nextIsCurrent = false

	if len(it.dbv.values) == 0 {
		return nil, false
	}

	it.current = len(it.dbv.values)
	if it.current > 0 {
		it.current--
	}

	v := it.dbv.values[it.current]
	return v, true
}

func (it *DBValueIterator) SeekToOverLast() {
	it.nextIsCurrent = false

	it.current = len(it.dbv.values)
}

func (it *DBValueIterator) MustSeekToValue(value []byte) {
	it.nextIsCurrent = false

	_, err := it.SeekExact(value)
	if err != nil {
		panic(fmt.Sprintf("MustSeekToValue failed. value=%x. err=%v", value, err))
	}
}

func (it *DBValueIterator) DeleteCurrent() {
	if it.current < 0 || it.current >= len(it.dbv.values) {
		panic(fmt.Sprintf("DeleteCurrent: current %d is invalid. values length:%d", it.current, len(it.dbv.values)))
	}

	it.dbv.values = append(it.dbv.values[:it.current], it.dbv.values[it.current+1:]...)
	// after calling MdbxCursor.DeleteCurrent(), calling  Current() immediately will get the value next to the delelted one,
	// but if calling Next() imediatelly after calling MdbxCursor.DeleteCurrent() will get the same value as Current(),
	// so we set this flag.
	// todo: is there a better way except set `nextIsCurrent` flag or set deleted value to nil in dbv.values?
	it.nextIsCurrent = true
}

func (it *DBValueIterator) setCurrent(current int) {
	if current < 0 || current >= len(it.dbv.values) {
		panic(fmt.Sprintf("current index out of range: new current:%d. max length:%d", current, len(it.dbv.values)))
	}

	it.current = current
}
