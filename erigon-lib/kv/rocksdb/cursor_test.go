package rocksdb

import (
	"os"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/rocksdb/rdb/common"
	"github.com/linxGnu/grocksdb"
	"github.com/stretchr/testify/require"
)

func TestRocksDbCursor_First(t *testing.T) {
	_, mtx, mc := mdbxBaseCase(t)
	_, rtx, rc := rocksdbBaseCase(t)

	k, v, e := mc.First()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)

	k, v, e = rc.First()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)

	// make sure First is valid after new value is inserted
	e = mc.Put([]byte("aaa"), []byte("vaaa"))
	require.NoError(t, e)
	k, v, e = mc.First()
	require.NoError(t, e)
	require.Equal(t, []byte("aaa"), k)
	require.Equal(t, []byte("vaaa"), v)

	e = rc.Put([]byte("aaa"), []byte("rvaaa"))
	require.NoError(t, e)
	k, v, e = rc.First()
	require.NoError(t, e)
	require.Equal(t, []byte("aaa"), k)
	require.Equal(t, []byte("rvaaa"), v)

	// check first not found
	mc2, err := mtx.RwCursor(mdbxTestTable2)
	require.NoError(t, err)
	defer mc2.Close()
	rc2, err := rtx.RwCursor(rocksdbTestTable2)
	require.NoError(t, err)
	defer rc2.Close()
	k, v, err = mc2.First()
	require.NoError(t, err)
	require.Nil(t, k)
	require.Nil(t, v)
	k, v, err = rc2.First()
	require.NoError(t, err)
	require.Nil(t, k)
	require.Nil(t, v)
}

func TestRocksDbCursor_Seek(t *testing.T) {
	_, mtx, _ := mdbxBaseCase(t)
	_, rtx, _ := rocksdbBaseCase(t)
	mc, err := mtx.RwCursor(mdbxTestTable)
	defer mc.Close()
	require.NoError(t, err)
	rc, err := rtx.RwCursor(rocksdbTestTable)
	require.NoError(t, err)
	defer rc.Close()

	// seek to exist key
	k, v, e := mc.Seek([]byte("key3"))
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)
	k, v, e = rc.Seek([]byte("key3"))
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)

	// seek to not exist key but has key after it
	k, v, e = mc.Seek([]byte("key2"))
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)
	k, v, e = rc.Seek([]byte("key2"))
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)
	k, v, e = mc.Next()
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.3"), v)
	k, v, e = rc.Next()
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.3"), v)

	// seek to not exist key
	k, v, e = mc.Seek([]byte("key3-notesist"))
	require.NoError(t, e)
	require.Nil(t, k)
	require.Nil(t, v)
	k, v, e = rc.Seek([]byte("key3-notesist"))
	require.NoError(t, e)
	require.Nil(t, k)
	require.Nil(t, v)
}

func TestRocksDbCursor_SeekExact(t *testing.T) {
	t.Run("AutoDupSortKeysConversion=false", func(t *testing.T) {
		_, _, mc := mdbxBaseCase(t)
		_, _, rc := rocksdbBaseCase(t)

		// seek to exist key
		k, v, e := mc.SeekExact([]byte("key3"))
		require.NoError(t, e)
		require.Equal(t, []byte("key3"), k)
		require.Equal(t, []byte("value3.1"), v)

		k, v, e = rc.SeekExact([]byte("key3"))
		require.NoError(t, e)
		require.Equal(t, []byte("key3"), k)
		require.Equal(t, []byte("value3.1"), v)

		// seek to not exist key but has key after it
		k, v, e = mc.SeekExact([]byte("key2"))
		require.NoError(t, e)
		require.Nil(t, k)
		require.Nil(t, v)
		k, v, e = rc.SeekExact([]byte("key2"))
		require.NoError(t, e)
		require.Nil(t, k)
		require.Nil(t, v)

		k, v, e = mc.SeekExact([]byte("key"))
		require.NoError(t, e)
		require.Nil(t, k)
		require.Nil(t, v)
		k, v, e = rc.SeekExact([]byte("key"))
		require.NoError(t, e)
		require.Nil(t, k)
		require.Nil(t, v)
	})
	t.Run("AutoDupSortKeysConversion=true", func(t *testing.T) {
		_, mtx, _ := mdbxBaseCase(t)
		rdb, rtx, _ := rocksdbBaseCase(t)
		mc, err := mtx.RwCursor(mdbxAutoDupSortKeysConversionTable)
		require.NoError(t, err)
		defer mc.Close()
		rc, err := rtx.RwCursor(rocksdbAutoDupSortKeysConversionTable)
		require.NoError(t, err)
		defer rc.Close()

		require.Equal(t, 8, rdb.AllTables()[rocksdbAutoDupSortKeysConversionTable].DupFromLen)
		require.Equal(t, 5, rdb.AllTables()[rocksdbAutoDupSortKeysConversionTable].DupToLen)

		require.NoError(t, mc.Put([]byte("key11111"), []byte("value1")))
		require.NoError(t, rc.Put([]byte("key11111"), []byte("value1")))
		require.NoError(t, mc.Put([]byte("key11112"), []byte("value2")))
		require.NoError(t, rc.Put([]byte("key11112"), []byte("value2")))

		k, v, err := mc.SeekExact([]byte("key11111"))
		require.NoError(t, err)
		require.Equal(t, []byte("key11"), k)
		require.Equal(t, []byte("value1"), v)
		k, v, err = rc.SeekExact([]byte("key11111"))
		require.NoError(t, err)
		require.Equal(t, []byte("key11"), k)
		require.Equal(t, []byte("value1"), v)

		k, v, err = mc.SeekExact([]byte("key11112"))
		require.NoError(t, err)
		require.Equal(t, []byte("key11"), k)
		require.Equal(t, []byte("value2"), v)
		k, v, err = rc.SeekExact([]byte("key11112"))
		require.NoError(t, err)
		require.Equal(t, []byte("key11"), k)
		require.Equal(t, []byte("value2"), v)
	})
}

func TestRocksDbCursor_NextPrev(t *testing.T) {
	_, mtx, _ := mdbxBaseCase(t)
	_, rtx, _ := rocksdbBaseCase(t)
	mc, err := mtx.RwCursor(mdbxTestTable)
	defer mc.Close()
	require.NoError(t, err)
	rc, err := rtx.RwCursor(rocksdbTestTable)
	require.NoError(t, err)
	defer rc.Close()

	k, v, e := mc.Next()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)
	k, v, e = rc.Next()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)

	k, v, e = mc.Next()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.3"), v)
	k, v, e = rc.Next()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.3"), v)

	k, v, e = mc.Next()
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)
	k, v, e = rc.Next()
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)

	k, v, e = mc.Next()
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.3"), v)
	k, v, e = rc.Next()
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.3"), v)

	// check Next failed won't change current
	k, v, err = mc.Current()
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.3"), v)
	k, v, err = rc.Current()
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.3"), v)
	k, v, e = mc.Next()
	require.NoError(t, e)
	require.Nil(t, k)
	require.Nil(t, v)
	k, v, e = rc.Next()
	require.NoError(t, e)
	require.Nil(t, k)
	require.Nil(t, v)
	k, v, err = mc.Current()
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.3"), v)
	k, v, err = rc.Current()
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.3"), v)

	// check Prev
	k, v, e = mc.Prev()
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)
	k, v, e = rc.Prev()
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)

	k, v, e = mc.Prev()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.3"), v)
	k, v, e = rc.Prev()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.3"), v)

	k, v, e = mc.Prev()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)
	k, v, e = rc.Prev()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)

	// check Prev failed won't change current
	k, v, err = mc.Current()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)
	k, v, err = rc.Current()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)
	k, v, e = mc.Prev()
	require.NoError(t, e)
	require.Nil(t, k)
	require.Nil(t, v)
	k, v, e = rc.Prev()
	require.NoError(t, e)
	require.Nil(t, k)
	require.Nil(t, v)
	k, v, err = mc.Current()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)
	k, v, err = rc.Current()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)
}

func TestRocksDbCursor_Last(t *testing.T) {
	_, _, mc := mdbxBaseCase(t)
	_, _, rc := rocksdbBaseCase(t)

	k, v, e := mc.Last()
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.3"), v)
	k, v, e = rc.Last()
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.3"), v)

	// make sure Last is valid after new value is inserted
	e = mc.Put([]byte("xxx"), []byte("valuex"))
	require.NoError(t, e)
	e = rc.Put([]byte("xxx"), []byte("valuex"))
	require.NoError(t, e)
	e = mc.Put([]byte("aaa"), []byte("valuea"))
	require.NoError(t, e)
	e = rc.Put([]byte("aaa"), []byte("valuea"))
	require.NoError(t, e)
	k, v, e = mc.Last()
	require.NoError(t, e)
	require.Equal(t, []byte("xxx"), k)
	require.Equal(t, []byte("valuex"), v)
	k, v, e = rc.Last()
	require.NoError(t, e)
	require.Equal(t, []byte("xxx"), k)
	require.Equal(t, []byte("valuex"), v)
}

func TestRocksDbCursor_FirstThenIterate(t *testing.T) {
	_, _, mc := mdbxBaseCase(t)
	_, _, rc := rocksdbBaseCase(t)

	k, v, e := mc.Next()
	require.NoError(t, e)
	require.Nil(t, k)
	require.Nil(t, v)
	k, v, e = rc.Next()
	require.NoError(t, e)
	require.Nil(t, k)
	require.Nil(t, v)

	k, v, e = mc.First()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)
	k, v, e = rc.First()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)

	k, v, e = mc.Next()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.3"), v)
	k, v, e = rc.Next()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.3"), v)

	k, v, e = mc.Next()
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)
	k, v, e = rc.Next()
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)

	k, v, e = mc.Next()
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.3"), v)
	k, v, e = rc.Next()
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.3"), v)

	k, v, e = mc.Next()
	require.NoError(t, e)
	require.Nil(t, k)
	require.Nil(t, v)
	k, v, e = rc.Next()
	require.NoError(t, e)
	require.Nil(t, k)
	require.Nil(t, v)
}

func TestRocksDbCursor_LastThenIterate(t *testing.T) {
	_, _, mc := mdbxBaseCase(t)
	_, _, rc := rocksdbBaseCase(t)

	k, v, e := mc.Last()
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.3"), v)
	k, v, e = rc.Last()
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.3"), v)

	k, v, e = mc.Prev()
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)
	k, v, e = rc.Prev()
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)

	k, v, e = mc.Prev()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.3"), v)
	k, v, e = rc.Prev()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.3"), v)

	k, v, e = mc.Prev()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)
	k, v, e = rc.Prev()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)

	k, v, e = mc.Prev()
	require.NoError(t, e)
	require.Nil(t, k)
	require.Nil(t, v)
	k, v, e = rc.Prev()
	require.NoError(t, e)
	require.Nil(t, k)
	require.Nil(t, v)
}

func TestRocksDbCursor_Current(t *testing.T) {
	_, _, mc := mdbxBaseCase(t)
	_, _, rc := rocksdbBaseCase(t)

	k, v, e := mc.Current()
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.3"), v)
	k, v, e = rc.Current()
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.3"), v)

	_, _, e = mc.First()
	require.NoError(t, e)
	_, _, e = rc.First()
	require.NoError(t, e)
	k, v, e = mc.Next()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.3"), v)
	k, v, e = rc.Next()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.3"), v)
	k, v, e = mc.Current()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.3"), v)
	k, v, e = rc.Current()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.3"), v)
}

func TestRocksDbCursor_Count(t *testing.T) {
	_, _, mc := mdbxBaseCase(t)
	_, _, rc := rocksdbBaseCase(t)

	count, err := mc.Count()
	require.NoError(t, err)
	require.Equal(t, uint64(4), count)
	count, err = rc.Count()
	require.NoError(t, err)
	require.Equal(t, uint64(4), count)
}

func TestRocksDbCursor_PutThenNext(t *testing.T) {
	_, _, mc := mdbxBaseCase(t)
	_, _, rc := rocksdbBaseCase(t)

	// check Next is nil after Put the latest key
	k, v, e := mc.Next()
	require.NoError(t, e)
	require.Nil(t, k)
	require.Nil(t, v)
	k, v, e = rc.Next()
	require.NoError(t, e)
	require.Nil(t, k)
	require.Nil(t, v)

	// check that Put relocate the iterator
	require.NoError(t, mc.Put([]byte("key0"), []byte("value0.0")))
	require.NoError(t, mc.Put([]byte("key0"), []byte("value0.1")))
	require.NoError(t, rc.Put([]byte("key0"), []byte("value0.0")))
	require.NoError(t, rc.Put([]byte("key0"), []byte("value0.1")))
	k, v, e = mc.Next()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)
	k, v, e = rc.Next()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)
	k, v, e = mc.Next()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.3"), v)
	k, v, e = rc.Next()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.3"), v)
	k, v, e = mc.Next()
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)
	k, v, e = rc.Next()
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)
	k, v, e = mc.Next()
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.3"), v)
	k, v, e = rc.Next()
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.3"), v)
}

func TestRocksDbCursor_PutThenPrev(t *testing.T) {
	_, _, mc := mdbxBaseCase(t)
	_, _, rc := rocksdbBaseCase(t)

	k, v, e := mc.Prev()
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)
	k, v, e = rc.Prev()
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)

	k, v, e = mc.Prev()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.3"), v)
	k, v, e = rc.Prev()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.3"), v)

	k, v, e = mc.Prev()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)
	k, v, e = rc.Prev()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)

	k, v, e = mc.Prev()
	require.NoError(t, e)
	require.Nil(t, k)
	require.Nil(t, v)
	k, v, e = rc.Prev()
	require.NoError(t, e)
	require.Nil(t, k)
	require.Nil(t, v)
}

func TestRocksDbCursor_IterateNewValue(t *testing.T) {
	_, _, mc := mdbxBaseCase(t)
	_, _, rc := rocksdbBaseCase(t)

	k, v, e := mc.Prev()
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)
	k, v, e = rc.Prev()
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)
	k, v, e = mc.Prev()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.3"), v)
	k, v, e = rc.Prev()
	require.NoError(t, e)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.3"), v)

	// put a new value
	require.NoError(t, mc.Put([]byte("key2"), []byte("value2")))
	require.NoError(t, rc.Put([]byte("key2"), []byte("value2")))

	// iterate the rest value
	k, v, e = mc.Next()
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)
	k, v, e = rc.Next()
	require.NoError(t, e)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)
	k, v, e = mc.Prev()
	require.NoError(t, e)
	require.Equal(t, []byte("key2"), k)
	require.Equal(t, []byte("value2"), v)
	k, v, e = rc.Prev()
	require.NoError(t, e)
	require.Equal(t, []byte("key2"), k)
	require.Equal(t, []byte("value2"), v)
}

func TestRocksDbCursor_DifferentTable(t *testing.T) {
	_, mtx, _ := mdbxBaseCase(t)
	_, rtx, _ := rocksdbBaseCase(t)
	mc2, err := mtx.RwCursor(mdbxTestTable2)
	require.NoError(t, err)
	defer mc2.Close()
	rc2, err := rtx.RwCursor(rocksdbTestTable2)
	require.NoError(t, err)
	defer rc2.Close()

	k, v, err := mc2.First()
	require.NoError(t, err)
	require.Nil(t, k)
	require.Nil(t, v)
	k, v, err = rc2.First()
	require.NoError(t, err)
	require.Nil(t, k)
	require.Nil(t, v)

	require.NoError(t, mc2.Put([]byte("key10"), []byte("value10")))
	require.NoError(t, rc2.Put([]byte("key10"), []byte("value10")))
	require.NoError(t, mc2.Put([]byte("key20"), []byte("value20")))
	require.NoError(t, rc2.Put([]byte("key20"), []byte("value20")))

	// check Prev in table2
	k, v, err = mc2.Prev()
	require.NoError(t, err)
	require.Equal(t, []byte("key10"), k)
	require.Equal(t, []byte("value10"), v)
	k, v, err = rc2.Prev()
	require.NoError(t, err)
	require.Equal(t, []byte("key10"), k)
	require.Equal(t, []byte("value10"), v)
	k, v, err = mc2.Prev()
	require.NoError(t, err)
	require.Nil(t, k)
	require.Nil(t, v)
	k, v, err = rc2.Prev()
	require.NoError(t, err)
	require.Nil(t, k)
	require.Nil(t, v)

	mc, err := mtx.RwCursor(mdbxTestTable)
	require.NoError(t, err)
	defer mc.Close()
	rc, err := rtx.RwCursor(rocksdbTestTable)
	require.NoError(t, err)
	defer rc.Close()
	k, v, err = mc.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)
	k, v, err = rc.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)
	k, v, err = mc.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.3"), v)
	k, v, err = rc.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.3"), v)
	k, v, err = mc.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)
	k, v, err = rc.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)
	k, v, err = mc.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.3"), v)
	k, v, err = rc.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.3"), v)
	k, v, err = mc.Next()
	require.NoError(t, err)
	require.Nil(t, k)
	require.Nil(t, v)
	k, v, err = rc.Next()
	require.NoError(t, err)
	require.Nil(t, k)
	require.Nil(t, v)
}

// see behaviour of mdbx in TestMdbxCursor_putNoOverwrite
func TestRocksDbCursor_putNoOverwrite(t *testing.T) {
	_, tx, ci := rocksdbBaseCase(t)

	// check empty table
	csi, err := tx.RwCursor(kv.Sequence)
	require.NoError(t, err)
	cs := csi.(*RocksDbCursor)
	err = cs.putNoOverwrite([]byte("key0"), []byte("value0"))
	require.NoError(t, err)
	k, v, err := cs.First()
	require.NoError(t, err)
	require.Equal(t, []byte("key0"), k)
	require.Equal(t, []byte("value0"), v)

	c := ci.(*RocksDbDupSortCursor)

	// make sure chrrent is key3
	k, v, err = c.Current()
	require.NoError(t, err)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.3"), v)

	// key exist, value exist: return error
	err = c.putNoOverwrite([]byte("key1"), []byte("value1.x"))
	require.EqualError(t, err, common.ErrKeyExist.Error())
	// even putNoOverwrite, but it change current
	k, v, err = c.Current()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)

	// key exist, value not exist: return error
	err = c.putNoOverwrite([]byte("key1"), []byte("value1.1xxx"))
	require.EqualError(t, err, common.ErrKeyExist.Error())

	// key not exist, value not exist, return success
	err = c.putNoOverwrite([]byte("key2"), []byte("value2.1"))
	require.NoError(t, err)

	// key not exist, value exist, return success
	err = c.putNoOverwrite([]byte("key2.1"), []byte("value2.1"))
	require.NoError(t, err)
}

// see behaviour of mdbx in TestMdbxCursor_putCurrent
func TestRocksDBCursor_putCurrent(t *testing.T) {
	_, tx, ci := rocksdbBaseCase(t)

	// check empty table
	csi, err := tx.RwCursor(kv.Sequence)
	require.NoError(t, err)
	cs := csi.(*RocksDbCursor)
	require.EqualError(t, cs.putCurrent([]byte("key0"), []byte("value0")), common.ErrInvalidIter.Error())

	c := ci.(*RocksDbDupSortCursor)

	k, v, err := c.First()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)

	require.EqualError(t, c.putCurrent([]byte("new1"), []byte("newvalue1")), common.ErrKeyMismatch.Error())

	require.NoError(t, c.putCurrent([]byte("key1"), []byte("newvalue1")))
	k, v, err = c.Current()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("newvalue1"), v)

	k, v, err = c.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.3"), v)
}

// see behaviour of mdbx in TestMdbxCursor_getBothRange
func TestRocksDBCursor_getBothRange(t *testing.T) {
	_, _, ci := rocksdbBaseCase(t)
	c := ci.(*RocksDbDupSortCursor)

	v, err := c.getBothRange([]byte("x"), []byte("value1.1"))
	require.Error(t, err)
	k, v, err := c.Current()
	require.Error(t, err)

	v, err = c.getBothRange([]byte("key"), []byte("value1.1"))
	require.Error(t, err)
	k, v, err = c.Current()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)

	v, err = c.getBothRange([]byte("key1"), []byte("v"))
	require.NoError(t, err)
	require.Equal(t, []byte("value1.1"), v)
	k, v, err = c.Current()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)

	v, err = c.getBothRange([]byte("key1"), []byte("u"))
	require.NoError(t, err)
	require.Equal(t, []byte("value1.1"), v)

	v, err = c.getBothRange([]byte("key1"), []byte("value1.11"))
	require.NoError(t, err)
	require.Equal(t, []byte("value1.3"), v)
	k, v, err = c.Current()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.3"), v)

	v, err = c.getBothRange([]byte("key1"), []byte("x"))
	require.EqualError(t, err, common.ErrNotFound.Error())
	k, v, err = c.Current()
	require.Error(t, err)
}

// see behaviour of mdbx in TestMdbxCursor_put
func TestRocksDBCursor_put(t *testing.T) {
	t.Run("DupSort", func(t *testing.T) {
		_, tx, ci := rocksdbBaseCase(t)

		// check empty table
		ci2, err := tx.RwCursor(rocksdbTestTable2)
		require.NoError(t, err)
		c2 := ci2.(*RocksDbDupSortCursor)
		require.NoError(t, c2.put([]byte("key0"), []byte("value0")))
		k, v, err := c2.Current()
		require.NoError(t, err)
		require.Equal(t, []byte("key0"), k)
		require.Equal(t, []byte("value0"), v)

		c := ci.(*RocksDbDupSortCursor)

		require.NoError(t, c.put([]byte("key1"), []byte("value0.0")))
		require.NoError(t, c.put([]byte("key1"), []byte("value0.1")))

		k, v, err = c.Next()
		require.NoError(t, err)
		require.Equal(t, []byte("key1"), k)
		require.Equal(t, []byte("value1.1"), v)

		k, v, err = c.First()
		require.NoError(t, err)
		require.Equal(t, []byte("key1"), k)
		require.Equal(t, []byte("value0.0"), v)
		k, v, err = c.Next()
		require.NoError(t, err)
		require.Equal(t, []byte("key1"), k)
		require.Equal(t, []byte("value0.1"), v)
		k, v, err = c.Next()
		require.NoError(t, err)
		require.Equal(t, []byte("key1"), k)
		require.Equal(t, []byte("value1.1"), v)
		k, v, err = c.Next()
		require.NoError(t, err)
		require.Equal(t, []byte("key1"), k)
		require.Equal(t, []byte("value1.3"), v)
	})
	t.Run("NotDupSort", func(t *testing.T) {
		_, tx, _ := rocksdbBaseCase(t)
		ci, err := tx.RwCursor(rocksdbNotDupSortTestTable)
		require.NoError(t, err)
		c := ci.(*RocksDbCursor)
		defer c.Close()

		require.NoError(t, c.put([]byte("key1"), []byte("value1.3")))
		require.NoError(t, c.put([]byte("key1"), []byte("value1.1")))
		require.NoError(t, c.put([]byte("key1"), []byte("value1.2")))

		k, v, err := c.Current()
		require.NoError(t, err)
		require.Equal(t, []byte("key1"), k)
		require.Equal(t, []byte("value1.2"), v)

		k, v, err = c.First()
		require.NoError(t, err)
		require.Equal(t, []byte("key1"), k)
		require.Equal(t, []byte("value1.2"), v)
		k, v, err = c.Next()
		require.NoError(t, err)
		require.Nil(t, k)
		require.Nil(t, v)

	})
}

// see behaviour of mdbx in TestMdbxCursor_setRange
func TestRocksDBCursor_setRange(t *testing.T) {
	_, tx, ci := rocksdbBaseCase(t)

	// check empty table
	csi, err := tx.RwCursor(kv.Sequence)
	require.NoError(t, err)
	cs := csi.(*RocksDbCursor)
	k, v, err := cs.setRange([]byte("key1"))
	require.EqualError(t, err, common.ErrNotFound.Error())

	c := ci.(*RocksDbDupSortCursor)

	k, v, err = c.setRange([]byte("key"))
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)

	k, v, err = c.setRange([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)

	k, v, err = c.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.3"), v)

	k, v, err = c.setRange([]byte("key11"))
	require.NoError(t, err)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)
}

// see behaviour of mdbx in TestMdbxCursor_set
func TestRocksDBCursor_set(t *testing.T) {
	_, tx, ci := rocksdbBaseCase(t)

	csi, err := tx.RwCursor(kv.Sequence)
	require.NoError(t, err)
	cs := csi.(*RocksDbCursor)
	k, v, err := cs.set([]byte("key1"))
	require.EqualError(t, err, common.ErrNotFound.Error())

	c := ci.(*RocksDbDupSortCursor)

	k, v, err = c.set([]byte("key"))
	require.Error(t, err)

	k, v, err = c.set([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)

	k, v, err = c.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.3"), v)

	k, v, err = c.set([]byte("key11"))
	require.Error(t, err)
}

// see behaviour of mdbx in TestMdbxCursor_putAppendDup
func TestRocksDBCursor_putAppendDup(t *testing.T) {
	_, tx, ci := rocksdbBaseCase(t)

	// check empty table
	csi, err := tx.RwCursor(kv.Sequence)
	require.NoError(t, err)
	cs := csi.(*RocksDbCursor)
	require.NoError(t, cs.putAppendDup([]byte("key0"), []byte("value0")))
	k, v, err := cs.Current()
	require.NoError(t, err)
	require.Equal(t, []byte("key0"), k)
	require.Equal(t, []byte("value0"), v)

	c := ci.(*RocksDbDupSortCursor)
	k, v, err = c.Current()
	require.NoError(t, err)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.3"), v)

	require.EqualError(t, c.putAppendDup([]byte("key3"), []byte("append3.1")), common.ErrValueLeLatest.Error())
	require.NoError(t, c.putAppendDup([]byte("key3"), []byte("xppend3.1")))

	require.EqualError(t, c.putAppendDup([]byte("key1"), []byte("value1.1")), common.ErrValueLeLatest.Error())
	require.EqualError(t, c.putAppendDup([]byte("key1"), []byte("value1.3")), common.ErrValueLeLatest.Error())
	require.EqualError(t, c.putAppendDup([]byte("key1"), []byte("append1.2")), common.ErrValueLeLatest.Error())
	require.NoError(t, c.putAppendDup([]byte("key1"), []byte("value1.4")))

	require.NoError(t, c.putAppendDup([]byte("key2"), []byte("append2.1")))

	k, v, err = c.First()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)
	k, v, err = c.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.3"), v)
	k, v, err = c.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.4"), v)
	k, v, err = c.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("key2"), k)
	require.Equal(t, []byte("append2.1"), v)
	k, v, err = c.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)
	k, v, err = c.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.3"), v)
	k, v, err = c.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("xppend3.1"), v)
	k, v, err = c.Next()
	require.NoError(t, err)
	require.Nil(t, k)
	require.Nil(t, v)
}

// see behaviour of mdbx in TestMdbxCursor_putAppend
func TestRocksDBCursor_putAppend(t *testing.T) {
	_, tx, ci := rocksdbBaseCase(t)
	c := ci.(*RocksDbDupSortCursor)

	// check empty table
	csi, err := tx.RwCursor(kv.Sequence)
	require.NoError(t, err)
	cs := csi.(*RocksDbCursor)
	err = cs.putAppend([]byte("key0"), []byte("value0.1"))
	require.NoError(t, err)
	k, v, err := cs.Current()
	require.NoError(t, err)
	require.Equal(t, []byte("key0"), k)
	require.Equal(t, []byte("value0.1"), v)

	err = c.putAppend([]byte("key1"), []byte("value1.1"))
	require.EqualError(t, err, common.ErrKeyMismatch.Error())

	k, v, err = c.Current()
	require.NoError(t, err)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.3"), v)

	err = c.putAppend([]byte("key3"), []byte("value3.4"))
	require.EqualError(t, err, common.ErrKeyMismatch.Error())
	err = c.putAppend([]byte("key4"), []byte("value4.4"))
	require.NoError(t, err)
	err = c.putAppend([]byte("key4"), []byte("value4.5"))
	require.EqualError(t, err, common.ErrKeyMismatch.Error())

	_, _, err = c.Seek([]byte("key1"))
	require.NoError(t, err)
	err = c.putAppend([]byte("key2"), []byte("value2.1"))
	require.EqualError(t, err, common.ErrKeyMismatch.Error())

	_, _, err = c.Last()
	require.NoError(t, err)
	err = c.putAppend([]byte("key5"), []byte("value5.1"))
	require.NoError(t, err)

	k, v, err = c.First()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)
	k, v, err = c.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.3"), v)
	k, v, err = c.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)
	k, v, err = c.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.3"), v)
	k, v, err = c.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("key4"), k)
	require.Equal(t, []byte("value4.4"), v)
	k, v, err = c.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("key5"), k)
	require.Equal(t, []byte("value5.1"), v)
}

// see behaviour of mdbx in TestMdbxCursor_delAllDupData
func TestRocksDBCursor_delAllDupData(t *testing.T) {
	_, tx, ci := rocksdbBaseCase(t)

	// check empty table
	csi, err := tx.RwCursor(kv.Sequence)
	require.NoError(t, err)
	cs := csi.(*RocksDbCursor)
	require.EqualError(t, cs.delAllDupData(), common.ErrInvalidIter.Error())

	c := ci.(*RocksDbDupSortCursor)

	require.NoError(t, c.delAllDupData())

	k, v, err := c.Current()
	require.EqualError(t, err, common.ErrInvalidIter.Error())

	k, v, err = c.First()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)
	k, v, err = c.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.3"), v)
	k, v, err = c.Next()
	require.NoError(t, err)
	require.Nil(t, k)
	require.Nil(t, v)

	require.NoError(t, c.Put([]byte("key2"), []byte("value2.1")))
	require.NoError(t, c.Put([]byte("key2"), []byte("value2.2")))
	k, v, err = c.SeekExact([]byte("key1"))
	require.NoError(t, err)
	require.NoError(t, c.delAllDupData())
	k, v, err = c.Current()
	require.NoError(t, err)
	require.Equal(t, []byte("key2"), k)
	require.Equal(t, []byte("value2.1"), v)
	k, v, err = c.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("key2"), k)
	require.Equal(t, []byte("value2.2"), v)

	require.NoError(t, c.Put([]byte("key5"), []byte("value5.1")))
	require.NoError(t, c.delAllDupData())

	k, v, err = c.First()
	require.NoError(t, err)
	require.Equal(t, []byte("key2"), k)
	require.Equal(t, []byte("value2.1"), v)
	k, v, err = c.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("key2"), k)
	require.Equal(t, []byte("value2.2"), v)
	k, v, err = c.Next()
	require.NoError(t, err)
	require.Nil(t, k)
	require.Nil(t, v)
}

// see behaviour of mdbx in TestMdbxCursor_delCurrentWithPrev
func TestRocksDBCursor_delCurrentWithPrev(t *testing.T) {
	_, _, ci := rocksdbBaseCase(t)

	k, v, err := ci.Last()
	require.NoError(t, err)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.3"), v)

	require.NoError(t, ci.DeleteCurrent())
	k, v, err = ci.Prev()
	require.NoError(t, err)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)

	require.NoError(t, ci.DeleteCurrent())
	k, v, err = ci.Prev()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.3"), v)

	require.NoError(t, ci.DeleteCurrent())
	k, v, err = ci.Prev()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)

	require.NoError(t, ci.DeleteCurrent())
	k, v, err = ci.Prev()
	require.NoError(t, err)
	require.Nil(t, k)
	require.Nil(t, v)
}

// see behaviour of mdbx in TestMdbxCursor_delCurrent
func TestRocksDBCursor_delCurrent(t *testing.T) {
	_, tx, ci := rocksdbBaseCase(t)

	// check empty table
	csi, err := tx.RwCursor(kv.Sequence)
	require.NoError(t, err)
	cs := csi.(*RocksDbCursor)
	require.EqualError(t, cs.delCurrent(), common.ErrInvalidIter.Error())

	c := ci.(*RocksDbDupSortCursor)

	// delete the last one(key3/value3.3), so current is invalid
	require.NoError(t, c.delCurrent())
	k, v, err := c.Current()
	require.EqualError(t, err, common.ErrInvalidIter.Error())

	// make sure the last one is deleted success
	k, v, err = c.First()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)
	k, v, err = c.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.3"), v)
	k, v, err = c.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)
	k, v, err = c.Next()
	require.NoError(t, err)
	require.Nil(t, k)
	require.Nil(t, v)

	// seek to the first one
	k, v, err = c.SeekExact([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)

	// after delCurrent:
	// 1. if call Current() first then call Next():
	//    current will be the value next to be deleted, Next will the value next to current
	// 2. if call Next() first then call Current():
	//    Next() will return the value next to be deleted, Current will be the value same to Next()
	require.NoError(t, c.delCurrent())
	k, v, err = c.Current()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.3"), v)
	k, v, err = c.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)

	k, v, err = c.SeekExact([]byte("key1"))
	require.NoError(t, c.delCurrent())
	k, v, err = c.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)
	k, v, err = c.Current()
	require.NoError(t, err)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)

	// now only key3/value3.1 valid
	require.NoError(t, c.Put([]byte("key2"), []byte("value2.1")))
	require.NoError(t, c.delCurrent())
	k, v, err = c.Current()
	require.NoError(t, err)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)

	_, _, err = c.First()
	require.NoError(t, err)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)
	k, v, err = c.Next()
	require.NoError(t, err)
	require.Nil(t, k)
	require.Nil(t, v)

	_, _, err = c.First()
	require.NoError(t, c.delCurrent())
	k, v, err = c.Current()
	require.Error(t, err)
}

// see behaviour of mdbx in TestMdbxCursor_lastDup
func TestRocksDBCursor_lastDup(t *testing.T) {
	_, tx, ci := rocksdbBaseCase(t)

	// check empty table
	csi, err := tx.RwCursor(kv.Sequence)
	require.NoError(t, err)
	cs := csi.(*RocksDbCursor)
	_, err = cs.lastDup()
	require.EqualError(t, err, common.ErrInvalidIter.Error())

	c := ci.(*RocksDbDupSortCursor)

	require.NoError(t, c.Put([]byte("key5"), []byte("value5.1")))
	v, err := c.lastDup()
	require.NoError(t, err)
	require.Equal(t, []byte("value5.1"), v)

	_, _, err = c.SeekExact([]byte("key1"))
	require.NoError(t, err)
	v, err = c.lastDup()
	require.NoError(t, err)
	require.Equal(t, []byte("value1.3"), v)

	k, v, err := c.Current()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.3"), v)

	// check situation when current is invalid
	require.NoError(t, c.Delete([]byte("key5")))
	_, _, err = c.Current()
	require.Error(t, err)
	_, err = c.lastDup()
	require.Error(t, err)
}

// see behaviour of mdbx in TestMdbxCursor_firstDup
func TestRocksDBCursor_firstDup(t *testing.T) {
	_, tx, ci := rocksdbBaseCase(t)

	// check empty table
	csi, err := tx.RwCursor(kv.Sequence)
	require.NoError(t, err)
	cs := csi.(*RocksDbCursor)
	_, err = cs.firstDup()
	require.EqualError(t, err, common.ErrInvalidIter.Error())

	c := ci.(*RocksDbDupSortCursor)

	require.NoError(t, c.Put([]byte("key5"), []byte("value5.1")))
	v, err := c.firstDup()
	require.NoError(t, err)
	require.Equal(t, []byte("value5.1"), v)

	_, _, err = c.SeekExact([]byte("key1"))
	require.NoError(t, err)
	v, err = c.firstDup()
	require.NoError(t, err)
	require.Equal(t, []byte("value1.1"), v)

	k, v, err := c.Current()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)
}

// see behaviour of mdbx in TestMdbxCursor_nextDup
func TestRocksDbCursor_nextDup(t *testing.T) {
	_, tx, ci := rocksdbBaseCase(t)

	// check empty table
	csi, err := tx.RwCursor(kv.Sequence)
	require.NoError(t, err)
	cs := csi.(*RocksDbCursor)
	_, _, err = cs.nextDup()
	require.EqualError(t, err, common.ErrNotFound.Error())

	c := ci.(*RocksDbDupSortCursor)

	require.NoError(t, c.Put([]byte("key5"), []byte("value5.1")))
	_, _, err = c.nextDup()
	require.Error(t, err)

	_, _, err = c.SeekExact([]byte("key1"))
	require.NoError(t, err)
	k, v, err := c.nextDup()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.3"), v)
	k, v, err = c.nextDup()
	require.Error(t, err)
}

// see behaviour of mdbx in TestMdbxCursor_prevDup
func TestRocksDbCursor_prevDup(t *testing.T) {
	_, tx, ci := rocksdbBaseCase(t)

	// check empty table
	csi, err := tx.RwCursor(kv.Sequence)
	require.NoError(t, err)
	cs := csi.(*RocksDbCursor)
	_, _, err = cs.prevDup()
	require.EqualError(t, err, common.ErrNotFound.Error())

	c := ci.(*RocksDbDupSortCursor)

	require.NoError(t, c.Put([]byte("key5"), []byte("value5.1")))
	_, _, err = c.prevDup()
	require.Error(t, err)

	_, _, err = c.SeekExact([]byte("key1"))
	require.NoError(t, err)
	_, _, err = c.Next()
	require.NoError(t, err)
	k, v, err := c.Current()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.3"), v)

	k, v, err = c.prevDup()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)
	k, v, err = c.prevDup()
	require.Error(t, err)
	k, v, err = c.Current()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)
}

// see behaviour of mdbx in TestMdbxCursor_getBoth
func TestRocksDbCursor_getBoth(t *testing.T) {
	_, tx, ci := rocksdbBaseCase(t)

	// check empty table
	csi, err := tx.RwCursor(kv.Sequence)
	require.NoError(t, err)
	cs := csi.(*RocksDbCursor)
	_, err = cs.getBoth([]byte("key0"), []byte("value0"))
	require.Error(t, err)

	c := ci.(*RocksDbDupSortCursor)

	v, err := c.getBoth([]byte("key"), []byte("value1.1"))
	require.Error(t, err)
	k, v, err := c.Current()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)

	_, _, err = c.Seek([]byte("key3"))
	require.NoError(t, err)

	v, err = c.getBoth([]byte("key3"), []byte("v"))
	require.EqualError(t, err, common.ErrNotFound.Error())
	k, v, err = c.Current()
	require.NoError(t, err)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)

	v, err = c.getBoth([]byte("key1"), []byte("u"))
	require.EqualError(t, err, common.ErrNotFound.Error())
	k, v, err = c.Current()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)

	v, err = c.getBoth([]byte("key1"), []byte("value1.11"))
	require.EqualError(t, err, common.ErrNotFound.Error())
	k, v, err = c.Current()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.3"), v)

	v, err = c.getBoth([]byte("key1"), []byte("value1.1"))
	require.NoError(t, err)
	require.Equal(t, []byte("value1.1"), v)
	k, v, err = c.Current()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)

	v, err = c.getBoth([]byte("key3"), []byte("value3.3"))
	require.NoError(t, err)
	require.Equal(t, []byte("value3.3"), v)
	k, v, err = c.Current()
	require.NoError(t, err)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.3"), v)
}

// see behaviour of mdbx in TestMdbxCursor_nextNoDup
func TestRocksDbCursor_nextNoDup(t *testing.T) {
	_, tx, ci := rocksdbBaseCase(t)

	// check empty table
	csi, err := tx.RwCursor(kv.Sequence)
	require.NoError(t, err)
	cs := csi.(*RocksDbCursor)
	_, _, err = cs.nextNoDup()
	require.EqualError(t, err, common.ErrNotFound.Error())

	c := ci.(*RocksDbDupSortCursor)

	k, v, err := c.Current()
	require.NoError(t, err)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.3"), v)
	_, _, err = c.nextNoDup()
	require.EqualError(t, err, common.ErrNotFound.Error())

	k, v, err = c.First()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.1"), v)
	k, v, err = c.nextNoDup()
	require.NoError(t, err)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)
	_, _, err = c.nextNoDup()
	require.EqualError(t, err, common.ErrNotFound.Error())

	_, _, err = c.First()
	require.NoError(t, err)
	k, v, err = c.Next()
	require.Equal(t, []byte("key1"), k)
	require.Equal(t, []byte("value1.3"), v)
	k, v, err = c.nextNoDup()
	require.NoError(t, err)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)
	_, _, err = c.nextNoDup()
	require.EqualError(t, err, common.ErrNotFound.Error())
}

func TestRocksDbCursor_Put(t *testing.T) {
	t.Run("AutoDupSortKeysConversion=true&&(KeyLen!=DupFromLen&&KeyLen>=DupToLen)", func(t *testing.T) {
		_, mtx, _ := mdbxBaseCase(t)
		rdb, rtx, _ := rocksdbBaseCase(t)
		mc, err := mtx.RwCursor(mdbxAutoDupSortKeysConversionTable)
		require.NoError(t, err)
		defer mc.Close()
		rc, err := rtx.RwCursor(rocksdbAutoDupSortKeysConversionTable)
		require.NoError(t, err)
		defer rc.Close()

		require.Equal(t, 8, rdb.AllTables()[rocksdbAutoDupSortKeysConversionTable].DupFromLen)
		require.Equal(t, 5, rdb.AllTables()[rocksdbAutoDupSortKeysConversionTable].DupToLen)

		invalidKey := []byte("key111")
		merr := mc.Put(invalidKey, []byte("value"))
		rerr := mc.Put(invalidKey, []byte("value"))
		require.Equal(t, merr, rerr)
	})
	t.Run("AutoDupSortKeysConversion=true&&(KeyLen!=DupFromLen&&KeyLen<DupToLen)&&KeyNotExist", func(t *testing.T) {
		_, mtx, _ := mdbxBaseCase(t)
		rdb, rtx, _ := rocksdbBaseCase(t)
		mc, err := mtx.RwCursor(mdbxAutoDupSortKeysConversionTable)
		require.NoError(t, err)
		defer mc.Close()
		rc, err := rtx.RwCursor(rocksdbAutoDupSortKeysConversionTable)
		require.NoError(t, err)
		defer rc.Close()

		require.Equal(t, 8, rdb.AllTables()[rocksdbAutoDupSortKeysConversionTable].DupFromLen)
		require.Equal(t, 5, rdb.AllTables()[rocksdbAutoDupSortKeysConversionTable].DupToLen)

		key := []byte("aa")
		err = mc.Put(key, []byte("value"))
		require.NoError(t, err)
		err = rc.Put(key, []byte("value"))
		require.NoError(t, err)

		v, err := mtx.GetOne(mdbxAutoDupSortKeysConversionTable, key)
		require.NoError(t, err)
		require.Equal(t, []byte("value"), v)
		v, err = rtx.GetOne(rocksdbAutoDupSortKeysConversionTable, key)
		require.NoError(t, err)
		require.Equal(t, []byte("value"), v)
	})
	t.Run("AutoDupSortKeysConversion=true&&(KeyLen!=DupFromLen&&KeyLen<DupToLen)&&KeyExist", func(t *testing.T) {
		_, mtx, _ := mdbxBaseCase(t)
		rdb, rtx, _ := rocksdbBaseCase(t)
		mc, err := mtx.RwCursor(mdbxAutoDupSortKeysConversionTable)
		require.NoError(t, err)
		defer mc.Close()
		rc, err := rtx.RwCursor(rocksdbAutoDupSortKeysConversionTable)
		require.NoError(t, err)
		defer rc.Close()

		require.Equal(t, 8, rdb.AllTables()[rocksdbAutoDupSortKeysConversionTable].DupFromLen)
		require.Equal(t, 5, rdb.AllTables()[rocksdbAutoDupSortKeysConversionTable].DupToLen)

		// check key exist and current is the key
		key := []byte("key1")
		err = mc.Put(key, []byte("value1.1"))
		require.NoError(t, err)
		err = rc.Put(key, []byte("value1.1"))
		require.NoError(t, err)
		// the 2nd time write. now the key has exist, and current is the key
		err = mc.Put(key, []byte("value1.2"))
		require.NoError(t, err)
		err = rc.Put(key, []byte("value1.2"))
		require.NoError(t, err)

		// check key exist but current is not the key
		key2 := []byte("key2")
		err = mc.Put(key2, []byte("value2.1"))
		require.NoError(t, err)
		err = rc.Put(key2, []byte("value2.1"))
		require.NoError(t, err)
		k, _, err := mc.SeekExact(key)
		require.NoError(t, err)
		require.Equal(t, key, k)
		k, _, err = rc.SeekExact(key)
		require.NoError(t, err)
		require.Equal(t, key, k)
		// the 2nd time write. now the key has exist, but current is not the key
		err = mc.Put(key2, []byte("value2.2"))
		require.NoError(t, err)
		err = rc.Put(key2, []byte("value2.2"))
		require.NoError(t, err)

		v, err := mtx.GetOne(mdbxAutoDupSortKeysConversionTable, key)
		require.NoError(t, err)
		require.Equal(t, []byte("value1.2"), v)
		v, err = rtx.GetOne(rocksdbAutoDupSortKeysConversionTable, key)
		require.NoError(t, err)
		require.Equal(t, []byte("value1.2"), v)
		v, err = mtx.GetOne(mdbxAutoDupSortKeysConversionTable, key2)
		require.NoError(t, err)
		require.Equal(t, []byte("value2.2"), v)
		v, err = rtx.GetOne(rocksdbAutoDupSortKeysConversionTable, key2)
		require.NoError(t, err)
		require.Equal(t, []byte("value2.2"), v)
	})
	t.Run("AutoDupSortKeysConversion=true&&KeyLen==DupFromLen&&KeyEqual", func(t *testing.T) {
		_, mtx, _ := mdbxBaseCase(t)
		rdb, rtx, _ := rocksdbBaseCase(t)
		mc, err := mtx.RwCursorDupSort(mdbxAutoDupSortKeysConversionTable)
		require.NoError(t, err)
		defer mc.Close()
		rc, err := rtx.RwCursorDupSort(rocksdbAutoDupSortKeysConversionTable)
		require.NoError(t, err)
		defer rc.Close()

		require.Equal(t, 8, rdb.AllTables()[rocksdbAutoDupSortKeysConversionTable].DupFromLen)
		require.Equal(t, 5, rdb.AllTables()[rocksdbAutoDupSortKeysConversionTable].DupToLen)

		key := []byte("key11111")
		require.NoError(t, mc.Put(key, []byte("value11111")))
		require.NoError(t, rc.Put(key, []byte("value11111")))
		k, v, err := mc.Current()
		require.NoError(t, err)
		require.Equal(t, key, k)
		require.Equal(t, []byte("value11111"), v)
		k, v, err = rc.Current()
		require.NoError(t, err)
		require.Equal(t, key, k)
		require.Equal(t, []byte("value11111"), v)

		require.NoError(t, mc.Put(key, []byte("a11111")))
		require.NoError(t, rc.Put(key, []byte("a11111")))
		k, v, err = mc.First()
		require.NoError(t, err)
		require.Equal(t, key, k)
		require.Equal(t, []byte("a11111"), v)
		k, v, err = rc.First()
		require.NoError(t, err)
		require.Equal(t, key, k)
		require.Equal(t, []byte("a11111"), v)

		k, v, err = mc.Next()
		require.NoError(t, err)
		require.Nil(t, k)
		require.Nil(t, v)
		k, v, err = rc.Next()
		require.NoError(t, err)
		require.Nil(t, k)
		require.Nil(t, v)
	})
	t.Run("AutoDupSortKeysConversion=true&&KeyLen==DupFromLen&&KeyNotEqual", func(t *testing.T) {
		_, mtx, _ := mdbxBaseCase(t)
		rdb, rtx, _ := rocksdbBaseCase(t)
		mc, err := mtx.RwCursorDupSort(mdbxAutoDupSortKeysConversionTable)
		require.NoError(t, err)
		defer mc.Close()
		rc, err := rtx.RwCursorDupSort(rocksdbAutoDupSortKeysConversionTable)
		require.NoError(t, err)
		defer rc.Close()

		require.Equal(t, 8, rdb.AllTables()[rocksdbAutoDupSortKeysConversionTable].DupFromLen)
		require.Equal(t, 5, rdb.AllTables()[rocksdbAutoDupSortKeysConversionTable].DupToLen)

		key1 := []byte("key11111")
		require.NoError(t, mc.Put(key1, []byte("value11111")))
		require.NoError(t, rc.Put(key1, []byte("value11111")))
		k, v, err := mc.Current()
		require.NoError(t, err)
		require.Equal(t, key1, k)
		require.Equal(t, []byte("value11111"), v)
		k, v, err = rc.Current()
		require.NoError(t, err)
		require.Equal(t, key1, k)
		require.Equal(t, []byte("value11111"), v)

		key2 := []byte("key22222")
		require.NoError(t, mc.Put(key2, []byte("a11111")))
		require.NoError(t, rc.Put(key2, []byte("a11111")))
		k, v, err = mc.First()
		require.NoError(t, err)
		require.Equal(t, key1, k)
		require.Equal(t, []byte("value11111"), v)
		k, v, err = rc.First()
		require.NoError(t, err)
		require.Equal(t, key1, k)
		require.Equal(t, []byte("value11111"), v)
		k, v, err = mc.Next()
		require.NoError(t, err)
		require.Equal(t, key2, k)
		require.Equal(t, []byte("a11111"), v)
		k, v, err = rc.Next()
		require.NoError(t, err)
		require.Equal(t, key2, k)
		require.Equal(t, []byte("a11111"), v)
	})
	t.Run("AutoDupSortKeysConversion=false", func(t *testing.T) {
		_, _, mc := mdbxBaseCase(t)
		_, _, rc := rocksdbBaseCase(t)

		require.NoError(t, mc.Put([]byte("key1"), []byte("aaa")))
		require.NoError(t, rc.Put([]byte("key1"), []byte("aaa")))

		k, v, err := mc.Current()
		require.NoError(t, err)
		require.Equal(t, []byte("key1"), k)
		require.Equal(t, []byte("aaa"), v)
		k, v, err = rc.Current()
		require.NoError(t, err)
		require.Equal(t, []byte("key1"), k)
		require.Equal(t, []byte("aaa"), v)

		k, v, err = mc.First()
		require.NoError(t, err)
		require.Equal(t, []byte("key1"), k)
		require.Equal(t, []byte("aaa"), v)
		k, v, err = rc.First()
		require.NoError(t, err)
		require.Equal(t, []byte("key1"), k)
		require.Equal(t, []byte("aaa"), v)

		k, v, err = mc.Next()
		require.NoError(t, err)
		require.Equal(t, []byte("key1"), k)
		require.Equal(t, []byte("value1.1"), v)
		k, v, err = rc.Next()
		require.NoError(t, err)
		require.Equal(t, []byte("key1"), k)
		require.Equal(t, []byte("value1.1"), v)

		k, v, err = mc.Next()
		require.NoError(t, err)
		require.Equal(t, []byte("key1"), k)
		require.Equal(t, []byte("value1.3"), v)
		k, v, err = rc.Next()
		require.NoError(t, err)
		require.Equal(t, []byte("key1"), k)
		require.Equal(t, []byte("value1.3"), v)

		k, v, err = mc.Next()
		require.NoError(t, err)
		require.Equal(t, []byte("key3"), k)
		require.Equal(t, []byte("value3.1"), v)
		k, v, err = rc.Next()
		require.NoError(t, err)
		require.Equal(t, []byte("key3"), k)
		require.Equal(t, []byte("value3.1"), v)

		k, v, err = mc.Next()
		require.NoError(t, err)
		require.Equal(t, []byte("key3"), k)
		require.Equal(t, []byte("value3.3"), v)
		k, v, err = rc.Next()
		require.NoError(t, err)
		require.Equal(t, []byte("key3"), k)
		require.Equal(t, []byte("value3.3"), v)
	})
}

func TestCurrentAfterDelete(t *testing.T) {
	_, _, mc := mdbxBaseCase(t)
	_, _, rc := rocksdbBaseCase(t)

	// make sure current is key3/value3.1
	_, _, err := mc.Seek([]byte("key3"))
	require.NoError(t, err)
	k, v, err := mc.Current()
	require.NoError(t, err)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)
	_, _, err = rc.Seek([]byte("key3"))
	require.NoError(t, err)
	k, v, err = rc.Current()
	require.NoError(t, err)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)

	require.NoError(t, mc.Put([]byte("key4"), []byte("value4.1")))
	require.NoError(t, rc.Put([]byte("key4"), []byte("value4.1")))

	require.NoError(t, mc.Delete([]byte("key1")))
	k, v, err = mc.Current()
	require.NoError(t, err)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)
	require.NoError(t, rc.Delete([]byte("key1")))
	k, v, err = rc.Current()
	require.NoError(t, err)
	require.Equal(t, []byte("key3"), k)
	require.Equal(t, []byte("value3.1"), v)

	require.NoError(t, mc.Delete([]byte("key3")))
	require.NoError(t, rc.Delete([]byte("key3")))

	k, v, err = mc.Current()
	require.NoError(t, err)
	require.Equal(t, []byte("key4"), k)
	require.Equal(t, []byte("value4.1"), v)
	k, v, err = rc.Current()
	require.NoError(t, err)
	require.Equal(t, []byte("key4"), k)
	require.Equal(t, []byte("value4.1"), v)

	require.NoError(t, mc.Delete([]byte("key4")))
	require.NoError(t, rc.Delete([]byte("key4")))
	k, v, err = mc.Current()
	require.Error(t, err)
	k, v, err = rc.Current()
	require.Error(t, err)
}

func testRDB(t *testing.T) {
	dbPath := t.TempDir()
	t.Cleanup(func() { os.RemoveAll(dbPath) })
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(grocksdb.NewLRUCache(3 << 30))

	opts := grocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)

	txopts := grocksdb.NewDefaultTransactionDBOptions()

	rdb, err := grocksdb.OpenTransactionDb(opts, txopts, dbPath)
	require.NoError(t, err)
	t.Cleanup(rdb.Close)

	tx := rdb.TransactionBegin(grocksdb.NewDefaultWriteOptions(), grocksdb.NewDefaultTransactionOptions(), nil)
	defer tx.Rollback()

	testTable1 := "abc"
	nextTestTable1, _ := kv.NextSubtree([]byte(testTable1))

	ropts := grocksdb.NewDefaultReadOptions()
	ropts.SetIterateLowerBound(common.MergeKey(testTable1, []byte{}))
	ropts.SetIterateUpperBound(common.MergeKey(string(nextTestTable1), []byte{}))
	// ropts.SetTailing(false)
	it := tx.NewIterator(ropts)

	// 如果 SeekToFirst 时没数据，那么无论怎么插入新数据，都是空 it
	// 如果 SeekToFirst 时有数据，那么新插入的数据，在后面可以被 Next 枚举到；在前面可以被 Prev 枚举到

	// it.SeekToFirst()
	// require.False(t, it.Valid())
	//k := it.Key()
	//defer k.Free()
	//v := it.Value()
	//defer v.Free()
	//require.Equal(t, []byte("key1"), k.Data())
	//require.Equal(t, []byte("value1"), v.Data())

	require.NoError(t, tx.Put(common.MergeKey(string(nextTestTable1), []byte("")), []byte("value next")))
	require.NoError(t, tx.Put([]byte("111"), []byte("value2")))

	require.NoError(t, tx.Put(common.MergeKey(testTable1, []byte("key2")), []byte("value2")))
	require.NoError(t, tx.Put(common.MergeKey(testTable1, []byte("key4")), []byte("value4")))

	it.SeekToFirst()
	require.True(t, it.Valid())
	require.NoError(t, it.Err())
	k := it.Key()
	defer k.Free()
	v := it.Value()
	defer v.Free()
	require.Equal(t, common.MergeKey(testTable1, []byte("key2")), k.Data())
	require.Equal(t, []byte("value2"), v.Data())
	it.Next()
	require.True(t, it.Valid())
	require.NoError(t, it.Err())
	k = it.Key()
	defer k.Free()
	v = it.Value()
	defer v.Free()
	require.Equal(t, common.MergeKey(testTable1, []byte("key4")), k.Data())
	require.Equal(t, []byte("value4"), v.Data())
	it.Next()
	require.False(t, it.Valid())

	it = tx.NewIterator(ropts)
	it.SeekForPrev(common.MergeKey(string(nextTestTable1), []byte{}))
	require.True(t, it.Valid())
	table, key := common.SplitKey(common.MoveSliceToBytes(it.Key()))
	if table != testTable1 {
		it.Prev()
		table, key = common.SplitKey(common.MoveSliceToBytes(it.Key()))
	}
	require.True(t, it.Valid())
	value := common.MoveSliceToBytes(it.Value())
	require.Equal(t, testTable1, table)
	require.Equal(t, []byte("key4"), key)
	require.Equal(t, []byte("value4"), value)

	it = tx.NewIterator(ropts)
	it.SeekToLast()
	require.True(t, it.Valid())
	table, key = common.SplitKey(common.MoveSliceToBytes(it.Key()))
	require.Equal(t, testTable1, table)
	require.Equal(t, []byte("key4"), key)
	require.Equal(t, []byte("value4"), it.Value().Data())
}
