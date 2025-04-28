package rocksdb

import (
	"encoding/binary"
	"fmt"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHas(t *testing.T) {
	_, mtx, _ := mdbxBaseCase(t)
	_, rtx, _ := rocksdbBaseCase(t)

	b, err := mtx.Has(mdbxTestTable, []byte("key1"))
	require.NoError(t, err)
	require.True(t, b)
	b, err = rtx.Has(rocksdbTestTable, []byte("key1"))
	require.NoError(t, err)
	require.True(t, b)

	b, err = mtx.Has(mdbxTestTable, []byte("key2"))
	require.NoError(t, err)
	require.False(t, b)
	b, err = rtx.Has(rocksdbTestTable, []byte("key2"))
	require.NoError(t, err)
	require.False(t, b)

	b, err = mtx.Has(mdbxTestTable, []byte("key3"))
	require.NoError(t, err)
	require.True(t, b)
	b, err = rtx.Has(rocksdbTestTable, []byte("key3"))
	require.NoError(t, err)
	require.True(t, b)

	b, err = mtx.Has("notexistTable", []byte("key3"))
	require.Error(t, err)
	// yztodo: is it necessary to return error too?
	b, err = rtx.Has("notexistTable", []byte("key3"))
	require.NoError(t, err)
	require.False(t, b)
}

func TestGetOne(t *testing.T) {
	_, mtx, _ := mdbxBaseCase(t)
	_, rtx, _ := rocksdbBaseCase(t)

	v, err := mtx.GetOne(mdbxTestTable, []byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []byte("value1.1"), v)
	v, err = rtx.GetOne(rocksdbTestTable, []byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []byte("value1.1"), v)

	v, err = mtx.GetOne(mdbxTestTable, []byte("key3"))
	require.NoError(t, err)
	require.Equal(t, []byte("value3.1"), v)
	v, err = rtx.GetOne(rocksdbTestTable, []byte("key3"))
	require.NoError(t, err)
	require.Equal(t, []byte("value3.1"), v)

	v, err = mtx.GetOne(mdbxTestTable, []byte("key5"))
	require.NoError(t, err)
	require.Nil(t, v)
	v, err = rtx.GetOne(rocksdbTestTable, []byte("key5"))
	require.NoError(t, err)
	require.Nil(t, v)
	v, err = mtx.GetOne("notexistTable", []byte("key1"))
	require.Error(t, err)
	// yztodo: is it necessary to return error too?
	v, err = rtx.GetOne("notexistTable", []byte("key1"))
	require.NoError(t, err)
	require.Nil(t, v)
}

func TestForEach(t *testing.T) {
	type kvPair struct {
		k string
		v []byte
	}
	noPrefixChecker := func(t *testing.T, kvPairSlice []kvPair, msg string) {
		require.Equal(t, 5, len(kvPairSlice), msg)
		require.Equal(t, "key1", kvPairSlice[0].k, msg)
		require.Equal(t, []byte("value1.1"), kvPairSlice[0].v, msg)
		require.Equal(t, "key1", kvPairSlice[1].k, msg)
		require.Equal(t, []byte("value1.3"), kvPairSlice[1].v, msg)
		require.Equal(t, "key2", kvPairSlice[2].k, msg)
		require.Equal(t, []byte("value2"), kvPairSlice[2].v, msg)
		require.Equal(t, "key3", kvPairSlice[3].k, msg)
		require.Equal(t, []byte("value3.1"), kvPairSlice[3].v, msg)
		require.Equal(t, "key3", kvPairSlice[4].k, msg)
		require.Equal(t, []byte("value3.3"), kvPairSlice[4].v, msg)
	}

	t.Run("Prefix==nil", func(t *testing.T) {
		_, mtx, _ := mdbxBaseCase(t)
		_, rtx, _ := rocksdbBaseCase(t)

		require.NoError(t, mtx.Put(mdbxTestTable, []byte("key2"), []byte("value2")))
		require.NoError(t, rtx.Put(rocksdbTestTable, []byte("key2"), []byte("value2")))

		kvPairSlice := make([]kvPair, 0)
		require.NoError(t, mtx.ForEach(mdbxTestTable, nil, func(k, v []byte) error {
			kvPairSlice = append(kvPairSlice, kvPair{
				k: string(k),
				v: v,
			})
			return nil
		}))
		noPrefixChecker(t, kvPairSlice, "mdbx ForEach")

		kvPairSlice = make([]kvPair, 0)
		require.NoError(t, rtx.ForEach(rocksdbTestTable, nil, func(k, v []byte) error {
			kvPairSlice = append(kvPairSlice, kvPair{
				k: string(k),
				v: v,
			})
			return nil
		}))
		noPrefixChecker(t, kvPairSlice, "rocksdb ForEach")
	})
	t.Run("Prefix==[]byte{}", func(t *testing.T) {
		_, mtx, _ := mdbxBaseCase(t)
		_, rtx, _ := rocksdbBaseCase(t)

		require.NoError(t, mtx.Put(mdbxTestTable, []byte("key2"), []byte("value2")))
		require.NoError(t, rtx.Put(rocksdbTestTable, []byte("key2"), []byte("value2")))

		kvPairSlice := make([]kvPair, 0)
		require.NoError(t, mtx.ForEach(mdbxTestTable, []byte{}, func(k, v []byte) error {
			kvPairSlice = append(kvPairSlice, kvPair{
				k: string(k),
				v: v,
			})
			return nil
		}))
		noPrefixChecker(t, kvPairSlice, "mdbx ForEach")

		kvPairSlice = make([]kvPair, 0)
		require.NoError(t, rtx.ForEach(rocksdbTestTable, []byte{}, func(k, v []byte) error {
			kvPairSlice = append(kvPairSlice, kvPair{
				k: string(k),
				v: v,
			})
			return nil
		}))
		noPrefixChecker(t, kvPairSlice, "rocksdb ForEach")
	})
	t.Run("Prefix!=nil", func(t *testing.T) {
		_, mtx, _ := mdbxBaseCase(t)
		_, rtx, _ := rocksdbBaseCase(t)

		require.NoError(t, mtx.Put(mdbxTestTable, []byte("key2"), []byte("value2")))
		require.NoError(t, rtx.Put(rocksdbTestTable, []byte("key2"), []byte("value2")))
		require.NoError(t, mtx.Put(mdbxTestTable, []byte("oto2"), []byte("valueoto2.2")))
		require.NoError(t, rtx.Put(rocksdbTestTable, []byte("oto2"), []byte("valueoto2.2")))
		require.NoError(t, mtx.Put(mdbxTestTable, []byte("oto2"), []byte("valueoto2.1")))
		require.NoError(t, rtx.Put(rocksdbTestTable, []byte("oto2"), []byte("valueoto2.1")))
		require.NoError(t, mtx.Put(mdbxTestTable, []byte("oto3"), []byte("valueoto3")))
		require.NoError(t, rtx.Put(rocksdbTestTable, []byte("oto3"), []byte("valueoto3")))
		require.NoError(t, mtx.Put(mdbxTestTable, []byte("aaa"), []byte("valueaaa")))
		require.NoError(t, rtx.Put(rocksdbTestTable, []byte("aaa"), []byte("valueaaa")))

		kvPairSlice := make([]kvPair, 0)
		require.NoError(t, mtx.ForEach(mdbxTestTable, []byte("key"), func(k, v []byte) error {
			kvPairSlice = append(kvPairSlice, kvPair{
				k: string(k),
				v: v,
			})
			return nil
		}))
		require.Equal(t, 8, len(kvPairSlice), kvPairSlice)
		noPrefixChecker(t, kvPairSlice[:5], "mdbx ForEach with prefix 'key'")
		require.Equal(t, "oto2", kvPairSlice[5].k)
		require.Equal(t, []byte("valueoto2.1"), kvPairSlice[5].v)
		require.Equal(t, "oto2", kvPairSlice[6].k)
		require.Equal(t, []byte("valueoto2.2"), kvPairSlice[6].v)
		require.Equal(t, "oto3", kvPairSlice[7].k)
		require.Equal(t, []byte("valueoto3"), kvPairSlice[7].v)

		kvPairSlice = make([]kvPair, 0)
		require.NoError(t, rtx.ForEach(rocksdbTestTable, []byte("key"), func(k, v []byte) error {
			kvPairSlice = append(kvPairSlice, kvPair{
				k: string(k),
				v: v,
			})
			return nil
		}))
		require.Equal(t, 8, len(kvPairSlice))
		noPrefixChecker(t, kvPairSlice[:5], "rocksdb ForEach with prefix 'key'")
		require.Equal(t, "oto2", kvPairSlice[5].k)
		require.Equal(t, []byte("valueoto2.1"), kvPairSlice[5].v)
		require.Equal(t, "oto2", kvPairSlice[6].k)
		require.Equal(t, []byte("valueoto2.2"), kvPairSlice[6].v)
		require.Equal(t, "oto3", kvPairSlice[7].k)
		require.Equal(t, []byte("valueoto3"), kvPairSlice[7].v)

		kvPairSlice = make([]kvPair, 0)
		require.NoError(t, mtx.ForEach(mdbxTestTable, []byte("oto"), func(k, v []byte) error {
			kvPairSlice = append(kvPairSlice, kvPair{
				k: string(k),
				v: v,
			})
			return nil
		}))
		require.Equal(t, 3, len(kvPairSlice))
		require.Equal(t, "oto2", kvPairSlice[0].k)
		require.Equal(t, []byte("valueoto2.1"), kvPairSlice[0].v)
		require.Equal(t, "oto2", kvPairSlice[1].k)
		require.Equal(t, []byte("valueoto2.2"), kvPairSlice[1].v)
		require.Equal(t, "oto3", kvPairSlice[2].k)
		require.Equal(t, []byte("valueoto3"), kvPairSlice[2].v)

		kvPairSlice = make([]kvPair, 0)
		require.NoError(t, rtx.ForEach(rocksdbTestTable, []byte("oto"), func(k, v []byte) error {
			kvPairSlice = append(kvPairSlice, kvPair{
				k: string(k),
				v: v,
			})
			return nil
		}))
		require.Equal(t, 3, len(kvPairSlice))
		require.Equal(t, "oto2", kvPairSlice[0].k)
		require.Equal(t, []byte("valueoto2.1"), kvPairSlice[0].v)
		require.Equal(t, "oto2", kvPairSlice[1].k)
		require.Equal(t, []byte("valueoto2.2"), kvPairSlice[1].v)
		require.Equal(t, "oto3", kvPairSlice[2].k)
		require.Equal(t, []byte("valueoto3"), kvPairSlice[2].v)
	})
}

func TestForPrefix(t *testing.T) {
	type kvPair struct {
		k string
		v []byte
	}
	noPrefixChecker := func(t *testing.T, kvPairSlice []kvPair, msg string) {
		require.Equal(t, 5, len(kvPairSlice), msg)
		require.Equal(t, "key1", kvPairSlice[0].k, msg)
		require.Equal(t, []byte("value1.1"), kvPairSlice[0].v, msg)
		require.Equal(t, "key1", kvPairSlice[1].k, msg)
		require.Equal(t, []byte("value1.3"), kvPairSlice[1].v, msg)
		require.Equal(t, "key2", kvPairSlice[2].k, msg)
		require.Equal(t, []byte("value2"), kvPairSlice[2].v, msg)
		require.Equal(t, "key3", kvPairSlice[3].k, msg)
		require.Equal(t, []byte("value3.1"), kvPairSlice[3].v, msg)
		require.Equal(t, "key3", kvPairSlice[4].k, msg)
		require.Equal(t, []byte("value3.3"), kvPairSlice[4].v, msg)
	}

	t.Run("Prefix==nil", func(t *testing.T) {
		_, mtx, _ := mdbxBaseCase(t)
		_, rtx, _ := rocksdbBaseCase(t)

		require.NoError(t, mtx.Put(mdbxTestTable, []byte("key2"), []byte("value2")))
		require.NoError(t, rtx.Put(rocksdbTestTable, []byte("key2"), []byte("value2")))

		kvPairSlice := make([]kvPair, 0)
		require.NoError(t, mtx.ForPrefix(mdbxTestTable, nil, func(k, v []byte) error {
			kvPairSlice = append(kvPairSlice, kvPair{
				k: string(k),
				v: v,
			})
			return nil
		}))
		noPrefixChecker(t, kvPairSlice, "mdbx ForEach")

		kvPairSlice = make([]kvPair, 0)
		require.NoError(t, rtx.ForPrefix(rocksdbTestTable, nil, func(k, v []byte) error {
			kvPairSlice = append(kvPairSlice, kvPair{
				k: string(k),
				v: v,
			})
			return nil
		}))
		noPrefixChecker(t, kvPairSlice, "rocksdb ForEach")
	})
	t.Run("Prefix==[]byte{}", func(t *testing.T) {
		_, mtx, _ := mdbxBaseCase(t)
		_, rtx, _ := rocksdbBaseCase(t)

		require.NoError(t, mtx.Put(mdbxTestTable, []byte("key2"), []byte("value2")))
		require.NoError(t, rtx.Put(rocksdbTestTable, []byte("key2"), []byte("value2")))

		kvPairSlice := make([]kvPair, 0)
		require.NoError(t, mtx.ForEach(mdbxTestTable, []byte{}, func(k, v []byte) error {
			kvPairSlice = append(kvPairSlice, kvPair{
				k: string(k),
				v: v,
			})
			return nil
		}))
		noPrefixChecker(t, kvPairSlice, "mdbx ForEach")

		kvPairSlice = make([]kvPair, 0)
		require.NoError(t, rtx.ForEach(rocksdbTestTable, []byte{}, func(k, v []byte) error {
			kvPairSlice = append(kvPairSlice, kvPair{
				k: string(k),
				v: v,
			})
			return nil
		}))
		noPrefixChecker(t, kvPairSlice, "rocksdb ForEach")
	})
	t.Run("Prefix!=nil", func(t *testing.T) {
		_, mtx, _ := mdbxBaseCase(t)
		_, rtx, _ := rocksdbBaseCase(t)

		require.NoError(t, mtx.Put(mdbxTestTable, []byte("key2"), []byte("value2")))
		require.NoError(t, rtx.Put(rocksdbTestTable, []byte("key2"), []byte("value2")))
		require.NoError(t, mtx.Put(mdbxTestTable, []byte("oto2"), []byte("valueoto2.2")))
		require.NoError(t, rtx.Put(rocksdbTestTable, []byte("oto2"), []byte("valueoto2.2")))
		require.NoError(t, mtx.Put(mdbxTestTable, []byte("oto2"), []byte("valueoto2.1")))
		require.NoError(t, rtx.Put(rocksdbTestTable, []byte("oto2"), []byte("valueoto2.1")))
		require.NoError(t, mtx.Put(mdbxTestTable, []byte("oto3"), []byte("valueoto3")))
		require.NoError(t, rtx.Put(rocksdbTestTable, []byte("oto3"), []byte("valueoto3")))
		require.NoError(t, mtx.Put(mdbxTestTable, []byte("aaa"), []byte("valueaaa")))
		require.NoError(t, rtx.Put(rocksdbTestTable, []byte("aaa"), []byte("valueaaa")))

		kvPairSlice := make([]kvPair, 0)
		require.NoError(t, mtx.ForPrefix(mdbxTestTable, []byte("key"), func(k, v []byte) error {
			kvPairSlice = append(kvPairSlice, kvPair{
				k: string(k),
				v: v,
			})
			return nil
		}))
		noPrefixChecker(t, kvPairSlice, "mdbx ForEach with prefix 'key'")

		kvPairSlice = make([]kvPair, 0)
		require.NoError(t, rtx.ForPrefix(rocksdbTestTable, []byte("key"), func(k, v []byte) error {
			kvPairSlice = append(kvPairSlice, kvPair{
				k: string(k),
				v: v,
			})
			return nil
		}))
		noPrefixChecker(t, kvPairSlice, "rocksdb ForEach with prefix 'key'")

		kvPairSlice = make([]kvPair, 0)
		require.NoError(t, mtx.ForPrefix(mdbxTestTable, []byte("oto"), func(k, v []byte) error {
			kvPairSlice = append(kvPairSlice, kvPair{
				k: string(k),
				v: v,
			})
			return nil
		}))
		require.Equal(t, 3, len(kvPairSlice))
		require.Equal(t, "oto2", kvPairSlice[0].k)
		require.Equal(t, []byte("valueoto2.1"), kvPairSlice[0].v)
		require.Equal(t, "oto2", kvPairSlice[1].k)
		require.Equal(t, []byte("valueoto2.2"), kvPairSlice[1].v)
		require.Equal(t, "oto3", kvPairSlice[2].k)
		require.Equal(t, []byte("valueoto3"), kvPairSlice[2].v)

		kvPairSlice = make([]kvPair, 0)
		require.NoError(t, rtx.ForPrefix(rocksdbTestTable, []byte("oto"), func(k, v []byte) error {
			kvPairSlice = append(kvPairSlice, kvPair{
				k: string(k),
				v: v,
			})
			return nil
		}))
		require.Equal(t, 3, len(kvPairSlice))
		require.Equal(t, "oto2", kvPairSlice[0].k)
		require.Equal(t, []byte("valueoto2.1"), kvPairSlice[0].v)
		require.Equal(t, "oto2", kvPairSlice[1].k)
		require.Equal(t, []byte("valueoto2.2"), kvPairSlice[1].v)
		require.Equal(t, "oto3", kvPairSlice[2].k)
		require.Equal(t, []byte("valueoto3"), kvPairSlice[2].v)
	})
}

func TestForAmount(t *testing.T) {
	_, mtx, _ := mdbxBaseCase(t)
	_, rtx, _ := rocksdbBaseCase(t)

	type kvPair struct {
		k string
		v []byte
	}
	kvPairSlice := make([]kvPair, 0)
	require.NoError(t, mtx.ForAmount(mdbxTestTable, []byte("key"), 3, func(k, v []byte) error {
		kvPairSlice = append(kvPairSlice, kvPair{
			k: string(k),
			v: v,
		})
		return nil
	}))
	require.Equal(t, 3, len(kvPairSlice))
	require.Equal(t, "key1", kvPairSlice[0].k)
	require.Equal(t, []byte("value1.1"), kvPairSlice[0].v)
	require.Equal(t, "key1", kvPairSlice[1].k)
	require.Equal(t, []byte("value1.3"), kvPairSlice[1].v)
	require.Equal(t, "key3", kvPairSlice[2].k)
	require.Equal(t, []byte("value3.1"), kvPairSlice[2].v)

	kvPairSlice = make([]kvPair, 0)
	require.NoError(t, rtx.ForAmount(rocksdbTestTable, []byte("key"), 3, func(k, v []byte) error {
		kvPairSlice = append(kvPairSlice, kvPair{
			k: string(k),
			v: v,
		})
		return nil
	}))
	require.Equal(t, 3, len(kvPairSlice))
	require.Equal(t, "key1", kvPairSlice[0].k)
	require.Equal(t, []byte("value1.1"), kvPairSlice[0].v)
	require.Equal(t, "key1", kvPairSlice[1].k)
	require.Equal(t, []byte("value1.3"), kvPairSlice[1].v)
	require.Equal(t, "key3", kvPairSlice[2].k)
	require.Equal(t, []byte("value3.1"), kvPairSlice[2].v)
}

func TestForEachWithWrongTable(t *testing.T) {
	_, mtx, _ := mdbxBaseCase(t)
	_, rtx, _ := rocksdbBaseCase(t)

	require.Error(t, mtx.ForEach("notexistTable", nil, func(k, v []byte) error {
		t.Fatal("should not have been called")
		return nil
	}))
	// todo: should rtx.ForEach return error too?
	require.NoError(t, rtx.ForEach("notexistTable", nil, func(k, v []byte) error {
		t.Fatal("should not have been called")
		return nil
	}))
}

func TestPut(t *testing.T) {
	t.Run("DupSort", func(t *testing.T) {
		_, mtx, _ := mdbxBaseCase(t)
		_, rtx, _ := rocksdbBaseCase(t)

		require.NoError(t, mtx.Put(mdbxTestTable, []byte("aaa"), []byte("valueaaa")))
		require.NoError(t, rtx.Put(rocksdbTestTable, []byte("aaa"), []byte("valueaaa")))
		require.NoError(t, mtx.Put(mdbxTestTable, []byte("key1"), []byte("value1.2")))
		require.NoError(t, rtx.Put(rocksdbTestTable, []byte("key1"), []byte("value1.2")))

		c, err := mtx.Cursor(mdbxTestTable)
		require.NoError(t, err)
		k, v, err := c.First()
		require.NoError(t, err)
		require.Equal(t, []byte("aaa"), k)
		require.Equal(t, []byte("valueaaa"), v)
		k, v, err = c.Next()
		require.NoError(t, err)
		require.Equal(t, []byte("key1"), k)
		require.Equal(t, []byte("value1.1"), v)
		k, v, err = c.Next()
		require.NoError(t, err)
		require.Equal(t, []byte("key1"), k)
		require.Equal(t, []byte("value1.2"), v)
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
		require.Nil(t, k)
		require.Nil(t, v)

		c, err = rtx.Cursor(rocksdbTestTable)
		require.NoError(t, err)
		k, v, err = c.First()
		require.NoError(t, err)
		require.Equal(t, []byte("aaa"), k)
		require.Equal(t, []byte("valueaaa"), v)
		k, v, err = c.Next()
		require.NoError(t, err)
		require.Equal(t, []byte("key1"), k)
		require.Equal(t, []byte("value1.1"), v)
		k, v, err = c.Next()
		require.NoError(t, err)
		require.Equal(t, []byte("key1"), k)
		require.Equal(t, []byte("value1.2"), v)
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
		require.Nil(t, k)
		require.Nil(t, v)
	})
	t.Run("NotDupSort", func(t *testing.T) {
		_, mtx, _ := mdbxBaseCase(t)
		_, rtx, _ := rocksdbBaseCase(t)
		require.NoError(t, mtx.Put(mdbxNotDupSortTestTable, []byte("key3"), []byte("value3.2")))
		require.NoError(t, mtx.Put(mdbxNotDupSortTestTable, []byte("key3"), []byte("value3.1")))
		require.NoError(t, mtx.Put(mdbxNotDupSortTestTable, []byte("key3"), []byte("value3.3")))
		require.NoError(t, mtx.Put(mdbxNotDupSortTestTable, []byte("key1"), []byte("value1.3")))
		require.NoError(t, mtx.Put(mdbxNotDupSortTestTable, []byte("key1"), []byte("value1.1")))
		require.NoError(t, rtx.Put(rocksdbNotDupSortTestTable, []byte("key3"), []byte("value3.2")))
		require.NoError(t, rtx.Put(rocksdbNotDupSortTestTable, []byte("key3"), []byte("value3.1")))
		require.NoError(t, rtx.Put(rocksdbNotDupSortTestTable, []byte("key3"), []byte("value3.3")))
		require.NoError(t, rtx.Put(rocksdbNotDupSortTestTable, []byte("key1"), []byte("value1.3")))
		require.NoError(t, rtx.Put(rocksdbNotDupSortTestTable, []byte("key1"), []byte("value1.1")))

		c, err := mtx.Cursor(mdbxNotDupSortTestTable)
		k, v, err := c.First()
		require.NoError(t, err)
		require.Equal(t, []byte("key1"), k)
		require.Equal(t, []byte("value1.1"), v)
		k, v, err = c.Next()
		require.NoError(t, err)
		require.Equal(t, []byte("key3"), k)
		require.Equal(t, []byte("value3.3"), v)
		k, v, err = c.Next()
		require.NoError(t, err)
		require.Nil(t, k)
		require.Nil(t, v)

		c, err = rtx.Cursor(rocksdbNotDupSortTestTable)
		k, v, err = c.First()
		require.NoError(t, err)
		require.Equal(t, []byte("key1"), k)
		require.Equal(t, []byte("value1.1"), v)
		k, v, err = c.Next()
		require.NoError(t, err)
		require.Equal(t, []byte("key3"), k)
		require.Equal(t, []byte("value3.3"), v)
		k, v, err = c.Next()
		require.NoError(t, err)
		require.Nil(t, k)
		require.Nil(t, v)
	})
}

func TestDelete(t *testing.T) {
	t.Run("DupSort", func(t *testing.T) {
		_, mtx, _ := mdbxBaseCase(t)
		_, rtx, _ := rocksdbBaseCase(t)

		require.NoError(t, mtx.Delete(mdbxTestTable, []byte("key1")))
		require.NoError(t, rtx.Delete(rocksdbTestTable, []byte("key1")))

		c, err := mtx.Cursor(mdbxTestTable)
		require.NoError(t, err)
		k, v, err := c.First()
		require.NoError(t, err)
		require.Equal(t, []byte("key3"), k)
		require.Equal(t, []byte("value3.1"), v)
		k, v, err = c.Next()
		require.NoError(t, err)
		require.Equal(t, []byte("key3"), k)
		require.Equal(t, []byte("value3.3"), v)
		k, v, err = c.Next()
		require.NoError(t, err)
		require.Nil(t, k)
		require.Nil(t, v)

		c, err = rtx.Cursor(rocksdbTestTable)
		require.NoError(t, err)
		k, v, err = c.First()
		require.NoError(t, err)
		require.Equal(t, []byte("key3"), k)
		require.Equal(t, []byte("value3.1"), v)
		k, v, err = c.Next()
		require.NoError(t, err)
		require.Equal(t, []byte("key3"), k)
		require.Equal(t, []byte("value3.3"), v)
		k, v, err = c.Next()
		require.NoError(t, err)
		require.Nil(t, k)
		require.Nil(t, v)
	})
	t.Run("DupSort.SeekNextFor", func(t *testing.T) {
		_, mtx, _ := mdbxBaseCase(t)
		_, rtx, _ := rocksdbBaseCase(t)

		// construct data
		fromBlock := uint64(1)
		for i := fromBlock; i < fromBlock+10; i++ {
			require.NoError(t, mtx.Put(mdbxTestTable2, Uint64ToBytes(i), []byte(fmt.Sprintf("value%d", i))))
			require.NoError(t, rtx.Put(rocksdbTestTable2, Uint64ToBytes(i), []byte(fmt.Sprintf("value%d", i))))
		}

		mc, err := mtx.RwCursor(mdbxTestTable2)
		require.NoError(t, err)
		rc, err := rtx.RwCursor(rocksdbTestTable2)
		require.NoError(t, err)

		for k, _, err := mc.Seek(Uint64ToBytes(fromBlock + 1)); k != nil; k, _, err = mc.Next() {
			require.NoError(t, err)
			require.NoError(t, mtx.Delete(mdbxTestTable2, k))
		}

		for k, _, err := rc.Seek(Uint64ToBytes(fromBlock + 1)); k != nil; k, _, err = rc.Next() {
			fmt.Printf("loop. rc:%v\n", rc)
			require.NoError(t, err)
			require.NoError(t, rtx.Delete(rocksdbTestTable2, k))
			fmt.Printf("loop end\n")
		}
	})
	t.Run("notDupSort", func(t *testing.T) {
		_, mtx, _ := mdbxBaseCase(t)
		_, rtx, _ := rocksdbBaseCase(t)

		require.NoError(t, mtx.Put(mdbxNotDupSortTestTable, []byte("key3"), []byte("value3.2")))
		require.NoError(t, mtx.Put(mdbxNotDupSortTestTable, []byte("key3"), []byte("value3.1")))
		require.NoError(t, mtx.Put(mdbxNotDupSortTestTable, []byte("key3"), []byte("value3.3")))
		require.NoError(t, mtx.Put(mdbxNotDupSortTestTable, []byte("key1"), []byte("value1.3")))
		require.NoError(t, mtx.Put(mdbxNotDupSortTestTable, []byte("key1"), []byte("value1.1")))
		require.NoError(t, rtx.Put(rocksdbNotDupSortTestTable, []byte("key3"), []byte("value3.2")))
		require.NoError(t, rtx.Put(rocksdbNotDupSortTestTable, []byte("key3"), []byte("value3.1")))
		require.NoError(t, rtx.Put(rocksdbNotDupSortTestTable, []byte("key3"), []byte("value3.3")))
		require.NoError(t, rtx.Put(rocksdbNotDupSortTestTable, []byte("key1"), []byte("value1.3")))
		require.NoError(t, rtx.Put(rocksdbNotDupSortTestTable, []byte("key1"), []byte("value1.1")))

		require.NoError(t, mtx.Delete(mdbxNotDupSortTestTable, []byte("key1")))
		require.NoError(t, rtx.Delete(rocksdbNotDupSortTestTable, []byte("key1")))

		c, err := mtx.Cursor(mdbxNotDupSortTestTable)
		require.NoError(t, err)
		k, v, err := c.First()
		require.NoError(t, err)
		require.Equal(t, []byte("key3"), k)
		require.Equal(t, []byte("value3.3"), v)
		k, v, err = c.Next()
		require.NoError(t, err)
		require.Nil(t, k)
		require.Nil(t, v)

		c, err = rtx.Cursor(rocksdbNotDupSortTestTable)
		require.NoError(t, err)
		k, v, err = c.First()
		require.NoError(t, err)
		require.Equal(t, []byte("key3"), k)
		require.Equal(t, []byte("value3.3"), v)
		k, v, err = c.Next()
		require.NoError(t, err)
		require.Nil(t, k)
		require.Nil(t, v)
	})
	t.Run("notDupSort.SeekNextFor", func(t *testing.T) {
		_, mtx, _ := mdbxBaseCase(t)
		_, rtx, _ := rocksdbBaseCase(t)

		// construct data
		fromBlock := uint64(1)
		for i := fromBlock; i < fromBlock+10; i++ {
			require.NoError(t, mtx.Put(mdbxNotDupSortTestTable, Uint64ToBytes(i), []byte(fmt.Sprintf("value%d", i))))
			require.NoError(t, rtx.Put(rocksdbNotDupSortTestTable, Uint64ToBytes(i), []byte(fmt.Sprintf("value%d", i))))
		}

		mc, err := mtx.RwCursor(mdbxNotDupSortTestTable)
		require.NoError(t, err)
		rc, err := rtx.RwCursor(rocksdbNotDupSortTestTable)
		require.NoError(t, err)

		for k, _, err := mc.Seek(Uint64ToBytes(fromBlock + 1)); k != nil; k, _, err = mc.Next() {
			require.NoError(t, err)
			require.NoError(t, mtx.Delete(mdbxNotDupSortTestTable, k))
		}

		for k, _, err := rc.Seek(Uint64ToBytes(fromBlock + 1)); k != nil; k, _, err = rc.Next() {
			fmt.Printf("loop. rc:%v\n", rc)
			require.NoError(t, err)
			require.NoError(t, rtx.Delete(rocksdbNotDupSortTestTable, k))
			fmt.Printf("loop end\n")
		}
	})
}

func TestReadAndIncrementSequence(t *testing.T) {
	_, mtx, _ := mdbxBaseCase(t)
	_, rtx, _ := rocksdbBaseCase(t)

	v, err := mtx.ReadSequence(mdbxTestTable)
	require.NoError(t, err)
	require.Equal(t, uint64(0), v)
	v, err = rtx.ReadSequence(rocksdbTestTable)
	require.NoError(t, err)
	require.Equal(t, uint64(0), v)

	v, err = mtx.IncrementSequence(mdbxTestTable, uint64(2))
	require.NoError(t, err)
	require.Equal(t, uint64(0), v)
	v, err = rtx.IncrementSequence(rocksdbTestTable, uint64(2))
	require.NoError(t, err)
	require.Equal(t, uint64(0), v)

	v, err = mtx.IncrementSequence(mdbxTestTable, uint64(3))
	require.NoError(t, err)
	require.Equal(t, uint64(2), v)
	v, err = rtx.IncrementSequence(rocksdbTestTable, uint64(3))
	require.NoError(t, err)
	require.Equal(t, uint64(2), v)

	v, err = mtx.ReadSequence(mdbxTestTable)
	require.NoError(t, err)
	require.Equal(t, uint64(5), v)
	v, err = rtx.ReadSequence(rocksdbTestTable)
	require.NoError(t, err)
	require.Equal(t, uint64(5), v)
}

func TestRange(t *testing.T) {
	type kvPair struct {
		k, v []byte
	}
	checker := func(t *testing.T, c iter.KV, expects []kvPair) {
		for i, pair := range expects {
			require.True(t, c.HasNext())
			k, v, err := c.Next()
			require.NoError(t, err, "index: %d", i)
			require.Equal(t, pair.k, k, "index: %d", i)
			require.Equal(t, pair.v, v, "index: %d", i)
		}
		require.False(t, c.HasNext())
	}

	t.Run("Range", func(t *testing.T) {
		t.Run("From=nil&To=nil", func(t *testing.T) {
			_, mtx, _ := mdbxBaseCase(t)
			_, rtx, _ := rocksdbBaseCase(t)

			require.NoError(t, mtx.Put(mdbxTestTable, []byte("aaa"), []byte("valueaaa1.1")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("aaa"), []byte("valueaaa1.1")))

			c, err := mtx.Range(mdbxTestTable, nil, nil)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("aaa"), []byte("valueaaa1.1")},
				{[]byte("key1"), []byte("value1.1")},
				{[]byte("key1"), []byte("value1.3")},
				{[]byte("key3"), []byte("value3.1")},
				{[]byte("key3"), []byte("value3.3")},
			})

			c, err = rtx.Range(rocksdbTestTable, nil, nil)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("aaa"), []byte("valueaaa1.1")},
				{[]byte("key1"), []byte("value1.1")},
				{[]byte("key1"), []byte("value1.3")},
				{[]byte("key3"), []byte("value3.1")},
				{[]byte("key3"), []byte("value3.3")},
			})
		})
		t.Run("From!=nil&To=nil", func(t *testing.T) {
			_, mtx, _ := mdbxBaseCase(t)
			_, rtx, _ := rocksdbBaseCase(t)

			require.NoError(t, mtx.Put(mdbxTestTable, []byte("aaa"), []byte("valueaaa1.3")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("zzz1"), []byte("valuezzz1.3")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("zzz1"), []byte("valuezzz1.1")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("aaa"), []byte("valueaaa1.3")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("zzz1"), []byte("valuezzz1.3")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("zzz1"), []byte("valuezzz1.1")))

			c, err := mtx.Range(mdbxTestTable, []byte("key"), nil)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("key1"), []byte("value1.1")},
				{[]byte("key1"), []byte("value1.3")},
				{[]byte("key3"), []byte("value3.1")},
				{[]byte("key3"), []byte("value3.3")},
				{[]byte("zzz1"), []byte("valuezzz1.1")},
				{[]byte("zzz1"), []byte("valuezzz1.3")},
			})

			c, err = rtx.Range(rocksdbTestTable, []byte("key"), nil)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("key1"), []byte("value1.1")},
				{[]byte("key1"), []byte("value1.3")},
				{[]byte("key3"), []byte("value3.1")},
				{[]byte("key3"), []byte("value3.3")},
				{[]byte("zzz1"), []byte("valuezzz1.1")},
				{[]byte("zzz1"), []byte("valuezzz1.3")},
			})
		})
		t.Run("From=nil&To!=nil", func(t *testing.T) {
			_, mtx, _ := mdbxBaseCase(t)
			_, rtx, _ := rocksdbBaseCase(t)

			require.NoError(t, mtx.Put(mdbxTestTable, []byte("aaa"), []byte("valueaaa1.3")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("zzz1"), []byte("valuezzz1.3")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("zzz1"), []byte("valuezzz1.1")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("aaa"), []byte("valueaaa1.3")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("zzz1"), []byte("valuezzz1.3")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("zzz1"), []byte("valuezzz1.1")))

			c, err := mtx.Range(mdbxTestTable, nil, []byte("zzz"))
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("aaa"), []byte("valueaaa1.3")},
				{[]byte("key1"), []byte("value1.1")},
				{[]byte("key1"), []byte("value1.3")},
				{[]byte("key3"), []byte("value3.1")},
				{[]byte("key3"), []byte("value3.3")},
			})

			c, err = rtx.Range(rocksdbTestTable, nil, []byte("zzz"))
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("aaa"), []byte("valueaaa1.3")},
				{[]byte("key1"), []byte("value1.1")},
				{[]byte("key1"), []byte("value1.3")},
				{[]byte("key3"), []byte("value3.1")},
				{[]byte("key3"), []byte("value3.3")},
			})
		})
		t.Run("From!=nil&To!=nil", func(t *testing.T) {
			_, mtx, _ := mdbxBaseCase(t)
			_, rtx, _ := rocksdbBaseCase(t)

			require.NoError(t, mtx.Put(mdbxTestTable, []byte("aaa"), []byte("valueaaa1.3")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("www"), []byte("valuewww1.3")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("www"), []byte("valuewww1.1")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("zzz1"), []byte("valuezzz1.3")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("zzz1"), []byte("valuezzz1.1")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("aaa"), []byte("valueaaa1.3")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("www"), []byte("valuewww1.3")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("www"), []byte("valuewww1.1")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("zzz1"), []byte("valuezzz1.3")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("zzz1"), []byte("valuezzz1.1")))

			c, err := mtx.Range(mdbxTestTable, []byte("k"), []byte("zzz"))
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("key1"), []byte("value1.1")},
				{[]byte("key1"), []byte("value1.3")},
				{[]byte("key3"), []byte("value3.1")},
				{[]byte("key3"), []byte("value3.3")},
				{[]byte("www"), []byte("valuewww1.1")},
				{[]byte("www"), []byte("valuewww1.3")},
			})

			c, err = rtx.Range(rocksdbTestTable, []byte("k"), []byte("zzz"))
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("key1"), []byte("value1.1")},
				{[]byte("key1"), []byte("value1.3")},
				{[]byte("key3"), []byte("value3.1")},
				{[]byte("key3"), []byte("value3.3")},
				{[]byte("www"), []byte("valuewww1.1")},
				{[]byte("www"), []byte("valuewww1.3")},
			})
		})
	})
	t.Run("RangeAscend", func(t *testing.T) {
		t.Run("From=nil&To=nil&limit=-1", func(t *testing.T) {
			_, mtx, _ := mdbxBaseCase(t)
			_, rtx, _ := rocksdbBaseCase(t)

			require.NoError(t, mtx.Put(mdbxTestTable, []byte("aaa"), []byte("valueaaa1.1")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("aaa"), []byte("valueaaa1.1")))

			c, err := mtx.RangeAscend(mdbxTestTable, nil, nil, -1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("aaa"), []byte("valueaaa1.1")},
				{[]byte("key1"), []byte("value1.1")},
				{[]byte("key1"), []byte("value1.3")},
				{[]byte("key3"), []byte("value3.1")},
				{[]byte("key3"), []byte("value3.3")},
			})

			c, err = rtx.RangeAscend(rocksdbTestTable, nil, nil, -1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("aaa"), []byte("valueaaa1.1")},
				{[]byte("key1"), []byte("value1.1")},
				{[]byte("key1"), []byte("value1.3")},
				{[]byte("key3"), []byte("value3.1")},
				{[]byte("key3"), []byte("value3.3")},
			})
		})
		t.Run("From!=nil&To=nil&limit=-1", func(t *testing.T) {
			_, mtx, _ := mdbxBaseCase(t)
			_, rtx, _ := rocksdbBaseCase(t)

			require.NoError(t, mtx.Put(mdbxTestTable, []byte("aaa"), []byte("valueaaa1.3")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("zzz1"), []byte("valuezzz1.3")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("zzz1"), []byte("valuezzz1.1")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("aaa"), []byte("valueaaa1.3")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("zzz1"), []byte("valuezzz1.3")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("zzz1"), []byte("valuezzz1.1")))

			c, err := mtx.RangeAscend(mdbxTestTable, []byte("key"), nil, -1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("key1"), []byte("value1.1")},
				{[]byte("key1"), []byte("value1.3")},
				{[]byte("key3"), []byte("value3.1")},
				{[]byte("key3"), []byte("value3.3")},
				{[]byte("zzz1"), []byte("valuezzz1.1")},
				{[]byte("zzz1"), []byte("valuezzz1.3")},
			})

			c, err = rtx.RangeAscend(rocksdbTestTable, []byte("key"), nil, -1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("key1"), []byte("value1.1")},
				{[]byte("key1"), []byte("value1.3")},
				{[]byte("key3"), []byte("value3.1")},
				{[]byte("key3"), []byte("value3.3")},
				{[]byte("zzz1"), []byte("valuezzz1.1")},
				{[]byte("zzz1"), []byte("valuezzz1.3")},
			})
		})
		t.Run("From=nil&To!=nil&limit=-1", func(t *testing.T) {
			_, mtx, _ := mdbxBaseCase(t)
			_, rtx, _ := rocksdbBaseCase(t)

			require.NoError(t, mtx.Put(mdbxTestTable, []byte("aaa"), []byte("valueaaa1.3")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("zzz1"), []byte("valuezzz1.3")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("zzz1"), []byte("valuezzz1.1")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("aaa"), []byte("valueaaa1.3")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("zzz1"), []byte("valuezzz1.3")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("zzz1"), []byte("valuezzz1.1")))

			c, err := mtx.RangeAscend(mdbxTestTable, nil, []byte("zzz"), -1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("aaa"), []byte("valueaaa1.3")},
				{[]byte("key1"), []byte("value1.1")},
				{[]byte("key1"), []byte("value1.3")},
				{[]byte("key3"), []byte("value3.1")},
				{[]byte("key3"), []byte("value3.3")},
			})

			c, err = rtx.RangeAscend(rocksdbTestTable, nil, []byte("zzz"), -1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("aaa"), []byte("valueaaa1.3")},
				{[]byte("key1"), []byte("value1.1")},
				{[]byte("key1"), []byte("value1.3")},
				{[]byte("key3"), []byte("value3.1")},
				{[]byte("key3"), []byte("value3.3")},
			})
		})
		t.Run("From!=nil&To!=nil&limit=-1", func(t *testing.T) {
			_, mtx, _ := mdbxBaseCase(t)
			_, rtx, _ := rocksdbBaseCase(t)

			require.NoError(t, mtx.Put(mdbxTestTable, []byte("aaa"), []byte("valueaaa1.3")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("www"), []byte("valuewww1.3")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("www"), []byte("valuewww1.1")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("zzz1"), []byte("valuezzz1.3")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("zzz1"), []byte("valuezzz1.1")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("aaa"), []byte("valueaaa1.3")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("www"), []byte("valuewww1.3")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("www"), []byte("valuewww1.1")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("zzz1"), []byte("valuezzz1.3")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("zzz1"), []byte("valuezzz1.1")))

			c, err := mtx.RangeAscend(mdbxTestTable, []byte("k"), []byte("zzz"), -1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("key1"), []byte("value1.1")},
				{[]byte("key1"), []byte("value1.3")},
				{[]byte("key3"), []byte("value3.1")},
				{[]byte("key3"), []byte("value3.3")},
				{[]byte("www"), []byte("valuewww1.1")},
				{[]byte("www"), []byte("valuewww1.3")},
			})

			c, err = rtx.RangeAscend(rocksdbTestTable, []byte("k"), []byte("zzz"), -1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("key1"), []byte("value1.1")},
				{[]byte("key1"), []byte("value1.3")},
				{[]byte("key3"), []byte("value3.1")},
				{[]byte("key3"), []byte("value3.3")},
				{[]byte("www"), []byte("valuewww1.1")},
				{[]byte("www"), []byte("valuewww1.3")},
			})
		})
		t.Run("From!=nil&To!=nil&limit!=-1", func(t *testing.T) {
			_, mtx, _ := mdbxBaseCase(t)
			_, rtx, _ := rocksdbBaseCase(t)

			require.NoError(t, mtx.Put(mdbxTestTable, []byte("aaa"), []byte("valueaaa1.3")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("www"), []byte("valuewww1.3")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("www"), []byte("valuewww1.1")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("zzz1"), []byte("valuezzz1.3")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("zzz1"), []byte("valuezzz1.1")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("aaa"), []byte("valueaaa1.3")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("www"), []byte("valuewww1.3")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("www"), []byte("valuewww1.1")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("zzz1"), []byte("valuezzz1.3")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("zzz1"), []byte("valuezzz1.1")))

			c, err := mtx.RangeAscend(mdbxTestTable, []byte("k"), []byte("zzz"), 3)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("key1"), []byte("value1.1")},
				{[]byte("key1"), []byte("value1.3")},
				{[]byte("key3"), []byte("value3.1")},
			})

			c, err = rtx.RangeAscend(rocksdbTestTable, []byte("k"), []byte("zzz"), 3)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("key1"), []byte("value1.1")},
				{[]byte("key1"), []byte("value1.3")},
				{[]byte("key3"), []byte("value3.1")},
			})
		})
	})
	t.Run("RangeDescend", func(t *testing.T) {
		t.Run("From=nil&To=nil&limit=-1", func(t *testing.T) {
			_, mtx, _ := mdbxBaseCase(t)
			_, rtx, _ := rocksdbBaseCase(t)

			require.NoError(t, mtx.Put(mdbxTestTable, []byte("aaa"), []byte("valueaaa1.1")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("aaa"), []byte("valueaaa1.1")))

			c, err := mtx.RangeDescend(mdbxTestTable, nil, nil, -1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("key3"), []byte("value3.3")},
				{[]byte("key3"), []byte("value3.1")},
				{[]byte("key1"), []byte("value1.3")},
				{[]byte("key1"), []byte("value1.1")},
				{[]byte("aaa"), []byte("valueaaa1.1")},
			})

			c, err = rtx.RangeDescend(rocksdbTestTable, nil, nil, -1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("key3"), []byte("value3.3")},
				{[]byte("key3"), []byte("value3.1")},
				{[]byte("key1"), []byte("value1.3")},
				{[]byte("key1"), []byte("value1.1")},
				{[]byte("aaa"), []byte("valueaaa1.1")},
			})
		})
		t.Run("From!=nil&To=nil&limit=-1", func(t *testing.T) {
			_, mtx, _ := mdbxBaseCase(t)
			_, rtx, _ := rocksdbBaseCase(t)

			require.NoError(t, mtx.Put(mdbxTestTable, []byte("aaa"), []byte("valueaaa1.3")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("zzz1"), []byte("valuezzz1.3")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("zzz1"), []byte("valuezzz1.1")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("aaa"), []byte("valueaaa1.3")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("zzz1"), []byte("valuezzz1.3")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("zzz1"), []byte("valuezzz1.1")))

			c, err := mtx.RangeDescend(mdbxTestTable, []byte("key3"), nil, -1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("key3"), []byte("value3.3")},
				{[]byte("key3"), []byte("value3.1")},
				{[]byte("key1"), []byte("value1.3")},
				{[]byte("key1"), []byte("value1.1")},
				{[]byte("aaa"), []byte("valueaaa1.3")},
			})

			c, err = rtx.RangeDescend(rocksdbTestTable, []byte("key3"), nil, -1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("key3"), []byte("value3.3")},
				{[]byte("key3"), []byte("value3.1")},
				{[]byte("key1"), []byte("value1.3")},
				{[]byte("key1"), []byte("value1.1")},
				{[]byte("aaa"), []byte("valueaaa1.3")},
			})
		})
		t.Run("From=nil&To!=nil&limit=-1", func(t *testing.T) {
			_, mtx, _ := mdbxBaseCase(t)
			_, rtx, _ := rocksdbBaseCase(t)

			require.NoError(t, mtx.Put(mdbxTestTable, []byte("aaa"), []byte("valueaaa1.3")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("zzz1"), []byte("valuezzz1.3")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("zzz1"), []byte("valuezzz1.1")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("aaa"), []byte("valueaaa1.3")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("zzz1"), []byte("valuezzz1.3")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("zzz1"), []byte("valuezzz1.1")))

			c, err := mtx.RangeDescend(mdbxTestTable, nil, []byte("key"), -1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("zzz1"), []byte("valuezzz1.3")},
				{[]byte("zzz1"), []byte("valuezzz1.1")},
				{[]byte("key3"), []byte("value3.3")},
				{[]byte("key3"), []byte("value3.1")},
				{[]byte("key1"), []byte("value1.3")},
				{[]byte("key1"), []byte("value1.1")},
			})

			c, err = rtx.RangeDescend(rocksdbTestTable, nil, []byte("key"), -1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("zzz1"), []byte("valuezzz1.3")},
				{[]byte("zzz1"), []byte("valuezzz1.1")},
				{[]byte("key3"), []byte("value3.3")},
				{[]byte("key3"), []byte("value3.1")},
				{[]byte("key1"), []byte("value1.3")},
				{[]byte("key1"), []byte("value1.1")},
			})
		})
		t.Run("From!=nil&To!=nil&limit=-1", func(t *testing.T) {
			_, mtx, _ := mdbxBaseCase(t)
			_, rtx, _ := rocksdbBaseCase(t)

			require.NoError(t, mtx.Put(mdbxTestTable, []byte("aaa"), []byte("valueaaa1.3")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("www"), []byte("valuewww1.3")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("www"), []byte("valuewww1.1")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("zzz1"), []byte("valuezzz1.3")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("zzz1"), []byte("valuezzz1.1")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("aaa"), []byte("valueaaa1.3")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("www"), []byte("valuewww1.3")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("www"), []byte("valuewww1.1")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("zzz1"), []byte("valuezzz1.3")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("zzz1"), []byte("valuezzz1.1")))

			c, err := mtx.RangeDescend(mdbxTestTable, []byte("zzz1"), []byte("k"), -1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("zzz1"), []byte("valuezzz1.3")},
				{[]byte("zzz1"), []byte("valuezzz1.1")},
				{[]byte("www"), []byte("valuewww1.3")},
				{[]byte("www"), []byte("valuewww1.1")},
				{[]byte("key3"), []byte("value3.3")},
				{[]byte("key3"), []byte("value3.1")},
				{[]byte("key1"), []byte("value1.3")},
				{[]byte("key1"), []byte("value1.1")},
			})

			c, err = rtx.RangeDescend(rocksdbTestTable, []byte("zzz1"), []byte("k"), -1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("zzz1"), []byte("valuezzz1.3")},
				{[]byte("zzz1"), []byte("valuezzz1.1")},
				{[]byte("www"), []byte("valuewww1.3")},
				{[]byte("www"), []byte("valuewww1.1")},
				{[]byte("key3"), []byte("value3.3")},
				{[]byte("key3"), []byte("value3.1")},
				{[]byte("key1"), []byte("value1.3")},
				{[]byte("key1"), []byte("value1.1")},
			})
		})
		t.Run("From!=nil&To!=nil&limit!=-1", func(t *testing.T) {
			_, mtx, _ := mdbxBaseCase(t)
			_, rtx, _ := rocksdbBaseCase(t)

			require.NoError(t, mtx.Put(mdbxTestTable, []byte("aaa"), []byte("valueaaa1.3")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("www"), []byte("valuewww1.3")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("www"), []byte("valuewww1.1")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("zzz1"), []byte("valuezzz1.3")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("zzz1"), []byte("valuezzz1.1")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("aaa"), []byte("valueaaa1.3")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("www"), []byte("valuewww1.3")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("www"), []byte("valuewww1.1")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("zzz1"), []byte("valuezzz1.3")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("zzz1"), []byte("valuezzz1.1")))

			c, err := mtx.RangeDescend(mdbxTestTable, []byte("zzz1"), []byte("k"), 3)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("zzz1"), []byte("valuezzz1.3")},
				{[]byte("zzz1"), []byte("valuezzz1.1")},
				{[]byte("www"), []byte("valuewww1.3")},
			})

			c, err = rtx.RangeDescend(rocksdbTestTable, []byte("zzz1"), []byte("k"), 3)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("zzz1"), []byte("valuezzz1.3")},
				{[]byte("zzz1"), []byte("valuezzz1.1")},
				{[]byte("www"), []byte("valuewww1.3")},
			})
		})
	})
}

func TestPrefix(t *testing.T) {
	type kvPair struct {
		k, v []byte
	}
	checker := func(t *testing.T, c iter.KV, expects []kvPair) {
		for i, pair := range expects {
			require.True(t, c.HasNext())
			k, v, err := c.Next()
			require.NoError(t, err, "index: %d", i)
			require.Equal(t, pair.k, k, "index: %d", i)
			require.Equal(t, pair.v, v, "index: %d", i)
		}
		require.False(t, c.HasNext())
	}

	_, mtx, _ := mdbxBaseCase(t)
	_, rtx, _ := rocksdbBaseCase(t)

	require.NoError(t, mtx.Put(mdbxTestTable, []byte("aaa"), []byte("valueaaa1.3")))
	require.NoError(t, mtx.Put(mdbxTestTable, []byte("www"), []byte("valuewww1.3")))
	require.NoError(t, mtx.Put(mdbxTestTable, []byte("www"), []byte("valuewww1.1")))
	require.NoError(t, mtx.Put(mdbxTestTable, []byte("zzz1"), []byte("valuezzz1.3")))
	require.NoError(t, mtx.Put(mdbxTestTable, []byte("zzz1"), []byte("valuezzz1.1")))
	require.NoError(t, rtx.Put(rocksdbTestTable, []byte("aaa"), []byte("valueaaa1.3")))
	require.NoError(t, rtx.Put(rocksdbTestTable, []byte("www"), []byte("valuewww1.3")))
	require.NoError(t, rtx.Put(rocksdbTestTable, []byte("www"), []byte("valuewww1.1")))
	require.NoError(t, rtx.Put(rocksdbTestTable, []byte("zzz1"), []byte("valuezzz1.3")))
	require.NoError(t, rtx.Put(rocksdbTestTable, []byte("zzz1"), []byte("valuezzz1.1")))

	c, err := mtx.Prefix(mdbxTestTable, []byte("key"))
	require.NoError(t, err)
	checker(t, c, []kvPair{
		{[]byte("key1"), []byte("value1.1")},
		{[]byte("key1"), []byte("value1.3")},
		{[]byte("key3"), []byte("value3.1")},
		{[]byte("key3"), []byte("value3.3")},
	})
	c, err = rtx.Prefix(rocksdbTestTable, []byte("key"))
	require.NoError(t, err)
	checker(t, c, []kvPair{
		{[]byte("key1"), []byte("value1.1")},
		{[]byte("key1"), []byte("value1.3")},
		{[]byte("key3"), []byte("value3.1")},
		{[]byte("key3"), []byte("value3.3")},
	})

	c, err = mtx.Prefix(mdbxTestTable, []byte("w"))
	require.NoError(t, err)
	checker(t, c, []kvPair{
		{[]byte("www"), []byte("valuewww1.1")},
		{[]byte("www"), []byte("valuewww1.3")},
	})
	c, err = rtx.Prefix(rocksdbTestTable, []byte("w"))
	require.NoError(t, err)
	checker(t, c, []kvPair{
		{[]byte("www"), []byte("valuewww1.1")},
		{[]byte("www"), []byte("valuewww1.3")},
	})
}

func TestRangeDupSort(t *testing.T) {
	type kvPair struct {
		k, v []byte
	}
	checker := func(t *testing.T, c iter.KV, expects []kvPair) {
		for i, pair := range expects {
			require.True(t, c.HasNext(), "index: %d", i)
			k, v, err := c.Next()
			require.NoError(t, err, "index: %d", i)
			require.Equal(t, pair.k, k, "index: %d", i)
			require.Equal(t, pair.v, v, "index: %d", i)
		}
		require.False(t, c.HasNext())
	}

	t.Run("Asc", func(t *testing.T) {
		t.Run("fromPrefix=nil&toPrefix=nil&limit=-1", func(t *testing.T) {
			_, mtx, _ := mdbxBaseCase(t)
			_, rtx, _ := rocksdbBaseCase(t)

			c, err := mtx.RangeDupSort(mdbxTestTable, []byte("key1"), nil, nil, order.Asc, -1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("key1"), []byte("value1.1")},
				{[]byte("key1"), []byte("value1.3")},
			})
			c, err = rtx.RangeDupSort(rocksdbTestTable, []byte("key1"), nil, nil, order.Asc, -1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("key1"), []byte("value1.1")},
				{[]byte("key1"), []byte("value1.3")},
			})
		})
		t.Run("fromPrefix!=nil&toPrefix=nil&limit=-1", func(t *testing.T) {
			_, mtx, _ := mdbxBaseCase(t)
			_, rtx, _ := rocksdbBaseCase(t)

			require.NoError(t, mtx.Put(mdbxTestTable, []byte("key5"), []byte("xxvalue")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("key5"), []byte("yyvalue")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("key5"), []byte("zzvalue")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("key5"), []byte("xxvalue")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("key5"), []byte("yyvalue")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("key5"), []byte("zzvalue")))

			c, err := mtx.RangeDupSort(mdbxTestTable, []byte("key5"), []byte("yy"), nil, order.Asc, -1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("key5"), []byte("yyvalue")},
				{[]byte("key5"), []byte("zzvalue")},
			})
			c, err = rtx.RangeDupSort(rocksdbTestTable, []byte("key5"), []byte("yy"), nil, order.Asc, -1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("key5"), []byte("yyvalue")},
				{[]byte("key5"), []byte("zzvalue")},
			})
		})
		t.Run("fromPrefix=nil&toPrefix!=nil&limit=-1", func(t *testing.T) {
			_, mtx, _ := mdbxBaseCase(t)
			_, rtx, _ := rocksdbBaseCase(t)

			require.NoError(t, mtx.Put(mdbxTestTable, []byte("key5"), []byte("xxvalue")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("key5"), []byte("yyvalue")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("key5"), []byte("zzvalue")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("key5"), []byte("xxvalue")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("key5"), []byte("yyvalue")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("key5"), []byte("zzvalue")))

			c, err := mtx.RangeDupSort(mdbxTestTable, []byte("key5"), nil, []byte("zz"), order.Asc, -1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("key5"), []byte("xxvalue")},
				{[]byte("key5"), []byte("yyvalue")},
			})
			c, err = rtx.RangeDupSort(rocksdbTestTable, []byte("key5"), nil, []byte("zz"), order.Asc, -1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("key5"), []byte("xxvalue")},
				{[]byte("key5"), []byte("yyvalue")},
			})
		})
		t.Run("fromPrefix!=nil&toPrefix!=nil&limit=-1", func(t *testing.T) {
			_, mtx, _ := mdbxBaseCase(t)
			_, rtx, _ := rocksdbBaseCase(t)

			require.NoError(t, mtx.Put(mdbxTestTable, []byte("key5"), []byte("xxvalue")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("key5"), []byte("yyvalue")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("key5"), []byte("zzvalue")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("key5"), []byte("xxvalue")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("key5"), []byte("yyvalue")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("key5"), []byte("zzvalue")))

			c, err := mtx.RangeDupSort(mdbxTestTable, []byte("key5"), []byte("yy"), []byte("zz"), order.Asc, -1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("key5"), []byte("yyvalue")},
			})
			c, err = rtx.RangeDupSort(rocksdbTestTable, []byte("key5"), []byte("yy"), []byte("zz"), order.Asc, -1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("key5"), []byte("yyvalue")},
			})
		})
		t.Run("fromPrefix!=nil&toPrefix!=nil&limit!=-1", func(t *testing.T) {
			_, mtx, _ := mdbxBaseCase(t)
			_, rtx, _ := rocksdbBaseCase(t)

			require.NoError(t, mtx.Put(mdbxTestTable, []byte("key5"), []byte("xxvalue")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("key5"), []byte("yyvalue")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("key5"), []byte("zzvalue")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("key5"), []byte("xxvalue")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("key5"), []byte("yyvalue")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("key5"), []byte("zzvalue")))

			c, err := mtx.RangeDupSort(mdbxTestTable, []byte("key5"), []byte("xx"), []byte("zz"), order.Asc, 1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("key5"), []byte("xxvalue")},
			})
			c, err = rtx.RangeDupSort(rocksdbTestTable, []byte("key5"), []byte("xx"), []byte("zz"), order.Asc, 1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("key5"), []byte("xxvalue")},
			})
		})
	})
	t.Run("Desc", func(t *testing.T) {
		t.Run("fromPrefix=nil&toPrefix=nil&limit=-1", func(t *testing.T) {
			_, mtx, _ := mdbxBaseCase(t)
			_, rtx, _ := rocksdbBaseCase(t)

			c, err := mtx.RangeDupSort(mdbxTestTable, []byte("key1"), nil, nil, order.Desc, -1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("key1"), []byte("value1.3")},
				{[]byte("key1"), []byte("value1.1")},
			})
			c, err = rtx.RangeDupSort(rocksdbTestTable, []byte("key1"), nil, nil, order.Desc, -1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("key1"), []byte("value1.3")},
				{[]byte("key1"), []byte("value1.1")},
			})
		})
		t.Run("fromPrefix!=nil&toPrefix=nil&limit=-1", func(t *testing.T) {
			_, mtx, _ := mdbxBaseCase(t)
			_, rtx, _ := rocksdbBaseCase(t)

			require.NoError(t, mtx.Put(mdbxTestTable, []byte("key5"), []byte("xxvalue")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("key5"), []byte("yyvalue")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("key5"), []byte("zzvalue")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("key5"), []byte("xxvalue")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("key5"), []byte("yyvalue")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("key5"), []byte("zzvalue")))

			c, err := mtx.RangeDupSort(mdbxTestTable, []byte("key5"), []byte("yy"), nil, order.Desc, -1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("key5"), []byte("xxvalue")},
			})
			c, err = rtx.RangeDupSort(rocksdbTestTable, []byte("key5"), []byte("yy"), nil, order.Desc, -1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("key5"), []byte("xxvalue")},
			})
		})
		t.Run("fromPrefix=nil&toPrefix!=nil&limit=-1", func(t *testing.T) {
			_, mtx, _ := mdbxBaseCase(t)
			_, rtx, _ := rocksdbBaseCase(t)

			require.NoError(t, mtx.Put(mdbxTestTable, []byte("key5"), []byte("xxvalue")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("key5"), []byte("yyvalue")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("key5"), []byte("zzvalue")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("key5"), []byte("xxvalue")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("key5"), []byte("yyvalue")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("key5"), []byte("zzvalue")))

			c, err := mtx.RangeDupSort(mdbxTestTable, []byte("key5"), nil, []byte("yy"), order.Desc, -1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("key5"), []byte("zzvalue")},
				{[]byte("key5"), []byte("yyvalue")},
			})
			c, err = rtx.RangeDupSort(rocksdbTestTable, []byte("key5"), nil, []byte("yy"), order.Desc, -1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("key5"), []byte("zzvalue")},
				{[]byte("key5"), []byte("yyvalue")},
			})
		})
		t.Run("fromPrefix!=nil&toPrefix!=nil&limit=-1", func(t *testing.T) {
			_, mtx, _ := mdbxBaseCase(t)
			_, rtx, _ := rocksdbBaseCase(t)

			require.NoError(t, mtx.Put(mdbxTestTable, []byte("key5"), []byte("xxvalue")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("key5"), []byte("yyvalue")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("key5"), []byte("zzvalue")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("key5"), []byte("xxvalue")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("key5"), []byte("yyvalue")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("key5"), []byte("zzvalue")))

			c, err := mtx.RangeDupSort(mdbxTestTable, []byte("key5"), []byte("zz"), []byte("yy"), order.Desc, -1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("key5"), []byte("yyvalue")},
			})
			c, err = rtx.RangeDupSort(rocksdbTestTable, []byte("key5"), []byte("zz"), []byte("yy"), order.Desc, -1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("key5"), []byte("yyvalue")},
			})
		})
		t.Run("fromPrefix!=nil&toPrefix!=nil&limit!=-1", func(t *testing.T) {
			_, mtx, _ := mdbxBaseCase(t)
			_, rtx, _ := rocksdbBaseCase(t)

			require.NoError(t, mtx.Put(mdbxTestTable, []byte("key5"), []byte("xxvalue")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("key5"), []byte("yyvalue")))
			require.NoError(t, mtx.Put(mdbxTestTable, []byte("key5"), []byte("zzvalue")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("key5"), []byte("xxvalue")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("key5"), []byte("yyvalue")))
			require.NoError(t, rtx.Put(rocksdbTestTable, []byte("key5"), []byte("zzvalue")))

			c, err := mtx.RangeDupSort(mdbxTestTable, []byte("key5"), []byte("zz"), []byte("xx"), order.Desc, 1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("key5"), []byte("yyvalue")},
			})
			c, err = rtx.RangeDupSort(rocksdbTestTable, []byte("key5"), []byte("zz"), []byte("xx"), order.Desc, 1)
			require.NoError(t, err)
			checker(t, c, []kvPair{
				{[]byte("key5"), []byte("yyvalue")},
			})
		})

	})
}

func TestClearBucket(t *testing.T) {
	_, mtx, _ := mdbxBaseCase(t)
	_, rtx, _ := rocksdbBaseCase(t)

	require.NoError(t, mtx.ClearBucket(mdbxTestTable))
	require.NoError(t, rtx.ClearBucket(rocksdbTestTable))

	v, err := mtx.GetOne(mdbxTestTable, []byte("key1"))
	require.NoError(t, err)
	require.Nil(t, v)

	v, err = rtx.GetOne(rocksdbTestTable, []byte("key1"))
	require.NoError(t, err)
	require.Nil(t, v)
}

func Uint64ToBytes(i uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, i)
	return buf
}
