package rocksdb

import (
	"context"
	"golang.org/x/sync/semaphore"
	"gotest.tools/v3/assert"
	"os"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
)

var mdbxTestTable string = "Table"
var mdbxTestTable2 string = "TestTable2"
var mdbxNotDupSortTestTable string = "mdbxNotDupSortTestTable"
var mdbxAutoDupSortKeysConversionTable = "mdbxAutoDupSortKeysConversionTable"
var rocksdbTestTable string = "rTable"
var rocksdbTestTable2 string = "rTestTable2"
var rocksdbNotDupSortTestTable string = "rocksdbNotDupSortTestTable"
var rocksdbAutoDupSortKeysConversionTable = "mdbxAutoDupSortKeysConversionTable"

func mdbxBaseCaseDB(t *testing.T) kv.RwDB {
	t.Helper()
	path := t.TempDir()
	logger := log.New()
	db := mdbx.NewMDBX(logger).InMem(path).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			mdbxTestTable:           kv.TableCfgItem{Flags: kv.DupSort},
			mdbxTestTable2:          kv.TableCfgItem{Flags: kv.DupSort},
			mdbxNotDupSortTestTable: kv.TableCfgItem{},
			kv.Sequence:             kv.TableCfgItem{},
			mdbxAutoDupSortKeysConversionTable: kv.TableCfgItem{
				Flags:                     kv.DupSort,
				AutoDupSortKeysConversion: true,
				DupFromLen:                8,
				DupToLen:                  5,
			},
		}
	}).MapSize(128 * datasize.MB).MustOpen()
	t.Cleanup(db.Close)
	return db
}

func mdbxBaseCase(t *testing.T) (kv.RwDB, kv.RwTx, kv.RwCursorDupSort) {
	t.Helper()
	db := mdbxBaseCaseDB(t)

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)

	c, err := tx.RwCursorDupSort(mdbxTestTable)
	require.NoError(t, err)
	t.Cleanup(c.Close)

	// Insert some dupsorted records
	require.NoError(t, c.Put([]byte("key1"), []byte("value1.1")))
	require.NoError(t, c.Put([]byte("key3"), []byte("value3.1")))
	require.NoError(t, c.Put([]byte("key1"), []byte("value1.3")))
	require.NoError(t, c.Put([]byte("key3"), []byte("value3.3")))

	return db, tx, c
}

func rocksdbBaseCaseDB(t *testing.T) kv.RwDB {
	t.Helper()
	rdbPath := t.TempDir()
	t.Cleanup(func() { os.RemoveAll(rdbPath) })

	buckets := kv.TableCfg{
		rocksdbTestTable:           kv.TableCfgItem{Flags: kv.DupSort},
		rocksdbTestTable2:          kv.TableCfgItem{Flags: kv.DupSort},
		rocksdbNotDupSortTestTable: kv.TableCfgItem{},
		kv.Sequence:                kv.TableCfgItem{},
		rocksdbAutoDupSortKeysConversionTable: kv.TableCfgItem{
			Flags:                     kv.DupSort,
			AutoDupSortKeysConversion: true,
			DupFromLen:                8,
			DupToLen:                  5,
		},
	}

	db, err := NewRocksDB(rdbPath, log.New("test"), buckets, kv.ChainDB, semaphore.NewWeighted(10), false, RealRDB)
	assert.NilError(t, err)
	t.Cleanup(db.Close)

	return db
}

func rocksdbBaseCase(t *testing.T) (kv.RwDB, kv.RwTx, kv.RwCursorDupSort) {
	t.Helper()
	db := rocksdbBaseCaseDB(t)

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)

	c, err := tx.RwCursorDupSort(rocksdbTestTable)
	require.NoError(t, err)
	t.Cleanup(c.Close)

	// Insert some dupsorted records
	require.NoError(t, c.Put([]byte("key1"), []byte("value1.1")))
	require.NoError(t, c.Put([]byte("key3"), []byte("value3.1")))
	require.NoError(t, c.Put([]byte("key1"), []byte("value1.3")))
	require.NoError(t, c.Put([]byte("key3"), []byte("value3.3")))

	return db, tx, c
}
