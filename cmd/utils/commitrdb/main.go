package main

import (
	"context"
	"encoding/json"
	"flag"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/rocksdb"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/semaphore"
)

type dataPair struct {
	K []byte
	V []byte
}

func main() {
	go func() {
		http.ListenAndServe(":6061", nil)
	}()
	rocksdbPath := flag.String("rocksdb", "", "Path to the target RocksDB database")
	dataPath := flag.String("data", "", "Path to the target RocksDB database")
	table := flag.String("table", "", "table name")
	flag.Parse()

	logger := log.Root()
	logger.SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))

	batch, err := loadDataFromFile(*dataPath)
	if err != nil {
		logger.Error("Failed to load data from file", "path", *dataPath, "error", err)
		os.Exit(1)
	}

	// Open the target RocksDB database
	logger.Info("Opening target RocksDB database", "path", *rocksdbPath)
	dstDB, err := openRocksDB(*rocksdbPath, logger)
	if err != nil {
		logger.Error("Failed to open RocksDB database", "path", *rocksdbPath, "error", err)
		os.Exit(1)
	}
	defer dstDB.Close()

	err = dstDB.Update(context.Background(), func(tx kv.RwTx) error {
		c, err := tx.RwCursor(*table)
		if err != nil {
			return err
		}

		for _, item := range batch {
			if err := c.Put(item.K, item.V); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		logger.Error("Failed to put batch to database", "path", dataPath, "error", err)
		os.Exit(1)
	}
}

func openRocksDB(path string, logger log.Logger) (kv.RwDB, error) {
	targetSemCount := int64(runtime.GOMAXPROCS(-1)) - 1
	if targetSemCount <= 0 {
		targetSemCount = 1
	}

	readTxLimit := int64(32)
	roTxsLimiter := semaphore.NewWeighted(readTxLimit)

	return rocksdb.NewRocksDB(path, logger, kv.ChaindataTablesCfg, kv.ChainDB, roTxsLimiter, false, rocksdb.RealRDB)
}

func loadDataFromFile(filename string) ([]dataPair, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var pairs []dataPair
	dec := json.NewDecoder(file)
	err = dec.Decode(&pairs)
	if err != nil {
		return nil, err
	}

	return pairs, nil
}
