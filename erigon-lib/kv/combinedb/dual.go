package combinedb

import (
	"fmt"
	"sync/atomic"

	"github.com/ledgerwatch/erigon-lib/kv/iter"
)

type CombineDual struct {
	mdbxDual    iter.KV
	rocksdbDual iter.KV

	logger *combineLogger
}

var dualCounter atomic.Uint64

func newCombineDual(parentLogger *combineLogger, mdbxDual iter.KV, rocksdbDual iter.KV) iter.KV {
	return &CombineDual{
		mdbxDual:    mdbxDual,
		rocksdbDual: rocksdbDual,
		logger:      newCombinLogger(parentLogger.isEnable(), fmt.Sprintf("%s dualid=%d", parentLogger.getPrefix(), dualCounter.Add(1))),
	}
}

func (d *CombineDual) Next() (k []byte, v []byte, err error) {
	d.logger.Info("Next")
	defer d.logger.Infof("Next done. k=%x, v=%x, err=%v", k, v, err)

	k1, v1, err1 := d.mdbxDual.Next()
	k2, v2, err2 := d.rocksdbDual.Next()
	if err := assertError(d.logger, err1, err2, "Next"); err != nil {
		return nil, nil, err
	}
	assertEqualF(d.logger, k1, k2, "Next key mismatch. mdbx=%x. rocksdb=%x.", k1, k2)
	assertEqualF(d.logger, v1, v2, "Next value mismatch. mdbx=%x. rocksdb=%s", v1, v2)

	return k1, v1, nil
}

func (d *CombineDual) HasNext() (b bool) {
	d.logger.Info("HasNext")
	defer d.logger.Infof("HasNext done. b=%v", b)

	b1 := d.mdbxDual.HasNext()
	b2 := d.rocksdbDual.HasNext()
	assertEqualF(d.logger, b1, b2, "HasNext mismatch. mdbx=%v. rocksdb=%v", b1, b2)

	return b1
}
