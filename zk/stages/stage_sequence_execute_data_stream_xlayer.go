package stages

import (
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/zkevm/log"
)

func newSequencerBatchNonValidationStreamWriter(batchContext *BatchContext, batchState *BatchState) *SequencerBatchStreamWriter {
	return &SequencerBatchStreamWriter{
		batchContext:   batchContext,
		batchState:     batchState,
		ctx:            batchContext.ctx,
		logPrefix:      batchContext.s.LogPrefix(),
		legacyVerifier: batchContext.cfg.legacyVerifier,
		sdb:            batchContext.sdb,
		streamServer:   batchContext.cfg.nonValidationDataStreamServer,
		hasExecutors:   batchState.hasExecutorForThisBatch,
	}
}

func (sbc *SequencerBatchStreamWriter) CommitNewUpdatesWithoutVerification(forkId, batchNumber uint64, blockNumbers []uint64) error {
	return sbc.writeBlockDetailsToDatastreamWithoutVerification(forkId, batchNumber, blockNumbers)
}

func (sbc *SequencerBatchStreamWriter) writeBlockDetailsToDatastreamWithoutVerification(forkId, batchNumber uint64, blockNumbers []uint64) error {
	highestClosedBatch, err := sbc.streamServer.GetHighestClosedBatch()
	if err != nil {
		return err
	}
	highestStartedBatch, err := sbc.streamServer.GetHighestBatchNumber()
	if err != nil {
		return err
	}

	isCurrentBatchHigherThanLastInDatastream := batchNumber > highestStartedBatch
	isLastBatchInDatastremClosed := highestClosedBatch == highestStartedBatch
	if isCurrentBatchHigherThanLastInDatastream && !isLastBatchInDatastremClosed {
		if err := finalizeLastBatchInDatastream(sbc.batchContext, highestStartedBatch, blockNumbers[0]-1); err != nil {
			return err
		}
	}
	previousBlock, err := rawdb.ReadBlockByNumber(sbc.sdb.tx, blockNumbers[len(blockNumbers)-1]-1)
	if err != nil {
		return err
	}
	block, err := rawdb.ReadBlockByNumber(sbc.sdb.tx, blockNumbers[len(blockNumbers)-1])
	if err != nil {
		return err
	}

	previousBlockBatchNumber := batchNumber
	if len(blockNumbers) == 1 {
		var found bool
		previousBlockBatchNumber, found, err = sbc.sdb.hermezDb.HermezDbReader.CheckBatchNoByL2Block(previousBlock.NumberU64())
		if !found || err != nil {
			return err
		}
	}

	log.Info(fmt.Sprintf("writeBlockDetailsToDatastreamWithoutVerification  time=%s, batchNumber=%d, first=%d, last=%d", time.Now().UnixMilli(), batchNumber, blockNumbers[0], blockNumbers[len(blockNumbers)-1]))
	if err := sbc.streamServer.WriteBlockWithBatchStartToStream(sbc.logPrefix, sbc.sdb.tx, sbc.sdb.hermezDb, forkId, batchNumber, previousBlockBatchNumber, *previousBlock, *block); err != nil {
		return err
	}

	if err = stages.SaveStageProgress(sbc.sdb.tx, stages.NonValidationDataStream, block.NumberU64()); err != nil {
		return err
	}

	return nil
}
