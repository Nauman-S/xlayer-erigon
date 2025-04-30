package stages

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/zk/datastream/server"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/sequencer"
	"github.com/ledgerwatch/log/v3"
)

func CatchupNonValidationDatastream(ctx context.Context, logPrefix string, tx kv.RwTx, srv server.DataStreamServer) (uint64, error) {
	reader := hermez_db.NewHermezDbReader(tx)

	var (
		err              error
		finalBlockNumber uint64
	)

	if sequencer.IsSequencer() {
		finalBlockNumber, err = stages.GetStageProgress(tx, stages.NonValidationDataStream)
		if err != nil {
			return 0, err
		}

		// this handles a case where the node was synced in RPC mode without a datastream
		// in this case the datastream progress will be 0 but it still should catch up
		if finalBlockNumber == 0 {
			finalBlockNumber, err = stages.GetStageProgress(tx, stages.Execution)
			if err != nil {
				return 0, err
			}
		}
	} else {
		finalBlockNumber, err = stages.GetStageProgress(tx, stages.Execution)
		if err != nil {
			return 0, err
		}
	}

	previousProgress, err := srv.GetHighestBlockNumber()
	if err != nil {
		return 0, err
	}

	log.Info(fmt.Sprintf("[%s] Getting progress", logPrefix),
		"adding up to blockNum", finalBlockNumber,
		"previousProgress", previousProgress,
	)

	// write genesis if we have no data in the stream yet
	if previousProgress == 0 {
		// a quick check that we haven't written anything to the stream yet.  Stage progress is a little misleading
		// for genesis as we are in fact at block 0 here!  Getting the header has some performance overhead, so
		// we only want to do this when we know the previous progress is 0.
		header := srv.GetStreamServer().GetHeader()
		if header.TotalEntries == 0 {
			genesis, err := rawdb.ReadBlockByNumber(tx, 0)
			if err != nil {
				return 0, err
			}
			if err = srv.WriteGenesisToStream(genesis, reader, tx); err != nil {
				return 0, err
			}
		}
	}

	if err = srv.WriteBlocksToStreamConsecutively(ctx, logPrefix, tx, reader, previousProgress+1, finalBlockNumber); err != nil {
		return 0, err
	}

	if err = stages.SaveStageProgress(tx, stages.NonValidationDataStream, finalBlockNumber); err != nil {
		return 0, err
	}

	return finalBlockNumber, nil
}
