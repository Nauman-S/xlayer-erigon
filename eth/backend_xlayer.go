package eth

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	log2 "github.com/0xPolygonHermez/zkevm-data-streamer/log"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/ledgerwatch/erigon/node"
	"github.com/ledgerwatch/erigon/zk/sequencer"
	zkStages "github.com/ledgerwatch/erigon/zk/stages"
	"github.com/ledgerwatch/log/v3"
)

func (s *Ethereum) initNonValidationStreamServer(stack *node.Node, httpCfg httpcfg.HttpCfg) (err error) {
	if httpCfg.NonValidationDataStreamPort > 0 && httpCfg.NonValidationDataStreamHost != "" {
		if !sequencer.IsSequencer() {
			return fmt.Errorf("Only sequencer can start non validation data stream")
		}
		file := stack.Config().Dirs.DataDir + "/non-validation-data-stream"
		logConfig := &log2.Config{
			Environment: "production",
			Level:       "warn",
			Outputs:     nil,
		}

		// todo [zkevm] read the stream version from config and figure out what system id is used for
		s.nonValidationStreamServer, err = dataStreamServerFactory.CreateStreamServer(uint16(httpCfg.NonValidationDataStreamPort), uint8(s.config.DatastreamVersion), 1, datastreamer.StreamType(1), file, httpCfg.NonValidationDataStreamWriteTimeout, httpCfg.NonValidationDataStreamInactivityTimeout, httpCfg.NonValidationDataStreamInactivityCheckInterval, logConfig)
		if err != nil {
			return err
		}

		// recovery here now, if the stream got into a bad state we want to be able to delete the file and have
		// the stream re-populated from scratch.  So we check the stream for the latest header and if it is
		// 0 we can just set the datastream progress to 0 also which will force a re-population of the stream
		latestHeader := s.nonValidationStreamServer.GetHeader()
		if latestHeader.TotalEntries == 0 {
			log.Info("[dataStream] setting the non validation stream progress to 0")
			s.preStartTasks.WarmUpNonValidationDataStream = true
		}
	}
	return nil
}

func (s *Ethereum) warmUpNonValidationStreamServer() error {
	if s.preStartTasks.WarmUpNonValidationDataStream {
		log.Info("[PreStart] warming up non validation data stream")
		tx, err := s.chainDB.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()

		// we don't know when the server has actually started as it doesn't expose a signal that is has spun up
		// so here we loop and take a brief pause waiting for it to be ready
		attempts := 0
		dataStreamServer := dataStreamServerFactory.CreateDataStreamServer(s.nonValidationStreamServer, s.chainConfig.ChainID.Uint64())
		for {
			_, err = zkStages.CatchupNonValidationDatastream(s.sentryCtx, "non-validation-stream-catchup", tx, dataStreamServer)
			if err != nil {
				if errors.Is(err, datastreamer.ErrAtomicOpNotAllowed) {
					attempts++
					if attempts == 10 {
						return err
					}
					time.Sleep(500 * time.Millisecond)
					continue
				}
				return err
			} else {
				break
			}
		}
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
