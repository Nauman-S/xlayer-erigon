package jsonrpc

import (
	"context"
	"strings"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/filters"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/zk/sequencer"
	"github.com/ledgerwatch/log/v3"
)

// NewPendingTransactionFilter new transaction filter
func (api *APIImpl) NewPendingTransactionFilter(_ context.Context) (string, error) {
	if !sequencer.IsSequencer() {
		return api.newPendingTransactionFilterForXLayer(api.l2RpcUrl)
	}
	if api.filters == nil {
		return "", rpc.ErrNotificationsUnsupported
	}
	txsCh, id := api.filters.SubscribePendingTxs(32)
	go func() {
		for txs := range txsCh {
			api.filters.AddPendingTxs(id, txs)
		}
	}()
	return "0x" + string(id), nil
}

// NewBlockFilter implements eth_newBlockFilter. Creates a filter in the node, to notify when a new block arrives.
func (api *APIImpl) NewBlockFilter(_ context.Context) (string, error) {
	if api.filters == nil {
		return "", rpc.ErrNotificationsUnsupported
	}
	ch, id := api.filters.SubscribeNewHeads(32)
	go func() {
		for block := range ch {
			api.filters.AddPendingBlock(id, block)
		}
	}()
	return "0x" + string(id), nil
}

// NewFilter implements eth_newFilter. Creates an arbitrary filter object, based on filter options, to notify when the state changes (logs).
func (api *APIImpl) NewFilter(_ context.Context, crit filters.FilterCriteria) (string, error) {
	if api.filters == nil {
		return "", rpc.ErrNotificationsUnsupported
	}
	logs, id := api.filters.SubscribeLogs(256, crit)
	go func() {
		for lg := range logs {
			api.filters.AddLogs(id, lg)
		}
	}()
	return "0x" + string(id), nil
}

// UninstallFilter new transaction filter
func (api *APIImpl) UninstallFilter(_ context.Context, index string) (isDeleted bool, err error) {
	if !sequencer.IsSequencer() {
		return api.uninstallFilterForXLayer(api.l2RpcUrl, index)
	}
	if api.filters == nil {
		return false, rpc.ErrNotificationsUnsupported
	}
	// remove 0x
	cutIndex := strings.TrimPrefix(index, "0x")
	if info, ok := api.filters.RegisteredFilters.Load(cutIndex); ok {
		switch info.Type {
		case rpchelper.HeaderFilterType:
			api.filters.UnsubscribeHeads(rpchelper.HeadsSubID(cutIndex))
		case rpchelper.PendingTxFilterType:
			api.filters.UnsubscribePendingTxs(rpchelper.PendingTxsSubID(cutIndex))
		case rpchelper.LogsFilterType:
			api.filters.UnsubscribeLogs(rpchelper.LogsSubID(cutIndex))
		}
		return true, nil
	}

	return false, nil
}

// GetFilterChanges implements eth_getFilterChanges.
// Polling method for a previously-created filter
// returns an array of logs, block headers, or pending transactions which occurred since last poll.
func (api *APIImpl) GetFilterChanges(_ context.Context, index string) ([]any, error) {
	if !sequencer.IsSequencer() {
		return api.getFilterChangesForXLayer(api.l2RpcUrl, index)
	}
	if api.filters == nil {
		return nil, rpc.ErrNotificationsUnsupported
	}

	// remove 0x
	cutIndex := strings.TrimPrefix(index, "0x")
	filter, ok := api.filters.RegisteredFilters.Load(cutIndex)
	if !ok {
		return nil, rpc.ErrSubscriptionNotFound
	}

	// Update to prevent deletion due to expiration
	filter.Update()

	stub := make([]any, 0)
	switch filter.Type {
	case rpchelper.HeaderFilterType:
		if blocks, ok := api.filters.ReadPendingBlocks(rpchelper.HeadsSubID(cutIndex)); ok {
			for _, v := range blocks {
				stub = append(stub, v.Hash())
			}
		}
	case rpchelper.PendingTxFilterType:
		if txs, ok := api.filters.ReadPendingTxs(rpchelper.PendingTxsSubID(cutIndex)); ok {
			for _, tx := range txs {
				stub = append(stub, tx.Hash())
			}
		}
	case rpchelper.LogsFilterType:
		if logs, ok := api.filters.ReadLogs(rpchelper.LogsSubID(cutIndex)); ok {
			for _, v := range logs {
				stub = append(stub, v)
			}
		}
	}
	return stub, nil
}

// GetFilterLogs implements eth_getFilterLogs.
// Polling method for a previously-created filter
// returns an array of logs which occurred since last poll.
func (api *APIImpl) GetFilterLogs(_ context.Context, index string) ([]*types.Log, error) {
	if api.filters == nil {
		return nil, rpc.ErrNotificationsUnsupported
	}
	cutIndex := strings.TrimPrefix(index, "0x")
	logs, ok := api.filters.ReadLogs(rpchelper.LogsSubID(cutIndex))
	if len(logs) == 0 || !ok {
		return []*types.Log{}, nil
	}
	return logs, nil
}

// NewHeads send a notification each time a new (header) block is appended to the chain.
func (api *APIImpl) NewHeads(ctx context.Context) (*rpc.Subscription, error) {
	if api.filters == nil {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}

	rpcSub := notifier.CreateSubscription()

	go func() {
		defer debug.LogPanic()
		headers, id := api.filters.SubscribeNewHeads(32)
		defer api.filters.UnsubscribeHeads(id)
		for {
			select {
			case h, ok := <-headers:
				if h != nil {
					err := notifier.Notify(rpcSub.ID, h)
					if err != nil {
						log.Warn("[rpc] error while notifying subscription", "err", err)
					}
				}
				if !ok {
					log.Warn("[rpc] new heads channel was closed")
					return
				}
			case <-rpcSub.Err():
				return
			}
		}
	}()

	return rpcSub, nil
}

// NewPendingTransactions send a notification each time when a transaction had added into mempool.
func (api *APIImpl) NewPendingTransactions(ctx context.Context, fullTx *bool) (*rpc.Subscription, error) {
	if api.filters == nil {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}

	rpcSub := notifier.CreateSubscription()

	go func() {
		defer debug.LogPanic()
		txsCh, id := api.filters.SubscribePendingTxs(256)
		defer api.filters.UnsubscribePendingTxs(id)

		for {
			select {
			case txs, ok := <-txsCh:
				for _, t := range txs {
					if t != nil {
						var err error
						if fullTx != nil && *fullTx {
							err = notifier.Notify(rpcSub.ID, t)
						} else {
							err = notifier.Notify(rpcSub.ID, t.Hash())
						}

						if err != nil {
							log.Warn("[rpc] error while notifying subscription", "err", err)
						}
					}
				}
				if !ok {
					log.Warn("[rpc] new pending transactions channel was closed")
					return
				}
			case <-rpcSub.Err():
				return
			}
		}
	}()

	return rpcSub, nil
}

// NewPendingTransactionsWithBody send a notification each time when a transaction had added into mempool.
func (api *APIImpl) NewPendingTransactionsWithBody(ctx context.Context) (*rpc.Subscription, error) {
	if api.filters == nil {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}

	rpcSub := notifier.CreateSubscription()

	go func() {
		defer debug.LogPanic()
		txsCh, id := api.filters.SubscribePendingTxs(512)
		defer api.filters.UnsubscribePendingTxs(id)

		for {
			select {
			case txs, ok := <-txsCh:
				for _, t := range txs {
					if t != nil {
						err := notifier.Notify(rpcSub.ID, t)
						if err != nil {
							log.Warn("[rpc] error while notifying subscription", "err", err)
						}
					}
				}
				if !ok {
					log.Warn("[rpc] new pending transactions channel was closed")
					return
				}
			case <-rpcSub.Err():
				return
			}
		}
	}()

	return rpcSub, nil
}

// Logs send a notification each time a new log appears.
func (api *APIImpl) Logs(ctx context.Context, crit filters.FilterCriteria) (*rpc.Subscription, error) {
	if api.filters == nil {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}

	rpcSub := notifier.CreateSubscription()

	go func() {
		defer debug.LogPanic()
		logs, id := api.filters.SubscribeLogs(api.SubscribeLogsChannelSize, crit)
		defer api.filters.UnsubscribeLogs(id)

		for {
			select {
			case h, ok := <-logs:
				if h != nil {
					if h.Topics == nil {
						h.Topics = make([]common.Hash, 0)
					}
					err := notifier.Notify(rpcSub.ID, h)
					if err != nil {
						log.Warn("[rpc] error while notifying subscription", "err", err)
					}
				}
				if !ok {
					log.Warn("[rpc] log channel was closed")
					return
				}
			case <-rpcSub.Err():
				return
			}
		}
	}()

	return rpcSub, nil
}
