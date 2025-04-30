package txpool

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/u256"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon-lib/kv/temporal/temporaltest"
	"github.com/ledgerwatch/erigon-lib/txpool/txpoolcfg"
	"github.com/ledgerwatch/erigon-lib/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test that the OkPay transactions are prioritized first, without monopolizing the
// entire block. OkPay transcations are included into the block first based on the
// configured block gas limit.
func TestAddLocalTxsWithOkPayTxs(t *testing.T) {
	assert, require := assert.New(t), require.New(t)
	ch := make(chan types.Announcements, 100)
	_, coreDB, _ := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	defer coreDB.Close()

	db := memdb.NewTestPoolDB(t)
	path := fmt.Sprintf("/tmp/db-test-%v", time.Now().UTC().Format(time.RFC3339Nano))
	txPoolDB := newTestTxPoolDB(t, path)
	defer txPoolDB.Close()
	aclsDB := newTestACLDB(t, path)
	defer aclsDB.Close()

	// Check if the dbs are created.
	require.NotNil(t, db)
	require.NotNil(t, txPoolDB)
	require.NotNil(t, aclsDB)

	cfg := txpoolcfg.DefaultConfig
	ethCfg := &ethconfig.Defaults
	sendersCache := kvcache.New(kvcache.DefaultCoherentConfig)

	// Create OkPay addresses for testing
	var okPayAddresses []common.Address
	for i := 0; i < 10; i++ {
		addr := common.HexToAddress(fmt.Sprintf("0x%x", i))
		okPayAddresses = append(okPayAddresses, addr)
	}

	// Set OkPay addresses and block gas limit configs
	for _, addr := range okPayAddresses {
		ethCfg.DeprecatedTxPool.OkPayAccountsList.Add(addr)
	}
	// Default intrinsic gas for normal transactions is 21_000.
	// We set the block gas limit to be 21_000 * 10 = 210_000
	ethCfg.DeprecatedTxPool.OkPayBlockGasLimit = 210_000

	// Set 100 normal addresses for testing
	var normalAddresses []common.Address
	for i := 0; i < 100; i++ {
		addr := common.HexToAddress(fmt.Sprintf("0x%x", i+10))
		normalAddresses = append(normalAddresses, addr)
	}

	// Create a new txpool
	pool, err := New(ch, coreDB, cfg, ethCfg, sendersCache, *u256.N1, nil, nil, aclsDB)
	assert.NoError(err)
	require.True(pool != nil)
	ctx := context.Background()
	var stateVersionID uint64 = 0
	pendingBaseFee := uint64(200000)
	h1 := gointerfaces.ConvertHashToH256([32]byte{})

	change := &remote.StateChangeBatch{
		StateVersionId:      stateVersionID,
		PendingBlockBaseFee: pendingBaseFee,
		BlockGasLimit:       1000000,
		ChangeBatch: []*remote.StateChange{
			{BlockHeight: 0, BlockHash: h1},
		},
	}

	// Fund all addresses with 18 Ether for sending transactions
	v := make([]byte, types.EncodeSenderLengthForStorage(0, *uint256.NewInt(18 * common.Ether)))
	types.EncodeSender(0, *uint256.NewInt(18 * common.Ether), v)

	for _, addr := range okPayAddresses {
		change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remote.AccountChange{
			Action:  remote.Action_UPSERT,
			Address: gointerfaces.ConvertAddressToH160(addr),
			Data:    v,
		})
	}

	for _, addr := range normalAddresses {
		change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remote.AccountChange{
			Action:  remote.Action_UPSERT,
			Address: gointerfaces.ConvertAddressToH160(addr),
			Data:    v,
		})
	}
	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()
	err = pool.OnNewBlock(ctx, change, types.TxSlots{}, types.TxSlots{}, tx)
	assert.NoError(err)

	// Spam the pool and add 100 normal transactions
	var normalTxSlots types.TxSlots
	for i := 0; i < 100; i++ {
		txSlot := &types.TxSlot{
			Rlp:    []byte{byte(i)},
			Tip:    *uint256.NewInt(300000),
			FeeCap: *uint256.NewInt(1000000000),
			Gas:    21_000,
			Nonce:  0,
		}
		txSlot.IDHash[0] = byte(i)
		normalTxSlots.Append(txSlot, normalAddresses[i][:], true)
	}
	reasons, err := pool.AddLocalTxs(ctx, normalTxSlots, tx)
	assert.NoError(err)
	for _, reason := range reasons {
		assert.Equal(Success, reason, reason.String())
	}

	// Add 10 mock OkPay transactions to the pool
	var okPayTxSlots types.TxSlots
	for i := 0; i < 10; i++ {
		txSlot := &types.TxSlot{
			Rlp:    []byte{byte(i + 100)},
			Tip:    *uint256.NewInt(300000),
			FeeCap: *uint256.NewInt(1000000000),
			Gas:    21_000,
			Nonce:  0,
		}
		txSlot.IDHash[0] = byte(i + 100)
		okPayTxSlots.Append(txSlot, okPayAddresses[i][:], true)
	}
	reasons, err = pool.AddLocalTxs(ctx, okPayTxSlots, tx)
	assert.NoError(err)
	for _, reason := range reasons {
		assert.Equal(Success, reason, reason.String())
	}

	// Limit the available block gas limit to 315_000, so that only 15 transactions can be included
	// since every transaction uses 21_000 intrinsic gas.
	slots := types.TxsRlp{}
	allConditionsOk, count, err := pool.YieldBest(20, &slots, tx, 0, 15*21_000, 0, mapset.NewSet[[32]byte]())
	assert.NoError(err)
	assert.True(allConditionsOk)

	// Check that 15 transactions are yielded, and normal transactions are included as well
	assert.Equal(15, count)

	// Check only all the OkPay transactions were included
	okPayCount := 0
	for _, rlpTx := range slots.Txs {
		for _, okPayTx := range okPayTxSlots.Txs {
			if bytes.Equal(rlpTx, okPayTx.Rlp) {
				okPayCount++
			}
		}
	}
	assert.Equal(10, okPayCount)
}
