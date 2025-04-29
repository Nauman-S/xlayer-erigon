package jsonrpc

import (
	"encoding/json"
	"fmt"
	"github.com/ledgerwatch/erigon/zkevm/jsonrpc/client"
	"strconv"
	"strings"
)

// NewPendingTransactionFilter new transaction filter
func (api *APIImpl) newPendingTransactionFilterForXLayer(rpcUrl string) (string, error) {
	res, err := client.JSONRPCCall(rpcUrl, "eth_newPendingTransactionFilter")
	if err != nil {
		return "", err
	}
	if res.Error != nil {
		return "", fmt.Errorf("RPC error response: %s", res.Error.Message)
	}
	// id comes in escaped quotes, so we trim them here
	// "\"0x100000000000000c727cc02e7ff67b3\"" -> "0x100000000000000c727cc02e7ff67b3"
	id := strings.Trim(string(res.Result), "\"")
	return id, nil
}

func (api *APIImpl) getFilterChangesForXLayer(rpcUrl string, index string) ([]any, error) {
	res, err := client.JSONRPCCall(rpcUrl, "eth_getFilterChanges", index)
	if err != nil {
		return nil, err
	}
	if res.Error != nil {
		return nil, fmt.Errorf("RPC error response: %s", res.Error.Message)
	}

	hashes := make([]any, 0)
	if err = json.Unmarshal(res.Result, &hashes); err != nil {
		return nil, err
	}

	return hashes, nil
}

func (api *APIImpl) uninstallFilterForXLayer(rpcUrl string, index string) (isDeleted bool, err error) {
	res, err := client.JSONRPCCall(rpcUrl, "eth_uninstallFilter", index)
	if err != nil {
		return false, err
	}

	if res.Error != nil {
		return false, fmt.Errorf("RPC error response: %s", res.Error.Message)
	}
	return strconv.ParseBool(string(res.Result))
}
