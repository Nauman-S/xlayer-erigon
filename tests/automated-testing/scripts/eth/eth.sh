#! /bin/sh

BASE_URL="https://xlayerrpc.okx.com"

methods=(
    "blockNumber"
    "getBlockByNumber"
    "getBlockByHash"
    "getBlockTransactionCountByNumber"
    "getBlockTransactionCountByHash"
    "getTransactionByHash"
    "getTransactionByBlockHashAndIndex"
    "getTransactionByBlockNumberAndIndex"
    "getRawTransactionByBlockNumberAndIndex"
    "getRawTransactionByBlockHashAndIndex"
    "getRawTransactionByHash"
)

for method in "${methods[@]}"
do
    method_name="eth_$method"  # Prepend "eth_" to the core method name
    #wrk -t 16 -c 5000 -d 60s -T 30s -s "$method_name.lua" "$BASE_URL"
    wrk -t 2 -c 10 -d 10s -T 10s -s "$method_name.lua" "$BASE_URL"
    printf "\n"
done