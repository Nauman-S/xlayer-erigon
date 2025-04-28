#!/bin/bash

set -e

if [ $# -ne 3 ]; then
  echo "usage: $0 mdbx_dir rocksdb_dir"
  exit 1
fi

MDBX_ABSOLUTE=$(cd "$1" && pwd)
ROCKSDB_ABSOLUTE=$(cd "$2" && pwd)
MDBX_LABEL="$3"

echo "mdbx database dir: $MDBX_ABSOLUTE"
echo "rocksdb database dir: $ROCKSDB_ABSOLUTE"

SCRIPT_DIR=$(pwd)

cd ../../../
docker build -t mdbx2rocksdb -f cmd/utils/mdbx2rocksdb/Dockerfile .

cd "$SCRIPT_DIR"

docker run --rm -p "7070:6060" -p "7071:6061" -v "$MDBX_ABSOLUTE:/mdbx_data" -v "$ROCKSDB_ABSOLUTE:/rocksdb_data" mdbx2rocksdb --mdbx /mdbx_data --rocksdb /rocksdb_data --verbose --label "$MDBX_LABEL"