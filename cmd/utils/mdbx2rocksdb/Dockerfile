FROM docker.io/library/golang:1.24-alpine3.21 AS builder

RUN apk --no-cache add build-base linux-headers git bash cmake ca-certificates libstdc++ libc-dev gcc lz4-dev zstd-dev gflags-dev snappy-dev zlib-dev


WORKDIR /app

ADD deps deps
ENV EXTRA_CFLAGS="-Wno-error=maybe-uninitialized -Wno-error=uninitialized" EXTRA_CXXFLAGS="-Wno-error=maybe-uninitialized -Wno-error=uninitialized"
RUN mkdir deps/rocksdb/build && cd deps/rocksdb/build && \
    cmake -DCMAKE_BUILD_TYPE=Release -DWITH_LZ4=ON -DWITH_ZSTD=ON -DWITH_ZLIB=OFF -DWITH_SNAPPY=OFF -DROCKSDB_BUILD_SHARED=OFF .. && \
    make rocksdb -j8 && \
    make install

ADD go.mod go.mod
ADD go.sum go.sum
ADD erigon-lib/go.mod erigon-lib/go.mod
ADD erigon-lib/go.sum erigon-lib/go.sum

RUN go mod download
ADD . .

RUN go build -o mdbx2rocksdb /app/cmd/utils/mdbx2rocksdb/main.go

FROM docker.io/library/alpine:3.17

RUN apk add --no-cache ca-certificates libstdc++ lz4-dev zstd-dev

WORKDIR /home/mdbx2rocksdb

COPY --from=builder /app/mdbx2rocksdb /usr/local/bin/mdbx2rocksdb

ENTRYPOINT ["mdbx2rocksdb"]
CMD ["--help"]
