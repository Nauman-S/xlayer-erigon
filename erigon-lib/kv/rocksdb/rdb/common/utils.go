package common

import (
	"fmt"
	"github.com/linxGnu/grocksdb"
	"math"
	"sort"
)

func MoveSliceToBytes(s *grocksdb.Slice) []byte {
	defer s.Free()
	if !s.Exists() {
		return nil
	}
	if len(s.Data()) == 0 {
		return nil
	}

	v := make([]byte, len(s.Data()))
	copy(v, s.Data())
	return v
}

func MergeKey(table string, k []byte) []byte {
	l := len(table)
	if l > math.MaxUint8 {
		panic(fmt.Sprintf("too large table len: [%d]%s", l, table))
	}
	lb := byte(l)

	buf := make([]byte, 0, 1+l+len(k))
	buf = append(buf, lb)
	buf = append(buf, []byte(table)...)
	buf = append(buf, k...)

	return buf
}

func SplitKey(k []byte) (string, []byte) {
	if len(k) < 1 {
		panic("invalid key with length < 1")
	}

	l := k[0]

	table := string(k[1 : l+1])
	k = k[l+1:]
	return table, k
}

// SortedSeek seek the first index that makes slice[index] >= v.
// `compare` compares the two given values and return if the first one >= the second one.
// if it seeked an appropriate position, it returns the index and ok,
// if it doesn't find an appropriate position, it returns length of the values slice and false.
func SortedSeek[V any](slice []V, seekV V, compare func(V, V) bool) (int, bool) {
	idx := sort.Search(len(slice), func(i int) bool {
		return compare(slice[i], seekV)
	})
	if idx < 0 {
		panic(fmt.Sprintf("dbv.sortedSeek: invalid index:%d", idx))
	}
	if idx >= len(slice) {
		return idx, false
	}

	return idx, true
}
