package combinedb

import (
	"bytes"
	"runtime"
	"strconv"
)

func copyBytes(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
func GoID() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	// Stack 形如： "goroutine 12345 [running]:\n"
	fields := bytes.Fields(buf[:n])
	id, _ := strconv.ParseInt(string(fields[1]), 10, 64)
	return id
}
