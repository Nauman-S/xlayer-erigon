package common

import "errors"

var ErrKeyNotExist = errors.New("key not exists")
var ErrKeyExist = errors.New("key exists")
var ErrNotFound = errors.New("No matching key/data pair found")
var ErrKeyMismatch = errors.New("given key value is mismatched to the current cursor position")
var ErrValueLeLatest = errors.New("the given value is little or equal to the latest value")
var ErrInvalidIter = errors.New("current iterator is invalid")
