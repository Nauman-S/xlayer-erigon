package combinedb

import (
	"bytes"
	"reflect"
)

func assertError(logger *combineLogger, mdbxErr, rocksdbErr error, funcName string) error {
	if mdbxErr != nil && rocksdbErr != nil {
		logger.Errorf("%s() err. mdbx err=%v. rocksdb err=%v", funcName, mdbxErr, rocksdbErr)
		return rocksdbErr
	}
	if mdbxErr != nil || rocksdbErr != nil {
		logger.Fatalf("%s() single error. mdbx err=%v. rocksdb err=%v", funcName, mdbxErr, rocksdbErr)
		return rocksdbErr
	}
	return nil
}

func assertEqualF(logger *combineLogger, expected, actual interface{}, format string, msgAndArgs ...interface{}) {
	if !isObjectEqual(expected, actual) {
		logger.Fatalf(format, msgAndArgs...)
	}
}

func isObjectEqual(expected, actual interface{}) bool {
	if expected == nil || actual == nil {
		return expected == actual
	}

	exp, ok := expected.([]byte)
	if !ok {
		return reflect.DeepEqual(expected, actual)
	}

	act, ok := actual.([]byte)
	if !ok {
		return false
	}
	if exp == nil || act == nil {
		return exp == nil && act == nil
	}
	return bytes.Equal(exp, act)
}
