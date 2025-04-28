package combinedb

import (
	"fmt"
	"github.com/ledgerwatch/log/v3"
)

type combineLogger struct {
	enable bool
	prefix string
}

func newCombinLogger(enable bool, prefix string) *combineLogger {
	return &combineLogger{
		enable: enable,
		prefix: prefix,
	}
}

func (cl *combineLogger) getPrefix() string {
	return cl.prefix
}

func (cl *combineLogger) isEnable() bool {
	return cl.enable
}

func (cl *combineLogger) Fatal(msg string, args ...interface{}) {
	args = append([]interface{}{"msg", msg}, args...)
	log.Error(fmt.Sprintf("[GID %d] %s", GoID(), cl.prefix), args...)
	panic("fatal error")
}

func (cl *combineLogger) Fatalf(format string, args ...interface{}) {
	cl.Fatal(fmt.Sprintf(format, args...))
}

func (cl *combineLogger) Error(msg string, args ...interface{}) {
	args = append([]interface{}{"msg", msg}, args...)
	log.Error(fmt.Sprintf("[GID %d] %s", GoID(), cl.prefix), args...)
}

func (cl *combineLogger) Errorf(format string, args ...interface{}) {
	cl.Error(fmt.Sprintf(format, args...))
}

func (cl *combineLogger) Warn(msg string, args ...interface{}) {
	args = append([]interface{}{"msg", msg}, args...)
	log.Warn(fmt.Sprintf("[GID %d] %s", GoID(), cl.prefix), args...)
}

func (cl *combineLogger) Warnf(format string, args ...interface{}) {
	cl.Warn(fmt.Sprintf(format, args...))
}

func (cl *combineLogger) Info(msg string, args ...interface{}) {
	if cl.enable {
		args = append([]interface{}{"msg", msg}, args...)
		log.Info(fmt.Sprintf("[GID %d] %s", GoID(), cl.prefix), args...)
	}
}

func (cl *combineLogger) Infof(format string, args ...interface{}) {
	cl.Info(fmt.Sprintf(format, args...))
}

func (cl *combineLogger) Debug(msg string, args ...interface{}) {
	if cl.enable {
		args = append([]interface{}{"msg", msg}, args...)
		log.Debug(fmt.Sprintf("[GID %d] %s", GoID(), cl.prefix), args...)
	}
}

func (cl *combineLogger) Debugf(format string, args ...interface{}) {
	cl.Debug(fmt.Sprintf(format, args...))
}

func (cl *combineLogger) Trace(msg string, args ...interface{}) {
	if cl.enable {
		args = append([]interface{}{"msg", msg}, args...)
		log.Trace(fmt.Sprintf("[GID %d] %s", GoID(), cl.prefix), args...)
	}
}

func (cl *combineLogger) Tracef(msg string, args ...interface{}) {
	cl.Trace(fmt.Sprintf(msg, args...))
}
