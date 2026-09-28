package taskkit

import (
	"os"
	"strings"

	"github.com/go-sdk/core/logx"
)

const schedulerLogEnvironment = "TASKKIT_GOCRON_LOG"

func schedulerLogEnabled() bool {
	switch strings.ToLower(os.Getenv(schedulerLogEnvironment)) {
	case "1", "t", "true", "y", "yes", "on":
		return true
	}
	return false
}

type schedulerLogger struct{}

func (schedulerLogger) Debug(message string, fields ...any) {
	l := logx.Debug()
	if len(fields) == 0 {
		l.Msg(message)
	} else {
		l.Interface("fields", fields).Msg(message)
	}
}

func (schedulerLogger) Info(message string, fields ...any) {
	l := logx.Info()
	if len(fields) == 0 {
		l.Msg(message)
	} else {
		l.Interface("fields", fields).Msg(message)
	}
}

func (schedulerLogger) Warn(message string, fields ...any) {
	l := logx.Warn()
	if len(fields) == 0 {
		l.Msg(message)
	} else {
		l.Interface("fields", fields).Msg(message)
	}
}

func (schedulerLogger) Error(message string, fields ...any) {
	l := logx.Error()
	if len(fields) == 0 {
		l.Msg(message)
	} else {
		l.Interface("fields", fields).Msg(message)
	}
}
