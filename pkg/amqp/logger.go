package amqp

import (
	"fmt"
	"log/slog"
)

type logger struct {
	level slog.Level
}

func NewLogger(level slog.Level) logger {
	return logger{level: level}
}

func (l logger) Fatalf(format string, v ...interface{}) {
	if l.level >= slog.LevelError {
		slog.Error(fmt.Sprintf("amqp: "+format, v...))
	}
}

func (l logger) Errorf(format string, v ...interface{}) {
	if l.level <= slog.LevelError {
		slog.Error(fmt.Sprintf("amqp: "+format, v...))
	}
}

func (l logger) Warnf(format string, v ...interface{}) {
	if l.level <= slog.LevelWarn {
		slog.Warn(fmt.Sprintf("amqp: "+format, v...))
	}
}

func (l logger) Infof(format string, v ...interface{}) {
	if l.level <= slog.LevelInfo {
		slog.Info(fmt.Sprintf("amqp: "+format, v...))
	}
}

func (l logger) Debugf(format string, v ...interface{}) {
	if l.level <= slog.LevelDebug {
		slog.Debug(fmt.Sprintf("amqp: "+format, v...))
	}
}

func (l logger) Tracef(format string, v ...interface{}) {
	if l.level <= slog.LevelDebug {
		slog.Debug(fmt.Sprintf("amqp: "+format, v...))
	}
}
