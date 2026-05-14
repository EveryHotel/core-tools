package amqp

import (
	"fmt"
	"log/slog"
)

type logger struct {
	level  slog.Level
	prefix string
}

func NewLogger(level slog.Level, prefix string) logger {
	return logger{
		level:  level,
		prefix: fmt.Sprintf("amqp: %s:", prefix),
	}
}

func (l logger) Fatalf(format string, v ...any) {
	if l.level >= slog.LevelError {
		slog.Error(fmt.Sprintf(l.prefix+format, v...))
	}
}

func (l logger) Errorf(format string, v ...any) {
	if l.level <= slog.LevelError {
		slog.Error(fmt.Sprintf(l.prefix+format, v...))
	}
}

func (l logger) Warnf(format string, v ...any) {
	if l.level <= slog.LevelWarn {
		slog.Warn(fmt.Sprintf(l.prefix+format, v...))
	}
}

func (l logger) Infof(format string, v ...any) {
	if l.level <= slog.LevelInfo {
		slog.Info(fmt.Sprintf(l.prefix+format, v...))
	}
}

func (l logger) Debugf(format string, v ...any) {
	if l.level <= slog.LevelDebug {
		slog.Debug(fmt.Sprintf(l.prefix+format, v...))
	}
}

func (l logger) Tracef(format string, v ...any) {
	if l.level <= slog.LevelDebug {
		slog.Debug(fmt.Sprintf(l.prefix+format, v...))
	}
}
