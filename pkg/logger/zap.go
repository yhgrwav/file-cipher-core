package logger

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// DEBUG
// INFO
// WARN
// ERROR

type Logger interface {
	Info(message string, args ...any)
	Warn(message string, args ...any)
	Debug(message string, args ...any)
	Error(message string, args ...any)
	Fatal(message string, args ...any)
}

type ZapLogger struct {
	logger *zap.SugaredLogger
}

func (l ZapLogger) Info(message string, args ...any) {
	l.logger.Infow(message, args...)
}

func (l ZapLogger) Warn(message string, args ...any) {
	l.logger.Warnw(message, args...)
}

func (l ZapLogger) Debug(message string, args ...any) {
	l.logger.Debugw(message, args...)
}

func (l ZapLogger) Error(message string, args ...any) {
	l.logger.Errorw(message, args...)
}

func (l ZapLogger) Fatal(message string, args ...any) {
	l.logger.Fatalw(message, args...)
}

// NewNop returns a Logger that discards everything. Intended for tests.
func NewNop() Logger {
	return ZapLogger{logger: zap.NewNop().Sugar()}
}

func NewLogger(logLevel string) (Logger, func() error, error) {
	lvl := zap.NewAtomicLevel()
	if err := lvl.UnmarshalText([]byte(logLevel)); err != nil {
		return nil, nil, fmt.Errorf("unmarshal log level: %w", err)
	}

	// 7           5              5
	// user        user-group     other
	// r w x       r w x          r w x
	// 1 1 1       1 0 1          1 0 1
	if err := os.MkdirAll("logs", 0755); err != nil {
		return nil, nil, fmt.Errorf("mkdir log folder: %w", err)
	}

	timestamp := time.Now().UTC().Format("2006-01-02T15-04-05.000000")
	logFilePath := filepath.Join("logs", fmt.Sprintf("%s.log", timestamp))

	// 6           4              4
	// user        user-group     other
	// r w x       r w x          r w x
	// 1 1 0       1 0 0          1 0 0
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, nil, fmt.Errorf("open log file: %w", err)
	}

	cfg := zap.NewDevelopmentEncoderConfig()
	cfg.EncodeTime = zapcore.TimeEncoderOfLayout("2006-01-02T15:04:05.000000")

	encoder := zapcore.NewConsoleEncoder(cfg)

	core := zapcore.NewTee(
		zapcore.NewCore(encoder, zapcore.AddSync(os.Stdout), lvl),
		zapcore.NewCore(encoder, zapcore.AddSync(logFile), lvl),
	)

	logger := zap.New(
		core,
		zap.AddCaller(),
		zap.AddStacktrace(zapcore.ErrorLevel),
	)

	return ZapLogger{logger: logger.Sugar()}, logFile.Close, nil
}
