package engine

import (
	"log"
	"os"
)

// Logger 是编排层使用的最小日志接口；测试可注入捕获型 logger 断言关键输入与判定依据。
type Logger interface {
	Printf(format string, args ...any)
}

type stdLogger struct{ l *log.Logger }

// NewStdLogger 输出到标准错误，带时间戳。
func NewStdLogger() Logger {
	return &stdLogger{l: log.New(os.Stderr, "[orch] ", log.LstdFlags|log.Lmicroseconds)}
}

func (s *stdLogger) Printf(format string, args ...any) { s.l.Printf(format, args...) }

type nopLogger struct{}

func (nopLogger) Printf(format string, args ...any) {}

// NopLogger 丢弃日志。
func NopLogger() Logger { return nopLogger{} }
