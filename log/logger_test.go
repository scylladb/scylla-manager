package log

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"go.uber.org/zap/zaptest/observer"
)

func TestLogger(t *testing.T) {
	table := []struct {
		msg       string
		expectMsg string
	}{
		{"foo", "foo"},
		{"", ""},
	}

	// Common to all test cases.
	context := []interface{}{"foo", "bar"}
	extra := []interface{}{"baz", false}
	expectedFields := []zapcore.Field{zap.String("foo", "bar"), zap.Bool("baz", false)}

	for _, test := range table {
		withLogger(zap.DebugLevel, nil, func(logger Logger, logs *observer.ObservedLogs) {
			logger.With(context...).Debug(test.msg, extra...)
			logger.With(context...).Info(test.msg, extra...)
			logger.With(context...).Error(test.msg, extra...)

			expected := make([]observer.LoggedEntry, 3)
			for i, lvl := range []zapcore.Level{zap.DebugLevel, zap.InfoLevel, zap.ErrorLevel} {
				expected[i] = observer.LoggedEntry{
					Entry:   zapcore.Entry{Message: test.expectMsg, Level: lvl},
					Context: expectedFields,
				}
			}
			if diff := cmp.Diff(expected, logs.AllUntimed()); diff != "" {
				t.Error(diff)
			}
		})
	}
}

func withLogger(e zapcore.LevelEnabler, opts []zap.Option, f func(Logger, *observer.ObservedLogs)) {
	fac, logs := observer.New(e)
	logger := zap.New(fac, opts...)
	f(Logger{base: logger}, logs)
}

func newZapLogger() *zap.Logger {
	cfg := zap.NewProductionConfig()
	enc := zapcore.NewJSONEncoder(cfg.EncoderConfig)
	return zap.New(zapcore.NewCore(
		enc,
		&zaptest.Discarder{},
		zapcore.DebugLevel,
	))
}

func BenchmarkZap(b *testing.B) {
	l := newZapLogger()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Debug("log message", zap.Int("key0", 0), zap.String("key1", "key1"), zap.String("key2", "key2"))
	}
}

func BenchmarkZapSugared(b *testing.B) {
	l := newZapLogger().Sugar()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Debugw("log message", "key0", 0, "key1", "key1", "key2", "key2")
	}
}

func BenchmarkLogger(b *testing.B) {
	l := Logger{newZapLogger()}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Debug("log message", "key0", 0, "key1", "key1", "key2", "key2")
	}
}
