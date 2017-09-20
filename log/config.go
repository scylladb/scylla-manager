package log

import (
	"go.uber.org/zap"
)

// NewDevelopment creates a new logger that writes DebugLevel and above
// logs to standard error in a human-friendly format.
func NewDevelopment() Logger {
	l, _ := zap.NewDevelopment()
	return Logger{base: l}
}
