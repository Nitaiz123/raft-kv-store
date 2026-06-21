// Package logger provides a simple structured logger for Raft components.
package logger

import (
	"fmt"
	"log"
	"os"
)

// Logger wraps the standard library logger with component and node context.
type Logger struct {
	prefix string
	inner  *log.Logger
}

// New creates a new Logger with the given component name and node ID.
func New(component string, nodeID int) *Logger {
	prefix := fmt.Sprintf("[%s][node-%d] ", component, nodeID)
	return &Logger{
		prefix: prefix,
		inner:  log.New(os.Stdout, prefix, log.LstdFlags|log.Lmicroseconds),
	}
}

// Infof logs an informational message.
func (l *Logger) Infof(format string, args ...interface{}) {
	l.inner.Printf("INFO  "+format, args...)
}

// Errorf logs an error message.
func (l *Logger) Errorf(format string, args ...interface{}) {
	l.inner.Printf("ERROR "+format, args...)
}

// Debugf logs a debug message.
func (l *Logger) Debugf(format string, args ...interface{}) {
	l.inner.Printf("DEBUG "+format, args...)
}
