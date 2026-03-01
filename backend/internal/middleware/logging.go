package middleware

import (
	"encoding/json"
	"fmt"
	"time"
)

type LogLevel string

const (
	LogLevelInfo  LogLevel = "INFO"
	LogLevelWarn  LogLevel = "WARN"
	LogLevelError LogLevel = "ERROR"
	LogLevelDebug LogLevel = "DEBUG"
)

type LogEntry struct {
	Timestamp string            `json:"timestamp"`
	Level     string            `json:"level"`
	Message   string            `json:"message"`
	Function  string            `json:"function,omitempty"`
	UserID    string            `json:"user_id,omitempty"`
	Route     string            `json:"route,omitempty"`
	Duration  string            `json:"duration,omitempty"`
	Error     string            `json:"error,omitempty"`
	Fields    map[string]string `json:"fields,omitempty"`
}

type Logger struct {
	prefix string
}

func NewLogger(prefix string) *Logger {
	return &Logger{prefix: prefix}
}

func (l *Logger) log(level LogLevel, message string, fields ...func(*LogEntry)) {
	entry := &LogEntry{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Level:     string(level),
		Message:   fmt.Sprintf("[%s] %s", l.prefix, message),
	}
	for _, f := range fields {
		f(entry)
	}
	b, err := json.Marshal(entry)
	if err != nil {
		fmt.Printf(`{"level":"ERROR","message":"failed to marshal log: %v"}`+"\n", err)
		return
	}
	fmt.Println(string(b))
}

func (l *Logger) Info(message string, fields ...func(*LogEntry)) {
	l.log(LogLevelInfo, message, fields...)
}

func (l *Logger) Warn(message string, fields ...func(*LogEntry)) {
	l.log(LogLevelWarn, message, fields...)
}

func (l *Logger) Error(message string, err error, fields ...func(*LogEntry)) {
	l.log(LogLevelError, message, append(fields, func(e *LogEntry) {
		if err != nil {
			e.Error = err.Error()
		}
	})...)
}

func (l *Logger) Debug(message string, fields ...func(*LogEntry)) {
	l.log(LogLevelDebug, message, fields...)
}

// Field helpers — never log PII, access tokens, or financial data.

func WithFunction(fn string) func(*LogEntry) {
	return func(e *LogEntry) { e.Function = fn }
}

func WithUserID(id string) func(*LogEntry) {
	return func(e *LogEntry) { e.UserID = id }
}

func WithRoute(route string) func(*LogEntry) {
	return func(e *LogEntry) { e.Route = route }
}

func WithDuration(d time.Duration) func(*LogEntry) {
	return func(e *LogEntry) { e.Duration = d.String() }
}

func WithField(key, value string) func(*LogEntry) {
	return func(e *LogEntry) {
		if e.Fields == nil {
			e.Fields = make(map[string]string)
		}
		e.Fields[key] = value
	}
}
