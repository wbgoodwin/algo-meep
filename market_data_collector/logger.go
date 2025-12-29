package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// LogLevel represents the logging level
type LogLevel string

const (
	LogLevelInfo  LogLevel = "INFO"
	LogLevelError LogLevel = "ERROR"
	LogLevelDebug LogLevel = "DEBUG"
)

// LogEntry represents a structured log entry
type LogEntry struct {
	Timestamp string `json:"timestamp"`
	Level     string `json:"level"`
	Message   string `json:"message"`
	Symbol    string `json:"symbol,omitempty"`
	Function  string `json:"function,omitempty"`
	Error     string `json:"error,omitempty"`
	S3Bucket  string `json:"s3_bucket,omitempty"`
	S3Key     string `json:"s3_key,omitempty"`
}

// Logger provides structured JSON logging
type Logger struct {
	prefix string
}

// NewLogger creates a new logger instance
func NewLogger(prefix string) *Logger {
	return &Logger{prefix: prefix}
}

// log writes a structured log entry
func (l *Logger) log(level LogLevel, message string, fields ...func(*LogEntry)) {
	entry := &LogEntry{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Level:     string(level),
		Message:   fmt.Sprintf("[%s] %s", l.prefix, message),
	}

	for _, field := range fields {
		field(entry)
	}

	jsonBytes, err := json.Marshal(entry)
	if err != nil {
		log.Printf("Failed to marshal log entry: %v", err)
		return
	}

	fmt.Println(string(jsonBytes))
}

// Info logs an info message
func (l *Logger) Info(message string, fields ...func(*LogEntry)) {
	l.log(LogLevelInfo, message, fields...)
}

// Error logs an error message
func (l *Logger) Error(message string, err error, fields ...func(*LogEntry)) {
	l.log(LogLevelError, message, append(fields, func(e *LogEntry) {
		if err != nil {
			e.Error = err.Error()
		}
	})...)
}

// Debug logs a debug message
func (l *Logger) Debug(message string, fields ...func(*LogEntry)) {
	l.log(LogLevelDebug, message, fields...)
}

// WithSymbol adds a symbol field to the log entry
func WithSymbol(symbol string) func(*LogEntry) {
	return func(e *LogEntry) {
		e.Symbol = symbol
	}
}

// WithFunction adds a function field to the log entry
func WithFunction(function string) func(*LogEntry) {
	return func(e *LogEntry) {
		e.Function = function
	}
}

// WithS3Info adds S3 bucket and key fields to the log entry
func WithS3Info(bucket, key string) func(*LogEntry) {
	return func(e *LogEntry) {
		e.S3Bucket = bucket
		e.S3Key = key
	}
}

// Global logger instance
var logger = NewLogger("MARKET_DATA_COLLECTOR")
