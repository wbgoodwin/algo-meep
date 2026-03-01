package middleware

import (
	"testing"
	"time"
)

func TestNewLogger(t *testing.T) {
	logger := NewLogger("TEST")
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}
	if logger.prefix != "TEST" {
		t.Errorf("expected prefix 'TEST', got '%s'", logger.prefix)
	}
}

func TestWithFunction(t *testing.T) {
	entry := &LogEntry{}
	WithFunction("myHandler")(entry)
	if entry.Function != "myHandler" {
		t.Errorf("expected function 'myHandler', got '%s'", entry.Function)
	}
}

func TestWithUserID(t *testing.T) {
	entry := &LogEntry{}
	WithUserID("user_123")(entry)
	if entry.UserID != "user_123" {
		t.Errorf("expected user_id 'user_123', got '%s'", entry.UserID)
	}
}

func TestWithRoute(t *testing.T) {
	entry := &LogEntry{}
	WithRoute("POST /bank/enroll")(entry)
	if entry.Route != "POST /bank/enroll" {
		t.Errorf("expected route 'POST /bank/enroll', got '%s'", entry.Route)
	}
}

func TestWithDuration(t *testing.T) {
	entry := &LogEntry{}
	d := 150 * time.Millisecond
	WithDuration(d)(entry)
	if entry.Duration != d.String() {
		t.Errorf("expected duration '%s', got '%s'", d.String(), entry.Duration)
	}
}

func TestWithField(t *testing.T) {
	entry := &LogEntry{}
	WithField("count", "42")(entry)
	if entry.Fields == nil {
		t.Fatal("expected non-nil fields map")
	}
	if entry.Fields["count"] != "42" {
		t.Errorf("expected field count='42', got '%s'", entry.Fields["count"])
	}
}

func TestWithField_Multiple(t *testing.T) {
	entry := &LogEntry{}
	WithField("a", "1")(entry)
	WithField("b", "2")(entry)
	if len(entry.Fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(entry.Fields))
	}
	if entry.Fields["a"] != "1" || entry.Fields["b"] != "2" {
		t.Errorf("unexpected fields: %v", entry.Fields)
	}
}

func TestLoggerMethods_DontPanic(t *testing.T) {
	// Just verify these don't panic — they write to stdout
	logger := NewLogger("TEST")
	logger.Info("test info", WithFunction("test"))
	logger.Warn("test warn")
	logger.Debug("test debug", WithField("key", "val"))
	logger.Error("test error", nil)
	logger.Error("test error with cause", &testError{msg: "boom"})
}

func TestLoggerError_NilErr(t *testing.T) {
	logger := NewLogger("TEST")
	// Error with nil error should not set the Error field
	logger.Error("no error", nil)
}

func TestLoggerError_WithErr(t *testing.T) {
	logger := NewLogger("TEST")
	logger.Error("has error", &testError{msg: "something failed"})
}

func TestLogger_AllFields(t *testing.T) {
	logger := NewLogger("TEST")
	logger.Info("full entry",
		WithFunction("handler"),
		WithUserID("u123"),
		WithRoute("GET /test"),
		WithDuration(100*time.Millisecond),
		WithField("extra", "data"),
	)
}

type testError struct{ msg string }

func (e *testError) Error() string { return e.msg }
