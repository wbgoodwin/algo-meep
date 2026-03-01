package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"
)

func TestNewBadRequest(t *testing.T) {
	err := NewBadRequest("invalid input")
	if err.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, err.StatusCode)
	}
	if err.Code != ErrCodeBadRequest {
		t.Errorf("expected code %s, got %s", ErrCodeBadRequest, err.Code)
	}
	if err.Message != "invalid input" {
		t.Errorf("expected message 'invalid input', got '%s'", err.Message)
	}
}

func TestNewUnauthorized(t *testing.T) {
	err := NewUnauthorized("bad token")
	if err.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected status %d, got %d", http.StatusUnauthorized, err.StatusCode)
	}
	if err.Code != ErrCodeUnauthorized {
		t.Errorf("expected code %s, got %s", ErrCodeUnauthorized, err.Code)
	}
}

func TestNewInternal(t *testing.T) {
	underlying := errors.New("db connection failed")
	err := NewInternal("something broke", underlying)
	if err.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected status %d, got %d", http.StatusInternalServerError, err.StatusCode)
	}
	if err.Err != underlying {
		t.Error("expected underlying error to be preserved")
	}
	// Unwrap should return the underlying error
	if !errors.Is(err, underlying) {
		t.Error("errors.Is should match underlying error")
	}
}

func TestNewProviderError(t *testing.T) {
	err := NewProviderError("teller down", errors.New("timeout"))
	if err.StatusCode != http.StatusBadGateway {
		t.Errorf("expected status %d, got %d", http.StatusBadGateway, err.StatusCode)
	}
	if err.Code != ErrCodeProviderError {
		t.Errorf("expected code %s, got %s", ErrCodeProviderError, err.Code)
	}
}

func TestNewRateLimited(t *testing.T) {
	err := NewRateLimited()
	if err.StatusCode != http.StatusTooManyRequests {
		t.Errorf("expected status %d, got %d", http.StatusTooManyRequests, err.StatusCode)
	}
}

func TestAppError_Error(t *testing.T) {
	// Without underlying error
	err := NewBadRequest("missing field")
	got := err.Error()
	if got != "BAD_REQUEST: missing field" {
		t.Errorf("expected 'BAD_REQUEST: missing field', got '%s'", got)
	}

	// With underlying error
	err2 := NewInternal("db error", errors.New("conn refused"))
	got2 := err2.Error()
	if got2 != "INTERNAL_ERROR: db error: conn refused" {
		t.Errorf("unexpected error string: %s", got2)
	}
}

func TestAppError_ToAPIResponse(t *testing.T) {
	err := NewNotFound("user not found")
	status, body := err.ToAPIResponse()

	if status != http.StatusNotFound {
		t.Errorf("expected status %d, got %d", http.StatusNotFound, status)
	}

	var resp APIResponse
	if jsonErr := json.Unmarshal(body, &resp); jsonErr != nil {
		t.Fatalf("failed to unmarshal response: %v", jsonErr)
	}
	if resp.Success {
		t.Error("expected success=false")
	}
	if resp.Error == nil {
		t.Fatal("expected error to be non-nil")
	}
	if resp.Error.Code != ErrCodeNotFound {
		t.Errorf("expected code %s, got %s", ErrCodeNotFound, resp.Error.Code)
	}
	if resp.Error.Message != "user not found" {
		t.Errorf("expected message 'user not found', got '%s'", resp.Error.Message)
	}
}

func TestSuccessJSON(t *testing.T) {
	data := map[string]string{"message": "ok"}
	body, err := SuccessJSON(data)
	if err != nil {
		t.Fatalf("SuccessJSON returned error: %v", err)
	}

	var resp APIResponse
	if jsonErr := json.Unmarshal(body, &resp); jsonErr != nil {
		t.Fatalf("failed to unmarshal: %v", jsonErr)
	}
	if !resp.Success {
		t.Error("expected success=true")
	}
	if resp.Error != nil {
		t.Error("expected error to be nil")
	}
}

func TestAllErrorConstructors(t *testing.T) {
	tests := []struct {
		name       string
		err        *AppError
		wantStatus int
		wantCode   string
	}{
		{"BadRequest", NewBadRequest("x"), http.StatusBadRequest, ErrCodeBadRequest},
		{"Unauthorized", NewUnauthorized("x"), http.StatusUnauthorized, ErrCodeUnauthorized},
		{"Forbidden", NewForbidden("x"), http.StatusForbidden, ErrCodeForbidden},
		{"NotFound", NewNotFound("x"), http.StatusNotFound, ErrCodeNotFound},
		{"Conflict", NewConflict("x"), http.StatusConflict, ErrCodeConflict},
		{"Internal", NewInternal("x", nil), http.StatusInternalServerError, ErrCodeInternal},
		{"Provider", NewProviderError("x", nil), http.StatusBadGateway, ErrCodeProviderError},
		{"RateLimited", NewRateLimited(), http.StatusTooManyRequests, ErrCodeRateLimited},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err.StatusCode != tt.wantStatus {
				t.Errorf("status: got %d, want %d", tt.err.StatusCode, tt.wantStatus)
			}
			if tt.err.Code != tt.wantCode {
				t.Errorf("code: got %s, want %s", tt.err.Code, tt.wantCode)
			}
		})
	}
}
