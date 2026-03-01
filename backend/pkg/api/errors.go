package api

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// Common error codes.
const (
	ErrCodeBadRequest     = "BAD_REQUEST"
	ErrCodeUnauthorized   = "UNAUTHORIZED"
	ErrCodeForbidden      = "FORBIDDEN"
	ErrCodeNotFound       = "NOT_FOUND"
	ErrCodeConflict       = "CONFLICT"
	ErrCodeInternal       = "INTERNAL_ERROR"
	ErrCodeProviderError  = "PROVIDER_ERROR"
	ErrCodeRateLimited    = "RATE_LIMITED"
)

// AppError is a rich error that carries an HTTP status code and API error code.
type AppError struct {
	StatusCode int
	Code       string
	Message    string
	Err        error // underlying error (not exposed to client)
}

func (e *AppError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Err)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *AppError) Unwrap() error {
	return e.Err
}

// ToAPIResponse converts an AppError into a JSON response body.
func (e *AppError) ToAPIResponse() (int, []byte) {
	resp := APIResponse{
		Success: false,
		Error: &APIError{
			Code:    e.Code,
			Message: e.Message,
		},
	}
	body, _ := json.Marshal(resp)
	return e.StatusCode, body
}

// Error constructors.

func NewBadRequest(msg string) *AppError {
	return &AppError{StatusCode: http.StatusBadRequest, Code: ErrCodeBadRequest, Message: msg}
}

func NewUnauthorized(msg string) *AppError {
	return &AppError{StatusCode: http.StatusUnauthorized, Code: ErrCodeUnauthorized, Message: msg}
}

func NewForbidden(msg string) *AppError {
	return &AppError{StatusCode: http.StatusForbidden, Code: ErrCodeForbidden, Message: msg}
}

func NewNotFound(msg string) *AppError {
	return &AppError{StatusCode: http.StatusNotFound, Code: ErrCodeNotFound, Message: msg}
}

func NewConflict(msg string) *AppError {
	return &AppError{StatusCode: http.StatusConflict, Code: ErrCodeConflict, Message: msg}
}

func NewInternal(msg string, err error) *AppError {
	return &AppError{StatusCode: http.StatusInternalServerError, Code: ErrCodeInternal, Message: msg, Err: err}
}

func NewProviderError(msg string, err error) *AppError {
	return &AppError{StatusCode: http.StatusBadGateway, Code: ErrCodeProviderError, Message: msg, Err: err}
}

func NewRateLimited() *AppError {
	return &AppError{StatusCode: http.StatusTooManyRequests, Code: ErrCodeRateLimited, Message: "rate limit exceeded"}
}

// SuccessJSON encodes a success response.
func SuccessJSON(data interface{}) ([]byte, error) {
	return json.Marshal(APIResponse{Success: true, Data: data})
}
