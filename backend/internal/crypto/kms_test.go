package crypto

import (
	"context"
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/kms"
)

// --- Mock KMS Client ---

type mockKMS struct {
	encryptFn func(ctx context.Context, params *kms.EncryptInput, optFns ...func(*kms.Options)) (*kms.EncryptOutput, error)
	decryptFn func(ctx context.Context, params *kms.DecryptInput, optFns ...func(*kms.Options)) (*kms.DecryptOutput, error)
}

func (m *mockKMS) Encrypt(ctx context.Context, params *kms.EncryptInput, optFns ...func(*kms.Options)) (*kms.EncryptOutput, error) {
	if m.encryptFn != nil {
		return m.encryptFn(ctx, params, optFns...)
	}
	// Default: return the plaintext reversed as "ciphertext"
	return &kms.EncryptOutput{CiphertextBlob: params.Plaintext}, nil
}

func (m *mockKMS) Decrypt(ctx context.Context, params *kms.DecryptInput, optFns ...func(*kms.Options)) (*kms.DecryptOutput, error) {
	if m.decryptFn != nil {
		return m.decryptFn(ctx, params, optFns...)
	}
	return &kms.DecryptOutput{Plaintext: params.CiphertextBlob}, nil
}

// --- Tests ---

func TestNewTokenEncryptor(t *testing.T) {
	enc := NewTokenEncryptor(&mockKMS{}, "key-123")
	if enc == nil {
		t.Fatal("expected non-nil encryptor")
	}
	if enc.keyID != "key-123" {
		t.Errorf("expected keyID key-123, got %s", enc.keyID)
	}
}

func TestEncrypt_Success(t *testing.T) {
	enc := NewTokenEncryptor(&mockKMS{}, "key-123")
	result, err := enc.Encrypt(context.Background(), "access_token_abc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify it's valid base64
	decoded, err := base64.StdEncoding.DecodeString(result)
	if err != nil {
		t.Fatalf("result is not valid base64: %v", err)
	}
	if string(decoded) != "access_token_abc" {
		t.Errorf("expected decoded ciphertext to match plaintext (mock), got %s", string(decoded))
	}
}

func TestEncrypt_KMSError(t *testing.T) {
	enc := NewTokenEncryptor(&mockKMS{
		encryptFn: func(ctx context.Context, params *kms.EncryptInput, optFns ...func(*kms.Options)) (*kms.EncryptOutput, error) {
			return nil, fmt.Errorf("kms unavailable")
		},
	}, "key-123")

	_, err := enc.Encrypt(context.Background(), "token")
	if err == nil {
		t.Fatal("expected error")
	}
	if got := err.Error(); got != "kms encrypt failed: kms unavailable" {
		t.Errorf("unexpected error: %s", got)
	}
}

func TestDecrypt_Success(t *testing.T) {
	enc := NewTokenEncryptor(&mockKMS{}, "key-123")

	// Encrypt first to get a valid ciphertext
	ciphertext := base64.StdEncoding.EncodeToString([]byte("my_secret_token"))

	result, err := enc.Decrypt(context.Background(), ciphertext)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "my_secret_token" {
		t.Errorf("expected my_secret_token, got %s", result)
	}
}

func TestDecrypt_InvalidBase64(t *testing.T) {
	enc := NewTokenEncryptor(&mockKMS{}, "key-123")
	_, err := enc.Decrypt(context.Background(), "not-valid-base64!!!")
	if err == nil {
		t.Fatal("expected error for invalid base64")
	}
}

func TestDecrypt_KMSError(t *testing.T) {
	enc := NewTokenEncryptor(&mockKMS{
		decryptFn: func(ctx context.Context, params *kms.DecryptInput, optFns ...func(*kms.Options)) (*kms.DecryptOutput, error) {
			return nil, fmt.Errorf("access denied")
		},
	}, "key-123")

	ciphertext := base64.StdEncoding.EncodeToString([]byte("blob"))
	_, err := enc.Decrypt(context.Background(), ciphertext)
	if err == nil {
		t.Fatal("expected error")
	}
	if got := err.Error(); got != "kms decrypt failed: access denied" {
		t.Errorf("unexpected error: %s", got)
	}
}

func TestEncryptDecrypt_Roundtrip(t *testing.T) {
	enc := NewTokenEncryptor(&mockKMS{}, "key-123")
	original := "access_token_super_secret_12345"

	encrypted, err := enc.Encrypt(context.Background(), original)
	if err != nil {
		t.Fatalf("encrypt failed: %v", err)
	}

	decrypted, err := enc.Decrypt(context.Background(), encrypted)
	if err != nil {
		t.Fatalf("decrypt failed: %v", err)
	}

	if decrypted != original {
		t.Errorf("roundtrip failed: expected %s, got %s", original, decrypted)
	}
}

func TestEncrypt_VerifiesKeyID(t *testing.T) {
	var capturedKeyID string
	enc := NewTokenEncryptor(&mockKMS{
		encryptFn: func(ctx context.Context, params *kms.EncryptInput, optFns ...func(*kms.Options)) (*kms.EncryptOutput, error) {
			capturedKeyID = *params.KeyId
			return &kms.EncryptOutput{CiphertextBlob: params.Plaintext}, nil
		},
	}, "arn:aws:kms:us-east-1:123456789012:key/my-key-id")

	_, _ = enc.Encrypt(context.Background(), "token")
	if capturedKeyID != "arn:aws:kms:us-east-1:123456789012:key/my-key-id" {
		t.Errorf("expected key ARN to be passed, got %s", capturedKeyID)
	}
}
