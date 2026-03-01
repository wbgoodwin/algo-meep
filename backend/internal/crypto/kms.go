package crypto

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
)

// KMSClient defines the minimal KMS operations needed for token encryption.
type KMSClient interface {
	Encrypt(ctx context.Context, params *kms.EncryptInput, optFns ...func(*kms.Options)) (*kms.EncryptOutput, error)
	Decrypt(ctx context.Context, params *kms.DecryptInput, optFns ...func(*kms.Options)) (*kms.DecryptOutput, error)
}

// TokenEncryptor encrypts and decrypts bank access tokens using AWS KMS.
type TokenEncryptor struct {
	client KMSClient
	keyID  string
}

// NewTokenEncryptor creates a new TokenEncryptor with the given KMS client and key ID.
func NewTokenEncryptor(client KMSClient, keyID string) *TokenEncryptor {
	return &TokenEncryptor{client: client, keyID: keyID}
}

// Encrypt encrypts a plaintext access token and returns a base64-encoded ciphertext.
func (e *TokenEncryptor) Encrypt(ctx context.Context, plaintext string) (string, error) {
	output, err := e.client.Encrypt(ctx, &kms.EncryptInput{
		KeyId:     aws.String(e.keyID),
		Plaintext: []byte(plaintext),
	})
	if err != nil {
		return "", fmt.Errorf("kms encrypt failed: %w", err)
	}
	return base64.StdEncoding.EncodeToString(output.CiphertextBlob), nil
}

// Decrypt decodes a base64-encoded ciphertext and decrypts it, returning the plaintext access token.
func (e *TokenEncryptor) Decrypt(ctx context.Context, ciphertext string) (string, error) {
	blob, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", fmt.Errorf("invalid base64 ciphertext: %w", err)
	}
	output, err := e.client.Decrypt(ctx, &kms.DecryptInput{
		CiphertextBlob: blob,
	})
	if err != nil {
		return "", fmt.Errorf("kms decrypt failed: %w", err)
	}
	return string(output.Plaintext), nil
}
