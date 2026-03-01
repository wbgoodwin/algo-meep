package sync

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// --- Mock clients ---

type mockDynamoDB struct {
	putItemFn func(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	getItemFn func(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
}

func (m *mockDynamoDB) PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	if m.putItemFn != nil {
		return m.putItemFn(ctx, params, optFns...)
	}
	return &dynamodb.PutItemOutput{}, nil
}

func (m *mockDynamoDB) GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if m.getItemFn != nil {
		return m.getItemFn(ctx, params, optFns...)
	}
	return &dynamodb.GetItemOutput{}, nil
}

type mockS3 struct {
	putObjectFn func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

func (m *mockS3) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if m.putObjectFn != nil {
		return m.putObjectFn(ctx, params, optFns...)
	}
	return &s3.PutObjectOutput{}, nil
}

type mockPresigner struct {
	presignGetObjectFn func(ctx context.Context, bucket, key string, expires time.Duration) (string, error)
}

func (m *mockPresigner) PresignGetObject(ctx context.Context, bucket, key string, expires time.Duration) (string, error) {
	if m.presignGetObjectFn != nil {
		return m.presignGetObjectFn(ctx, bucket, key, expires)
	}
	return "https://s3.example.com/presigned", nil
}

// --- Helper ---

func newTestService(db *mockDynamoDB, s3c *mockS3, presigner *mockPresigner) *Service {
	return NewServiceWithClients(db, s3c, presigner, "test-bucket")
}

func makeSyncItem(userID string, version int, sizeBytes int64, checksum, deviceID string) map[string]dbtypes.AttributeValue {
	record := SyncRecord{
		PK:        fmt.Sprintf("USER#%s", userID),
		SK:        "SYNC",
		Version:   version,
		SizeBytes: sizeBytes,
		Checksum:  checksum,
		DeviceID:  deviceID,
		UpdatedAt: "2025-01-01T00:00:00Z",
	}
	item, _ := attributevalue.MarshalMap(record)
	return item
}

// --- Tests ---

func TestGetStatus_Found(t *testing.T) {
	db := &mockDynamoDB{
		getItemFn: func(_ context.Context, params *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			if *params.TableName != syncTable {
				t.Errorf("unexpected table: %s", *params.TableName)
			}
			return &dynamodb.GetItemOutput{
				Item: makeSyncItem("user-1", 3, 1024, "abc123", "device-a"),
			}, nil
		},
	}
	svc := newTestService(db, &mockS3{}, &mockPresigner{})

	record, err := svc.GetStatus(context.Background(), "user-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if record == nil {
		t.Fatal("expected record, got nil")
	}
	if record.Version != 3 {
		t.Errorf("expected version 3, got %d", record.Version)
	}
	if record.SizeBytes != 1024 {
		t.Errorf("expected size 1024, got %d", record.SizeBytes)
	}
	if record.Checksum != "abc123" {
		t.Errorf("expected checksum abc123, got %s", record.Checksum)
	}
	if record.DeviceID != "device-a" {
		t.Errorf("expected device-a, got %s", record.DeviceID)
	}
}

func TestGetStatus_NotFound(t *testing.T) {
	db := &mockDynamoDB{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: nil}, nil
		},
	}
	svc := newTestService(db, &mockS3{}, &mockPresigner{})

	record, err := svc.GetStatus(context.Background(), "user-missing")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if record != nil {
		t.Fatalf("expected nil, got %+v", record)
	}
}

func TestGetStatus_DynamoError(t *testing.T) {
	db := &mockDynamoDB{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return nil, fmt.Errorf("dynamo unavailable")
		},
	}
	svc := newTestService(db, &mockS3{}, &mockPresigner{})

	_, err := svc.GetStatus(context.Background(), "user-1")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "dynamo unavailable") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestPush_Success(t *testing.T) {
	var capturedS3Key string
	var capturedDDBItem map[string]dbtypes.AttributeValue

	db := &mockDynamoDB{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			// No existing record
			return &dynamodb.GetItemOutput{Item: nil}, nil
		},
		putItemFn: func(_ context.Context, params *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			capturedDDBItem = params.Item
			return &dynamodb.PutItemOutput{}, nil
		},
	}
	s3c := &mockS3{
		putObjectFn: func(_ context.Context, params *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			capturedS3Key = *params.Key
			if *params.Bucket != "test-bucket" {
				t.Errorf("unexpected bucket: %s", *params.Bucket)
			}
			return &s3.PutObjectOutput{}, nil
		},
	}

	svc := newTestService(db, s3c, &mockPresigner{})

	data := strings.NewReader("encrypted-blob-data")
	record, err := svc.Push(context.Background(), "user-1", data, 19, "sha256:abc", "device-a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify S3 key
	if capturedS3Key != "sync/user-1/db.enc" {
		t.Errorf("unexpected S3 key: %s", capturedS3Key)
	}

	// Verify returned record
	if record.Version != 1 {
		t.Errorf("expected version 1, got %d", record.Version)
	}
	if record.SizeBytes != 19 {
		t.Errorf("expected size 19, got %d", record.SizeBytes)
	}
	if record.Checksum != "sha256:abc" {
		t.Errorf("expected checksum sha256:abc, got %s", record.Checksum)
	}

	// Verify DDB item was written
	if capturedDDBItem == nil {
		t.Fatal("expected DDB put, got none")
	}
}

func TestPush_IncrementsVersion(t *testing.T) {
	db := &mockDynamoDB{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: makeSyncItem("user-1", 5, 512, "old", "device-b"),
			}, nil
		},
		putItemFn: func(_ context.Context, _ *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			return &dynamodb.PutItemOutput{}, nil
		},
	}
	s3c := &mockS3{}

	svc := newTestService(db, s3c, &mockPresigner{})
	record, err := svc.Push(context.Background(), "user-1", strings.NewReader("data"), 4, "sha256:new", "device-a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if record.Version != 6 {
		t.Errorf("expected version 6, got %d", record.Version)
	}
}

func TestPush_S3Error(t *testing.T) {
	db := &mockDynamoDB{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: nil}, nil
		},
	}
	s3c := &mockS3{
		putObjectFn: func(_ context.Context, _ *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			return nil, fmt.Errorf("s3 write failed")
		},
	}

	svc := newTestService(db, s3c, &mockPresigner{})
	_, err := svc.Push(context.Background(), "user-1", strings.NewReader("data"), 4, "sha256:x", "d")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "s3 write failed") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestPush_DynamoError(t *testing.T) {
	db := &mockDynamoDB{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: nil}, nil
		},
		putItemFn: func(_ context.Context, _ *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			return nil, fmt.Errorf("dynamo write failed")
		},
	}
	s3c := &mockS3{}

	svc := newTestService(db, s3c, &mockPresigner{})
	_, err := svc.Push(context.Background(), "user-1", strings.NewReader("data"), 4, "sha256:x", "d")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "dynamo write failed") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestPush_VersionConflict(t *testing.T) {
	db := &mockDynamoDB{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: makeSyncItem("user-1", 5, 512, "old", "device-b"),
			}, nil
		},
		putItemFn: func(_ context.Context, _ *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			return nil, &dbtypes.ConditionalCheckFailedException{Message: stringPtr("condition not met")}
		},
	}
	s3c := &mockS3{}

	svc := newTestService(db, s3c, &mockPresigner{})
	_, err := svc.Push(context.Background(), "user-1", strings.NewReader("data"), 4, "sha256:x", "d")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrVersionConflict) {
		t.Errorf("expected ErrVersionConflict, got: %v", err)
	}
}

func TestPush_DynamoReadError(t *testing.T) {
	db := &mockDynamoDB{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return nil, fmt.Errorf("dynamo unavailable")
		},
	}

	svc := newTestService(db, &mockS3{}, &mockPresigner{})
	_, err := svc.Push(context.Background(), "user-1", strings.NewReader("data"), 4, "sha256:x", "d")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "get current version") {
		t.Errorf("expected get current version error, got: %v", err)
	}
}

func stringPtr(s string) *string { return &s }

func TestPull_Success(t *testing.T) {
	db := &mockDynamoDB{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: makeSyncItem("user-1", 3, 1024, "abc123", "device-a"),
			}, nil
		},
	}
	presigner := &mockPresigner{
		presignGetObjectFn: func(_ context.Context, bucket, key string, _ time.Duration) (string, error) {
			if key != "sync/user-1/db.enc" {
				t.Errorf("unexpected key: %s", key)
			}
			if bucket != "test-bucket" {
				t.Errorf("unexpected bucket: %s", bucket)
			}
			return "https://s3.example.com/presigned-url", nil
		},
	}

	svc := newTestService(db, &mockS3{}, presigner)

	url, record, err := svc.Pull(context.Background(), "user-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if url != "https://s3.example.com/presigned-url" {
		t.Errorf("unexpected URL: %s", url)
	}
	if record == nil {
		t.Fatal("expected record, got nil")
	}
	if record.Version != 3 {
		t.Errorf("expected version 3, got %d", record.Version)
	}
}

func TestPull_NoData(t *testing.T) {
	db := &mockDynamoDB{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: nil}, nil
		},
	}
	svc := newTestService(db, &mockS3{}, &mockPresigner{})

	url, record, err := svc.Pull(context.Background(), "user-missing")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if url != "" {
		t.Errorf("expected empty URL, got %s", url)
	}
	if record != nil {
		t.Errorf("expected nil record, got %+v", record)
	}
}

func TestPull_PresignError(t *testing.T) {
	db := &mockDynamoDB{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: makeSyncItem("user-1", 1, 100, "x", "d"),
			}, nil
		},
	}
	presigner := &mockPresigner{
		presignGetObjectFn: func(_ context.Context, _, _ string, _ time.Duration) (string, error) {
			return "", fmt.Errorf("presign failed")
		},
	}
	svc := newTestService(db, &mockS3{}, presigner)

	_, _, err := svc.Pull(context.Background(), "user-1")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "presign failed") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestS3Key(t *testing.T) {
	key := s3Key("abc-123")
	if key != "sync/abc-123/db.enc" {
		t.Errorf("unexpected key: %s", key)
	}
}

func TestParsePushBody(t *testing.T) {
	original := "hello world"
	encoded := base64.StdEncoding.EncodeToString([]byte(original))

	reader, size, err := ParsePushBody(encoded)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if size != int64(len(original)) {
		t.Errorf("expected size %d, got %d", len(original), size)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("unexpected read error: %v", err)
	}
	if string(data) != original {
		t.Errorf("expected %q, got %q", original, string(data))
	}
}

func TestParsePushBody_InvalidBase64(t *testing.T) {
	_, _, err := ParsePushBody("not valid base64!!!")
	if err == nil {
		t.Fatal("expected error for invalid base64, got nil")
	}
	if !strings.Contains(err.Error(), "base64 decode") {
		t.Errorf("unexpected error: %v", err)
	}
}
