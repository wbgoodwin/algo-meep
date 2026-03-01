package sync

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const syncTable = "algoflow-sync"

// DynamoDBClient defines the subset of DynamoDB operations used by the sync service.
type DynamoDBClient interface {
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
}

// S3Client defines the subset of S3 operations used by the sync service.
type S3Client interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// S3PresignClient defines the presign operations used for generating download URLs.
type S3PresignClient interface {
	PresignGetObject(ctx context.Context, userID, bucket, key string, expires time.Duration) (string, error)
}

// Service handles encrypted database sync via S3 + DynamoDB metadata.
type Service struct {
	db        DynamoDBClient
	s3Client  S3Client
	presigner S3PresignClient
	bucket    string
}

// s3Presigner wraps the real S3 presign client to satisfy our simplified interface.
type s3Presigner struct {
	client *s3.PresignClient
}

func (p *s3Presigner) PresignGetObject(ctx context.Context, _, bucket, key string, expires time.Duration) (string, error) {
	resp, err := p.client.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}, func(opts *s3.PresignOptions) {
		opts.Expires = expires
	})
	if err != nil {
		return "", err
	}
	return resp.URL, nil
}

// NewService creates a new sync service from an AWS config.
func NewService(cfg aws.Config, bucket string) *Service {
	s3Client := s3.NewFromConfig(cfg)
	return &Service{
		db:        dynamodb.NewFromConfig(cfg),
		s3Client:  s3Client,
		presigner: &s3Presigner{client: s3.NewPresignClient(s3Client)},
		bucket:    bucket,
	}
}

// NewServiceWithClients creates a new sync service with injected clients (for testing).
func NewServiceWithClients(db DynamoDBClient, s3Client S3Client, presigner S3PresignClient, bucket string) *Service {
	return &Service{
		db:        db,
		s3Client:  s3Client,
		presigner: presigner,
		bucket:    bucket,
	}
}

// SyncRecord represents sync metadata stored in DynamoDB.
type SyncRecord struct {
	PK        string `dynamodbav:"PK"`
	SK        string `dynamodbav:"SK"`
	Version   int    `dynamodbav:"version"`
	SizeBytes int64  `dynamodbav:"size_bytes"`
	Checksum  string `dynamodbav:"checksum"`
	DeviceID  string `dynamodbav:"device_id"`
	UpdatedAt string `dynamodbav:"updated_at"`
}

// s3Key returns the S3 object key for a user's sync blob.
func s3Key(userID string) string {
	return fmt.Sprintf("sync/%s/db.enc", userID)
}

// Push uploads an encrypted database blob to S3 and updates DynamoDB metadata.
func (s *Service) Push(ctx context.Context, userID string, data io.Reader, sizeBytes int64, checksum, deviceID string) (*SyncRecord, error) {
	key := s3Key(userID)

	// Upload to S3
	_, err := s.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          data,
		ContentLength: aws.Int64(sizeBytes),
		ContentType:   aws.String("application/octet-stream"),
		Metadata: map[string]string{
			"user-id":  userID,
			"checksum": checksum,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("s3 put object: %w", err)
	}

	// Get current version (if any) to increment
	currentVersion := 0
	existing, err := s.GetStatus(ctx, userID)
	if err == nil && existing != nil {
		currentVersion = existing.Version
	}

	record := SyncRecord{
		PK:        fmt.Sprintf("USER#%s", userID),
		SK:        "SYNC",
		Version:   currentVersion + 1,
		SizeBytes: sizeBytes,
		Checksum:  checksum,
		DeviceID:  deviceID,
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	item, err := attributevalue.MarshalMap(record)
	if err != nil {
		return nil, fmt.Errorf("marshal sync record: %w", err)
	}

	_, err = s.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(syncTable),
		Item:      item,
	})
	if err != nil {
		return nil, fmt.Errorf("put sync metadata: %w", err)
	}

	return &record, nil
}

// Pull generates a presigned S3 URL for downloading the encrypted database blob.
func (s *Service) Pull(ctx context.Context, userID string) (string, *SyncRecord, error) {
	// Check that sync data exists
	record, err := s.GetStatus(ctx, userID)
	if err != nil {
		return "", nil, err
	}
	if record == nil {
		return "", nil, nil
	}

	key := s3Key(userID)
	url, err := s.presigner.PresignGetObject(ctx, userID, s.bucket, key, 15*time.Minute)
	if err != nil {
		return "", nil, fmt.Errorf("presign get object: %w", err)
	}

	return url, record, nil
}

// GetStatus retrieves the current sync metadata for a user.
func (s *Service) GetStatus(ctx context.Context, userID string) (*SyncRecord, error) {
	result, err := s.db.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(syncTable),
		Key: map[string]dbtypes.AttributeValue{
			"PK": &dbtypes.AttributeValueMemberS{Value: fmt.Sprintf("USER#%s", userID)},
			"SK": &dbtypes.AttributeValueMemberS{Value: "SYNC"},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("get sync status: %w", err)
	}

	if result.Item == nil {
		return nil, nil
	}

	var record SyncRecord
	if err := attributevalue.UnmarshalMap(result.Item, &record); err != nil {
		return nil, fmt.Errorf("unmarshal sync record: %w", err)
	}
	return &record, nil
}

// ParsePushBody parses the base64-encoded body from a push request.
// The client sends the encrypted blob as the raw body with metadata in headers/query params.
// For Lambda, we receive the body as a string (possibly base64-encoded by API Gateway).
func ParsePushBody(body string) io.Reader {
	return strings.NewReader(body)
}
