package user

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const usersTable = "algoflow-users"

// DynamoDBClient defines the subset of DynamoDB operations used by the user service.
type DynamoDBClient interface {
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
}

// Service handles user profile operations via DynamoDB.
type Service struct {
	db DynamoDBClient
}

// NewService creates a new user service from an AWS config.
func NewService(cfg aws.Config) *Service {
	return &Service{db: dynamodb.NewFromConfig(cfg)}
}

// NewServiceWithClient creates a new user service with an injected client (for testing).
func NewServiceWithClient(client DynamoDBClient) *Service {
	return &Service{db: client}
}

// UserRecord represents a user stored in DynamoDB.
type UserRecord struct {
	PK                string `dynamodbav:"PK"`
	SK                string `dynamodbav:"SK"`
	Email             string `dynamodbav:"email"`
	Plan              string `dynamodbav:"plan"`
	ConnectedAccounts int    `dynamodbav:"connected_accounts"`
	CreatedAt         string `dynamodbav:"created_at"`
}

// CreateUser stores a new user profile after Cognito registration.
func (s *Service) CreateUser(ctx context.Context, userID, email string) error {
	record := UserRecord{
		PK:                fmt.Sprintf("USER#%s", userID),
		SK:                "PROFILE",
		Email:             email,
		Plan:              "free",
		ConnectedAccounts: 0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339),
	}

	item, err := attributevalue.MarshalMap(record)
	if err != nil {
		return fmt.Errorf("marshal user record: %w", err)
	}

	_, err = s.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(usersTable),
		Item:                item,
		ConditionExpression: aws.String("attribute_not_exists(PK)"),
	})
	if err != nil {
		return fmt.Errorf("put user: %w", err)
	}
	return nil
}

// GetUser retrieves a user profile from DynamoDB.
func (s *Service) GetUser(ctx context.Context, userID string) (*UserRecord, error) {
	result, err := s.db.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(usersTable),
		Key: map[string]dbtypes.AttributeValue{
			"PK": &dbtypes.AttributeValueMemberS{Value: fmt.Sprintf("USER#%s", userID)},
			"SK": &dbtypes.AttributeValueMemberS{Value: "PROFILE"},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("get user: %w", err)
	}

	if result.Item == nil {
		return nil, nil
	}

	var record UserRecord
	if err := attributevalue.UnmarshalMap(result.Item, &record); err != nil {
		return nil, fmt.Errorf("unmarshal user record: %w", err)
	}
	return &record, nil
}

// UpdatePlan updates the user's subscription plan.
func (s *Service) UpdatePlan(ctx context.Context, userID, plan string) error {
	_, err := s.db.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(usersTable),
		Key: map[string]dbtypes.AttributeValue{
			"PK": &dbtypes.AttributeValueMemberS{Value: fmt.Sprintf("USER#%s", userID)},
			"SK": &dbtypes.AttributeValueMemberS{Value: "PROFILE"},
		},
		UpdateExpression: aws.String("SET #plan = :plan"),
		ExpressionAttributeNames: map[string]string{
			"#plan": "plan",
		},
		ExpressionAttributeValues: map[string]dbtypes.AttributeValue{
			":plan": &dbtypes.AttributeValueMemberS{Value: plan},
		},
	})
	if err != nil {
		return fmt.Errorf("update plan: %w", err)
	}
	return nil
}

// IncrementConnectedAccounts atomically increments the connected account count.
func (s *Service) IncrementConnectedAccounts(ctx context.Context, userID string, delta int) error {
	_, err := s.db.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(usersTable),
		Key: map[string]dbtypes.AttributeValue{
			"PK": &dbtypes.AttributeValueMemberS{Value: fmt.Sprintf("USER#%s", userID)},
			"SK": &dbtypes.AttributeValueMemberS{Value: "PROFILE"},
		},
		UpdateExpression: aws.String("ADD connected_accounts :delta"),
		ExpressionAttributeValues: map[string]dbtypes.AttributeValue{
			":delta": &dbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", delta)},
		},
	})
	if err != nil {
		return fmt.Errorf("increment connected accounts: %w", err)
	}
	return nil
}

// DeleteUser removes a user record from DynamoDB.
func (s *Service) DeleteUser(ctx context.Context, userID string) error {
	_, err := s.db.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(usersTable),
		Key: map[string]dbtypes.AttributeValue{
			"PK": &dbtypes.AttributeValueMemberS{Value: fmt.Sprintf("USER#%s", userID)},
			"SK": &dbtypes.AttributeValueMemberS{Value: "PROFILE"},
		},
	})
	if err != nil {
		return fmt.Errorf("delete user: %w", err)
	}
	return nil
}
