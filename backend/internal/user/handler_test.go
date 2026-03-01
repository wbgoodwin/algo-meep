package user

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// --- Mock DynamoDB client ---

type mockDynamoDBClient struct {
	putItemFn    func(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	getItemFn    func(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	updateItemFn func(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	deleteItemFn func(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
}

func (m *mockDynamoDBClient) PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	return m.putItemFn(ctx, params, optFns...)
}

func (m *mockDynamoDBClient) GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	return m.getItemFn(ctx, params, optFns...)
}

func (m *mockDynamoDBClient) UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	return m.updateItemFn(ctx, params, optFns...)
}

func (m *mockDynamoDBClient) DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	return m.deleteItemFn(ctx, params, optFns...)
}

// --- NewServiceWithClient test ---

func TestNewServiceWithClient(t *testing.T) {
	mock := &mockDynamoDBClient{}
	svc := NewServiceWithClient(mock)
	if svc == nil {
		t.Fatal("expected non-nil service")
	}
}

// --- CreateUser tests ---

func TestCreateUser_Success(t *testing.T) {
	mock := &mockDynamoDBClient{
		putItemFn: func(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			if *params.TableName != usersTable {
				t.Errorf("expected table %s, got %s", usersTable, *params.TableName)
			}
			// Verify PK is set correctly
			pk, ok := params.Item["PK"]
			if !ok {
				t.Fatal("expected PK in item")
			}
			if pkVal, ok := pk.(*dbtypes.AttributeValueMemberS); ok {
				if pkVal.Value != "USER#user-123" {
					t.Errorf("expected PK=USER#user-123, got %s", pkVal.Value)
				}
			}
			// Verify condition expression prevents overwrites
			if params.ConditionExpression == nil || *params.ConditionExpression != "attribute_not_exists(PK)" {
				t.Error("expected condition expression attribute_not_exists(PK)")
			}
			return &dynamodb.PutItemOutput{}, nil
		},
	}
	svc := NewServiceWithClient(mock)
	err := svc.CreateUser(context.Background(), "user-123", "test@example.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCreateUser_DynamoDBError(t *testing.T) {
	mock := &mockDynamoDBClient{
		putItemFn: func(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			return nil, fmt.Errorf("ConditionalCheckFailedException: item already exists")
		},
	}
	svc := NewServiceWithClient(mock)
	err := svc.CreateUser(context.Background(), "user-123", "test@example.com")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strContains(err.Error(), "put user") {
		t.Errorf("expected wrapped error, got: %v", err)
	}
}

func TestCreateUser_DefaultValues(t *testing.T) {
	mock := &mockDynamoDBClient{
		putItemFn: func(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			// Verify default values
			plan, ok := params.Item["plan"]
			if !ok {
				t.Fatal("expected plan attribute")
			}
			if planVal, ok := plan.(*dbtypes.AttributeValueMemberS); ok {
				if planVal.Value != "free" {
					t.Errorf("expected plan=free, got %s", planVal.Value)
				}
			}
			return &dynamodb.PutItemOutput{}, nil
		},
	}
	svc := NewServiceWithClient(mock)
	_ = svc.CreateUser(context.Background(), "user-123", "test@example.com")
}

// --- GetUser tests ---

func TestGetUser_Found(t *testing.T) {
	record := UserRecord{
		PK:                "USER#user-123",
		SK:                "PROFILE",
		Email:             "test@example.com",
		Plan:              "premium",
		ConnectedAccounts: 3,
		CreatedAt:         "2024-01-01T00:00:00Z",
	}
	item, _ := attributevalue.MarshalMap(record)

	mock := &mockDynamoDBClient{
		getItemFn: func(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			if *params.TableName != usersTable {
				t.Errorf("expected table %s, got %s", usersTable, *params.TableName)
			}
			// Verify key structure
			pk := params.Key["PK"].(*dbtypes.AttributeValueMemberS)
			if pk.Value != "USER#user-123" {
				t.Errorf("expected PK=USER#user-123, got %s", pk.Value)
			}
			sk := params.Key["SK"].(*dbtypes.AttributeValueMemberS)
			if sk.Value != "PROFILE" {
				t.Errorf("expected SK=PROFILE, got %s", sk.Value)
			}
			return &dynamodb.GetItemOutput{Item: item}, nil
		},
	}
	svc := NewServiceWithClient(mock)
	result, err := svc.GetUser(context.Background(), "user-123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil user record")
	}
	if result.Email != "test@example.com" {
		t.Errorf("expected test@example.com, got %s", result.Email)
	}
	if result.Plan != "premium" {
		t.Errorf("expected premium, got %s", result.Plan)
	}
	if result.ConnectedAccounts != 3 {
		t.Errorf("expected 3 connected accounts, got %d", result.ConnectedAccounts)
	}
}

func TestGetUser_NotFound(t *testing.T) {
	mock := &mockDynamoDBClient{
		getItemFn: func(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: nil}, nil
		},
	}
	svc := NewServiceWithClient(mock)
	result, err := svc.GetUser(context.Background(), "nonexistent")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Error("expected nil for nonexistent user")
	}
}

func TestGetUser_DynamoDBError(t *testing.T) {
	mock := &mockDynamoDBClient{
		getItemFn: func(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return nil, fmt.Errorf("network timeout")
		},
	}
	svc := NewServiceWithClient(mock)
	_, err := svc.GetUser(context.Background(), "user-123")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strContains(err.Error(), "get user") {
		t.Errorf("expected wrapped error, got: %v", err)
	}
}

// --- UpdatePlan tests ---

func TestUpdatePlan_Success(t *testing.T) {
	mock := &mockDynamoDBClient{
		updateItemFn: func(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
			if *params.TableName != usersTable {
				t.Errorf("expected table %s, got %s", usersTable, *params.TableName)
			}
			pk := params.Key["PK"].(*dbtypes.AttributeValueMemberS)
			if pk.Value != "USER#user-123" {
				t.Errorf("expected PK=USER#user-123, got %s", pk.Value)
			}
			if *params.UpdateExpression != "SET #plan = :plan" {
				t.Errorf("unexpected update expression: %s", *params.UpdateExpression)
			}
			planVal := params.ExpressionAttributeValues[":plan"].(*dbtypes.AttributeValueMemberS)
			if planVal.Value != "premium" {
				t.Errorf("expected plan=premium, got %s", planVal.Value)
			}
			return &dynamodb.UpdateItemOutput{}, nil
		},
	}
	svc := NewServiceWithClient(mock)
	err := svc.UpdatePlan(context.Background(), "user-123", "premium")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestUpdatePlan_Error(t *testing.T) {
	mock := &mockDynamoDBClient{
		updateItemFn: func(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
			return nil, fmt.Errorf("throttled")
		},
	}
	svc := NewServiceWithClient(mock)
	err := svc.UpdatePlan(context.Background(), "user-123", "premium")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strContains(err.Error(), "update plan") {
		t.Errorf("expected wrapped error, got: %v", err)
	}
}

// --- IncrementConnectedAccounts tests ---

func TestIncrementConnectedAccounts_Success(t *testing.T) {
	mock := &mockDynamoDBClient{
		updateItemFn: func(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
			if *params.UpdateExpression != "ADD connected_accounts :delta" {
				t.Errorf("unexpected update expression: %s", *params.UpdateExpression)
			}
			deltaVal := params.ExpressionAttributeValues[":delta"].(*dbtypes.AttributeValueMemberN)
			if deltaVal.Value != "1" {
				t.Errorf("expected delta=1, got %s", deltaVal.Value)
			}
			return &dynamodb.UpdateItemOutput{}, nil
		},
	}
	svc := NewServiceWithClient(mock)
	err := svc.IncrementConnectedAccounts(context.Background(), "user-123", 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestIncrementConnectedAccounts_NegativeDelta(t *testing.T) {
	mock := &mockDynamoDBClient{
		updateItemFn: func(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
			deltaVal := params.ExpressionAttributeValues[":delta"].(*dbtypes.AttributeValueMemberN)
			if deltaVal.Value != "-1" {
				t.Errorf("expected delta=-1, got %s", deltaVal.Value)
			}
			return &dynamodb.UpdateItemOutput{}, nil
		},
	}
	svc := NewServiceWithClient(mock)
	err := svc.IncrementConnectedAccounts(context.Background(), "user-123", -1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestIncrementConnectedAccounts_Error(t *testing.T) {
	mock := &mockDynamoDBClient{
		updateItemFn: func(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
			return nil, fmt.Errorf("throttled")
		},
	}
	svc := NewServiceWithClient(mock)
	err := svc.IncrementConnectedAccounts(context.Background(), "user-123", 1)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strContains(err.Error(), "increment connected accounts") {
		t.Errorf("expected wrapped error, got: %v", err)
	}
}

// --- DeleteUser tests ---

func TestDeleteUser_Success(t *testing.T) {
	mock := &mockDynamoDBClient{
		deleteItemFn: func(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
			if *params.TableName != usersTable {
				t.Errorf("expected table %s, got %s", usersTable, *params.TableName)
			}
			pk := params.Key["PK"].(*dbtypes.AttributeValueMemberS)
			if pk.Value != "USER#user-123" {
				t.Errorf("expected PK=USER#user-123, got %s", pk.Value)
			}
			return &dynamodb.DeleteItemOutput{}, nil
		},
	}
	svc := NewServiceWithClient(mock)
	err := svc.DeleteUser(context.Background(), "user-123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDeleteUser_Error(t *testing.T) {
	mock := &mockDynamoDBClient{
		deleteItemFn: func(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
			return nil, fmt.Errorf("item not found")
		},
	}
	svc := NewServiceWithClient(mock)
	err := svc.DeleteUser(context.Background(), "user-123")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strContains(err.Error(), "delete user") {
		t.Errorf("expected wrapped error, got: %v", err)
	}
}

func strContains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
