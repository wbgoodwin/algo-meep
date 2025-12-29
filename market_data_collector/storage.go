package main

import (
	"bytes"
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	s3Prefix  = "market_data" // S3 prefix/folder
	awsRegion = "us-east-1"   // <-- set your region
)

// uploadToS3 uploads Parquet bytes to S3 at the given key
func uploadToS3(data []byte, key string) error {
	bucket := os.Getenv("S3_BUCKET")
	if bucket == "" {
		return fmt.Errorf("S3_BUCKET environment variable not set. Please set it to your S3 bucket name meep")
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(awsRegion),
	)
	if err != nil {
		return err
	}

	// Configure S3 client with LocalStack endpoint if available
	svc := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		if endpoint := os.Getenv("LOCALSTACK_ENDPOINT"); endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	})

	_, err = svc.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/octet-stream"),
	})
	return err
}
