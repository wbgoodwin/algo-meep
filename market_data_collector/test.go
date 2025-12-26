package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

// ================================
// DATA MODELS
// ================================

// TestMarketData represents raw market data with Parquet tags for efficient storage
type TestMarketData struct {
	Timestamp  int64   `parquet:"name=timestamp, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	Symbol     string  `parquet:"name=symbol, type=BYTE_ARRAY, convertedtype=UTF8"`
	Open       float64 `parquet:"name=open, type=DOUBLE"`
	High       float64 `parquet:"name=high, type=DOUBLE"`
	Low        float64 `parquet:"name=low, type=DOUBLE"`
	Close      float64 `parquet:"name=close, type=DOUBLE"`
	Volume     int64   `parquet:"name=volume, type=INT64"`
	VWAP       float64 `parquet:"name=vwap, type=DOUBLE"`
	TradeCount int32   `parquet:"name=trade_count, type=INT32"`
}

// FeatureData represents processed features for ML
type FeatureData struct {
	Timestamp      int64   `parquet:"name=timestamp, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	Symbol         string  `parquet:"name=symbol, type=BYTE_ARRAY, convertedtype=UTF8"`
	RSI14          float64 `parquet:"name=rsi_14, type=DOUBLE"`
	MACD           float64 `parquet:"name=macd, type=DOUBLE"`
	MACDSignal     float64 `parquet:"name=macd_signal, type=DOUBLE"`
	BollingerUpper float64 `parquet:"name=bb_upper, type=DOUBLE"`
	BollingerLower float64 `parquet:"name=bb_lower, type=DOUBLE"`
	SMA20          float64 `parquet:"name=sma_20, type=DOUBLE"`
	SMA50          float64 `parquet:"name=sma_50, type=DOUBLE"`
	VolumeRatio    float64 `parquet:"name=volume_ratio, type=DOUBLE"`
	SPYCorrelation float64 `parquet:"name=spy_correlation, type=DOUBLE"`
	VIXLevel       float64 `parquet:"name=vix_level, type=DOUBLE"`
	Target         float64 `parquet:"name=target, type=DOUBLE"`
}

// ================================
// S3 PARTITIONING STRATEGY
// ================================

// GenerateS3Key creates properly partitioned S3 keys
// Partitioning scheme: bucket/data_type/year/month/day/hour/symbol/filename.parquet
func GenerateS3Key(dataType string, symbol string, timestamp time.Time, dataInterval string) string {
	// For minute data, partition by hour to keep file sizes manageable
	// For daily data, partition by month

	if dataInterval == "minute" {
		return fmt.Sprintf("%s/year=%d/month=%02d/day=%02d/hour=%02d/symbol=%s/data_%d.parquet",
			dataType,
			timestamp.Year(),
			timestamp.Month(),
			timestamp.Day(),
			timestamp.Hour(),
			symbol,
			timestamp.Unix(),
		)
	}

	// For daily data
	return fmt.Sprintf("%s/year=%d/month=%02d/symbol=%s/daily_%d.parquet",
		dataType,
		timestamp.Year(),
		timestamp.Month(),
		symbol,
		timestamp.Unix(),
	)
}

// ================================
// DATA STORAGE FUNCTIONS
// ================================

// StoreMarketDataToS3 stores market data in Parquet format with proper partitioning
func StoreMarketDataToS3(ctx context.Context, s3Client *s3.Client, bucketName string, data []TestMarketData) error {
	if len(data) == 0 {
		return fmt.Errorf("no data to store")
	}

	// Group data by symbol and hour for efficient partitioning
	dataByPartition := make(map[string][]TestMarketData)

	for _, record := range data {
		timestamp := time.Unix(record.Timestamp/1000, 0)
		key := GenerateS3Key("market-data", record.Symbol, timestamp, "minute")
		dataByPartition[key] = append(dataByPartition[key], record)
	}

	// Store each partition
	for s3Key, partitionData := range dataByPartition {
		if err := writeParquetToS3(ctx, s3Client, bucketName, s3Key, partitionData); err != nil {
			return fmt.Errorf("failed to write partition %s: %w", s3Key, err)
		}
		log.Printf("Successfully stored %d records to s3://%s/%s", len(partitionData), bucketName, s3Key)
	}

	return nil
}

// writeParquetToS3 writes Parquet data to S3
func writeParquetToS3(ctx context.Context, s3Client *s3.Client, bucketName, key string, data interface{}) error {
	// Create a buffer to write Parquet data
	buf := new(bytes.Buffer)

	// Create Parquet writer using the buffer directly
	pw, err := writer.NewParquetWriterFromWriter(buf, data, 4)
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}

	pw.RowGroupSize = 128 * 1024 * 1024 // 128MB row groups
	pw.PageSize = 8 * 1024              // 8KB pages
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	// Write data based on type
	switch v := data.(type) {
	case []TestMarketData:
		for _, record := range v {
			if err := pw.Write(record); err != nil {
				return fmt.Errorf("failed to write record: %w", err)
			}
		}
	case []FeatureData:
		for _, record := range v {
			if err := pw.Write(record); err != nil {
				return fmt.Errorf("failed to write record: %w", err)
			}
		}
	}

	if err := pw.WriteStop(); err != nil {
		return fmt.Errorf("failed to stop writer: %w", err)
	}

	// Upload to S3 with metadata
	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(key),
		Body:        bytes.NewReader(buf.Bytes()),
		ContentType: aws.String("application/octet-stream"),
		Metadata: map[string]string{
			"format":      "parquet",
			"compression": "snappy",
			"created":     time.Now().UTC().Format(time.RFC3339),
		},
		StorageClass: types.StorageClassIntelligentTiering,
	})

	return err
}

// ================================
// DATA QUERY FUNCTIONS
// ================================

// QueryMarketData queries market data using S3 Select with SQL
func QueryMarketData(ctx context.Context, s3Client *s3.Client, bucketName string, symbol string, startTime, endTime time.Time) ([]TestMarketData, error) {
	var results []TestMarketData

	// Generate S3 prefixes for the date range
	prefixes := generatePrefixesForDateRange("market-data", symbol, startTime, endTime)

	for _, prefix := range prefixes {
		// List objects with prefix
		listInput := &s3.ListObjectsV2Input{
			Bucket: aws.String(bucketName),
			Prefix: aws.String(prefix),
		}

		paginator := s3.NewListObjectsV2Paginator(s3Client, listInput)
		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to list objects: %w", err)
			}

			for _, obj := range page.Contents {
				// Use S3 Select to query Parquet files
				data, err := queryParquetWithS3Select(ctx, s3Client, bucketName, *obj.Key, symbol, startTime, endTime)
				if err != nil {
					log.Printf("Error querying %s: %v", *obj.Key, err)
					continue
				}
				results = append(results, data...)
			}
		}
	}

	return results, nil
}

// queryParquetWithS3Select uses S3 Select to query Parquet files efficiently
func queryParquetWithS3Select(ctx context.Context, s3Client *s3.Client, bucketName, key, symbol string, startTime, endTime time.Time) ([]TestMarketData, error) {
	// SQL query for S3 Select
	query := fmt.Sprintf(`
		SELECT * FROM S3Object[*] s
		WHERE s.symbol = '%s'
		AND s.timestamp >= %d
		AND s.timestamp <= %d
		ORDER BY s.timestamp
	`, symbol, startTime.Unix()*1000, endTime.Unix()*1000)

	selectInput := &s3.SelectObjectContentInput{
		Bucket:         aws.String(bucketName),
		Key:            aws.String(key),
		ExpressionType: types.ExpressionTypeSql,
		Expression:     aws.String(query),
		InputSerialization: &types.InputSerialization{
			Parquet: &types.ParquetInput{},
		},
		OutputSerialization: &types.OutputSerialization{
			JSON: &types.JSONOutput{
				RecordDelimiter: aws.String("\n"),
			},
		},
	}

	resp, err := s3Client.SelectObjectContent(ctx, selectInput)
	if err != nil {
		return nil, fmt.Errorf("S3 Select failed: %w", err)
	}
	defer resp.GetStream().Close()

	var results []TestMarketData
	
	for event := range resp.GetStream().Events() {
		switch v := event.(type) {
		case *types.SelectObjectContentEventStreamMemberRecords:
			// Parse the records from the event
			decoder := json.NewDecoder(bytes.NewReader(v.Value.Payload))
			for {
				var record TestMarketData
				err := decoder.Decode(&record)
				if err != nil {
					break
				}
				results = append(results, record)
			}
		case *types.SelectObjectContentEventStreamMemberStats:
			// Handle stats if needed
			continue
		case *types.SelectObjectContentEventStreamMemberProgress:
			// Handle progress if needed
			continue
		case *types.SelectObjectContentEventStreamMemberCont:
			// Handle continuation if needed
			continue
		case *types.SelectObjectContentEventStreamMemberEnd:
			// End of stream
			break
		}
	}

	return results, nil
}

// ================================
// ATHENA INTEGRATION (Alternative Query Method)
// ================================

// CreateAthenaTable creates an Athena table for querying with standard SQL
func CreateAthenaTable() string {
	return `
	CREATE EXTERNAL TABLE IF NOT EXISTS market_data (
		timestamp bigint,
		symbol string,
		open double,
		high double,
		low double,
		close double,
		volume bigint,
		vwap double,
		trade_count int
	)
	PARTITIONED BY (
		year int,
		month int,
		day int,
		hour int,
		symbol_partition string
	)
	STORED AS PARQUET
	LOCATION 's3://your-bucket/market-data/'
	TBLPROPERTIES ('parquet.compression'='SNAPPY');
	
	-- After creating table, run MSCK REPAIR TABLE to discover partitions
	MSCK REPAIR TABLE market_data;
	`
}

// Example Athena queries
func ExampleAthenaQueries() []string {
	return []string{
		// Query 1: Get hourly OHLC for a symbol
		`SELECT 
			date_trunc('hour', from_unixtime(timestamp/1000)) as hour,
			symbol,
			MIN(low) as low,
			MAX(high) as high,
			FIRST_VALUE(open) OVER (PARTITION BY date_trunc('hour', from_unixtime(timestamp/1000)) ORDER BY timestamp) as open,
			LAST_VALUE(close) OVER (PARTITION BY date_trunc('hour', from_unixtime(timestamp/1000)) ORDER BY timestamp) as close,
			SUM(volume) as total_volume
		FROM market_data
		WHERE symbol = 'AAPL'
			AND year = 2024
			AND month = 1
			AND day = 15
		GROUP BY date_trunc('hour', from_unixtime(timestamp/1000)), symbol`,

		// Query 2: Calculate moving averages
		`WITH price_data AS (
			SELECT 
				timestamp,
				symbol,
				close,
				AVG(close) OVER (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as sma_20,
				AVG(close) OVER (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) as sma_50
			FROM market_data
			WHERE symbol IN ('AAPL', 'GOOGL', 'MSFT')
				AND year = 2024
				AND month = 1
		)
		SELECT * FROM price_data
		WHERE sma_20 > sma_50`,

		// Query 3: Find high volume anomalies
		`WITH volume_stats AS (
			SELECT 
				symbol,
				AVG(volume) as avg_volume,
				STDDEV(volume) as stddev_volume
			FROM market_data
			WHERE year = 2024 AND month = 1
			GROUP BY symbol
		)
		SELECT 
			m.timestamp,
			m.symbol,
			m.volume,
			v.avg_volume,
			(m.volume - v.avg_volume) / v.stddev_volume as z_score
		FROM market_data m
		JOIN volume_stats v ON m.symbol = v.symbol
		WHERE m.year = 2024 
			AND m.month = 1 
			AND m.day = 15
			AND ABS((m.volume - v.avg_volume) / v.stddev_volume) > 3`,
	}
}

// ================================
// LAMBDA HANDLER
// ================================

type LambdaEvent struct {
	Action    string          `json:"action"`
	Symbol    string          `json:"symbol"`
	StartTime string          `json:"start_time"`
	EndTime   string          `json:"end_time"`
	Data      json.RawMessage `json:"data"`
}

func HandleRequest(ctx context.Context, event LambdaEvent) (interface{}, error) {
	// Load AWS configuration
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	s3Client := s3.NewFromConfig(cfg)
	bucketName := "your-trading-data-bucket"

	switch event.Action {
	case "store":
		var marketData []TestMarketData
		if err := json.Unmarshal(event.Data, &marketData); err != nil {
			return nil, fmt.Errorf("failed to unmarshal data: %w", err)
		}

		if err := StoreMarketDataToS3(ctx, s3Client, bucketName, marketData); err != nil {
			return nil, fmt.Errorf("failed to store data: %w", err)
		}

		return map[string]string{"status": "success", "records": fmt.Sprintf("%d", len(marketData))}, nil

	case "query":
		startTime, _ := time.Parse(time.RFC3339, event.StartTime)
		endTime, _ := time.Parse(time.RFC3339, event.EndTime)

		data, err := QueryMarketData(ctx, s3Client, bucketName, event.Symbol, startTime, endTime)
		if err != nil {
			return nil, fmt.Errorf("failed to query data: %w", err)
		}

		return data, nil

	default:
		return nil, fmt.Errorf("unknown action: %s", event.Action)
	}
}

func testMain() {
	lambda.Start(HandleRequest)
}

// ================================
// HELPER FUNCTIONS
// ================================

// generatePrefixesForDateRange generates S3 prefixes for efficient querying
func generatePrefixesForDateRange(dataType, symbol string, startTime, endTime time.Time) []string {
	var prefixes []string
	current := startTime

	for current.Before(endTime) || current.Equal(endTime) {
		// Generate prefix for each day
		prefix := fmt.Sprintf("%s/year=%d/month=%02d/day=%02d/",
			dataType,
			current.Year(),
			current.Month(),
			current.Day(),
		)

		// Add symbol-specific prefix if provided
		if symbol != "" {
			prefix = fmt.Sprintf("%shour=%02d/symbol=%s/", prefix, current.Hour(), symbol)
		}

		prefixes = append(prefixes, prefix)
		current = current.Add(24 * time.Hour)
	}

	return prefixes
}

// ================================
// USAGE EXAMPLES
// ================================

/*
EXAMPLE 1: Storing Data
-----------------------
curl -X POST https://your-api-gateway.execute-api.us-east-1.amazonaws.com/prod/market-data \
  -H "Content-Type: application/json" \
  -d '{
    "action": "store",
    "data": [
      {
        "timestamp": 1705325400000,
        "symbol": "AAPL",
        "open": 195.20,
        "high": 195.45,
        "low": 195.10,
        "close": 195.35,
        "volume": 1250000,
        "vwap": 195.28,
        "trade_count": 8500
      }
    ]
  }'

This will store the data in S3 at:
s3://your-bucket/market-data/year=2024/month=01/day=15/hour=09/symbol=AAPL/data_1705325400.parquet

EXAMPLE 2: Querying Data
------------------------
curl -X POST https://your-api-gateway.execute-api.us-east-1.amazonaws.com/prod/market-data \
  -H "Content-Type: application/json" \
  -d '{
    "action": "query",
    "symbol": "AAPL",
    "start_time": "2024-01-15T09:30:00Z",
    "end_time": "2024-01-15T16:00:00Z"
  }'

EXAMPLE 3: Using AWS CLI for Direct S3 Select Query
---------------------------------------------------
aws s3api select-object-content \
  --bucket your-trading-data-bucket \
  --key "market-data/year=2024/month=01/day=15/hour=09/symbol=AAPL/data_1705325400.parquet" \
  --expression "SELECT * FROM S3Object[*] WHERE close > 195.0" \
  --expression-type SQL \
  --input-serialization '{"Parquet": {}}' \
  --output-serialization '{"JSON": {"RecordDelimiter": "\n"}}' \
  output.json

EXAMPLE 4: Athena Query via AWS CLI
------------------------------------
aws athena start-query-execution \
  --query-string "SELECT symbol, AVG(close) as avg_close, MAX(volume) as max_volume FROM market_data WHERE year=2024 AND month=1 AND day=15 GROUP BY symbol" \
  --result-configuration "OutputLocation=s3://your-athena-results-bucket/" \
  --work-group "primary"

PARTITION STRUCTURE BENEFITS:
-----------------------------
1. Cost Efficiency: Query only relevant partitions, reducing data scanned
2. Performance: Parallel processing of partitions
3. Maintenance: Easy to archive/delete old data by partition
4. Compliance: Simple data retention policies by date

ESTIMATED QUERY COSTS:
----------------------
- S3 Select: $0.002 per GB scanned + $0.0007 per GB returned
- Athena: $5 per TB scanned (partition pruning reduces this significantly)
- Direct S3 GET: $0.0004 per 1000 requests + data transfer costs

For 1 year of minute data for 100 symbols:
- Raw size: ~50GB
- Compressed (Parquet + Snappy): ~5-10GB
- With proper partitioning, typical query scans: 10-100MB
- Athena query cost: ~$0.00005 per query
*/
