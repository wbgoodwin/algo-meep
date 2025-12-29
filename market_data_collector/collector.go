package main

import (
	"fmt"
	"os"
	"path"
	"time"

	"github.com/jonreiter/govader"
)

var analyzer *govader.SentimentIntensityAnalyzer

// collectMarketData orchestrates the collection of market data
func collectMarketData(input Input) (MarketData, error) {
	bars, err := bars(input)
	if err != nil {
		return MarketData{}, fmt.Errorf("bars API call failed: %w", err)
	}

	news, err := news(input)
	if err != nil {
		return MarketData{}, fmt.Errorf("news API call failed: %w", err)
	}

	if analyzer == nil {
		analyzer = govader.NewSentimentIntensityAnalyzer()
	}
	var sentiment govader.Sentiment
	if len(news) == 0 {
		sentiment = analyzer.PolarityScores("No news available")
	} else {
		sentiment = analyzer.PolarityScores(news[0].Headline + " " + news[0].Summary + " " + news[0].Content)
	}

	// Transform bars to Parquet and upload to S3
	barsParquetBytes, err := barsToParquet(bars, input.Symbol)
	if err != nil {
		fmt.Printf("Failed to transform bars to Parquet: %v\n", err)
	} else {
		// Partition by symbol and date for efficient querying
		partition := fmt.Sprintf("symbol=%s/year=%d/month=%02d/day=%02d",
			input.Symbol, input.Start.Year, input.Start.Month, input.Start.Day)
		filename := fmt.Sprintf("bars_%s.parquet", time.Now().Format("20060102_150405"))
		s3Key := path.Join(s3Prefix, "bars", partition, filename)
		if err := uploadToS3(barsParquetBytes, s3Key); err != nil {
			fmt.Printf("Failed to upload bars Parquet to S3: %v\n", err)
		} else {
			fmt.Printf("Successfully uploaded bars data to s3://%s/%s\n", os.Getenv("S3_BUCKET"), s3Key)
		}
	}

	// Transform news to Parquet and upload to S3
	newsParquetBytes, err := newsToParquet(news, sentiment, input.Symbol)
	if err != nil {
		fmt.Printf("Failed to transform news to Parquet: %v\n", err)
	} else {
		// Partition by symbol and date for efficient querying
		partition := fmt.Sprintf("symbol=%s/year=%d/month=%02d/day=%02d",
			input.Symbol, input.Start.Year, input.Start.Month, input.Start.Day)
		filename := fmt.Sprintf("news_%s.parquet", time.Now().Format("20060102_150405"))
		s3Key := path.Join(s3Prefix, "news", partition, filename)
		if err := uploadToS3(newsParquetBytes, s3Key); err != nil {
			fmt.Printf("Failed to upload news Parquet to S3: %v\n", err)
		} else {
			fmt.Printf("Successfully uploaded news data to s3://%s/%s\n", os.Getenv("S3_BUCKET"), s3Key)
		}
	}

	return MarketData{Bars: bars, NewsSentiment: sentiment}, nil
}
