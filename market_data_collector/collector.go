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
	logger.Debug("Starting market data collection", WithSymbol(input.Symbol), WithFunction("collectMarketData"))

	bars, err := bars(input)
	if err != nil {
		logger.Error("Bars API call failed", err, WithSymbol(input.Symbol), WithFunction("collectMarketData"))
		return MarketData{}, fmt.Errorf("bars API call failed: %w", err)
	}

	news, err := news(input)
	if err != nil {
		logger.Error("News API call failed", err, WithSymbol(input.Symbol), WithFunction("collectMarketData"))
		return MarketData{}, fmt.Errorf("news API call failed: %w", err)
	}

	if analyzer == nil {
		logger.Debug("Initializing sentiment analyzer", WithFunction("collectMarketData"))
		analyzer = govader.NewSentimentIntensityAnalyzer()
	}

	var sentiment govader.Sentiment
	if len(news) == 0 {
		logger.Debug("No news available, using default sentiment", WithSymbol(input.Symbol), WithFunction("collectMarketData"))
		sentiment = analyzer.PolarityScores("No news available")
	} else {
		logger.Debug("Analyzing sentiment for news", WithSymbol(input.Symbol), WithFunction("collectMarketData"))
		sentiment = analyzer.PolarityScores(news[0].Headline + " " + news[0].Summary + " " + news[0].Content)
	}

	// Transform bars to Parquet and upload to S3
	barsParquetBytes, err := barsToParquet(bars, input.Symbol)
	if err != nil {
		logger.Error("Failed to transform bars to Parquet", err, WithSymbol(input.Symbol), WithFunction("barsToParquet"))
	} else {
		// Partition by symbol and date for efficient querying
		partition := fmt.Sprintf("symbol=%s/year=%d/month=%02d/day=%02d",
			input.Symbol, input.Start.Year, input.Start.Month, input.Start.Day)
		filename := fmt.Sprintf("bars_%s.parquet", time.Now().Format("20060102_150405"))
		s3Key := path.Join(s3Prefix, "bars", partition, filename)
		if err := uploadToS3(barsParquetBytes, s3Key); err != nil {
			logger.Error("Failed to upload bars Parquet to S3", err, WithSymbol(input.Symbol), WithFunction("uploadToS3"))
		} else {
			logger.Info("Successfully uploaded bars data to S3",
				WithSymbol(input.Symbol),
				WithS3Info(os.Getenv("S3_BUCKET"), s3Key))
		}
	}

	// Transform news to Parquet and upload to S3
	newsParquetBytes, err := newsToParquet(news, sentiment, input.Symbol)
	if err != nil {
		logger.Error("Failed to transform news to Parquet", err, WithSymbol(input.Symbol), WithFunction("newsToParquet"))
	} else {
		// Partition by symbol and date for efficient querying
		partition := fmt.Sprintf("symbol=%s/year=%d/month=%02d/day=%02d",
			input.Symbol, input.Start.Year, input.Start.Month, input.Start.Day)
		filename := fmt.Sprintf("news_%s.parquet", time.Now().Format("20060102_150405"))
		s3Key := path.Join(s3Prefix, "news", partition, filename)
		if err := uploadToS3(newsParquetBytes, s3Key); err != nil {
			logger.Error("Failed to upload news Parquet to S3", err, WithSymbol(input.Symbol), WithFunction("uploadToS3"))
		} else {
			logger.Info("Successfully uploaded news data to S3",
				WithSymbol(input.Symbol),
				WithS3Info(os.Getenv("S3_BUCKET"), s3Key))
		}
	}

	return MarketData{Bars: bars, NewsSentiment: sentiment}, nil
}
