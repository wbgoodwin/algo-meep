package main

import (
	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata"
	"github.com/jonreiter/govader"
)

// Response represents the Lambda function response
type Response struct {
	Message    string     `json:"message"`
	MarketData MarketData `json:"market_data"`
}

// MarketData contains the collected market data
type MarketData struct {
	Bars          []marketdata.Bar
	NewsSentiment govader.Sentiment
}

// Time represents a time input with optional hour/minute/second
type Time struct {
	Year   int `json:"year"`
	Month  int `json:"month"`
	Day    int `json:"day"`
	Hour   int `json:"hour,omitempty"`
	Minute int `json:"minute,omitempty"`
	Second int `json:"second,omitempty"`
}

// Input represents the Lambda function input
type Input struct {
	Symbol string `json:"symbol"`
	Start  Time   `json:"start"`
	End    Time   `json:"end"`
}

// ParquetBar is a struct for Parquet serialization optimized for Athena/S3 tables
type ParquetBar struct {
	Symbol     string  `parquet:"name=symbol, type=BYTE_ARRAY, convertedtype=UTF8"`
	Timestamp  int64   `parquet:"name=timestamp, type=INT64"`
	Open       float64 `parquet:"name=open, type=DOUBLE"`
	High       float64 `parquet:"name=high, type=DOUBLE"`
	Low        float64 `parquet:"name=low, type=DOUBLE"`
	Close      float64 `parquet:"name=close, type=DOUBLE"`
	Volume     int64   `parquet:"name=volume, type=INT64"`
	TradeCount int64   `parquet:"name=trade_count, type=INT64"`
	VWAP       float64 `parquet:"name=vwap, type=DOUBLE"`
	Date       string  `parquet:"name=date, type=BYTE_ARRAY, convertedtype=UTF8"`
}

// ParquetNews is a struct for news sentiment data in Parquet optimized for Athena/S3 tables
type ParquetNews struct {
	Symbol    string  `parquet:"name=symbol, type=BYTE_ARRAY, convertedtype=UTF8"`
	Timestamp int64   `parquet:"name=timestamp, type=INT64"`
	Headline  string  `parquet:"name=headline, type=BYTE_ARRAY, convertedtype=UTF8"`
	Summary   string  `parquet:"name=summary, type=BYTE_ARRAY, convertedtype=UTF8"`
	Positive  float64 `parquet:"name=positive, type=DOUBLE"`
	Negative  float64 `parquet:"name=negative, type=DOUBLE"`
	Neutral   float64 `parquet:"name=neutral, type=DOUBLE"`
	Compound  float64 `parquet:"name=compound, type=DOUBLE"`
	Date      string  `parquet:"name=date, type=BYTE_ARRAY, convertedtype=UTF8"`
}
