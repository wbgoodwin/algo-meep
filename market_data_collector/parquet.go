package main

import (
	"bytes"

	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata"
	"github.com/jonreiter/govader"
	"github.com/xitongsys/parquet-go/writer"
)

// barsToParquet transforms bars to Parquet bytes
func barsToParquet(bars []marketdata.Bar, symbol string) ([]byte, error) {
	buf := new(bytes.Buffer)
	pw, err := writer.NewParquetWriterFromWriter(buf, new(ParquetBar), 1)
	if err != nil {
		return nil, err
	}
	for _, bar := range bars {
		pb := ParquetBar{
			Symbol:     symbol,
			Timestamp:  bar.Timestamp.Unix(),
			Open:       bar.Open,
			High:       bar.High,
			Low:        bar.Low,
			Close:      bar.Close,
			Volume:     int64(bar.Volume),
			TradeCount: 0, // Alpaca doesn't provide trade count in bar data
			VWAP:       0, // Alpaca doesn't provide VWAP in bar data
		}
		if err := pw.Write(pb); err != nil {
			return nil, err
		}
	}
	if err := pw.WriteStop(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// newsToParquet transforms news and sentiment to Parquet bytes
func newsToParquet(news []marketdata.News, sentiment govader.Sentiment, symbol string) ([]byte, error) {
	buf := new(bytes.Buffer)
	pw, err := writer.NewParquetWriterFromWriter(buf, new(ParquetNews), 1)
	if err != nil {
		return nil, err
	}
	for _, newsItem := range news {
		pn := ParquetNews{
			Symbol:    symbol,
			Timestamp: newsItem.CreatedAt.Unix(),
			Headline:  newsItem.Headline,
			Summary:   newsItem.Summary,
			Sentiment: sentiment.Compound,
			Positive:  sentiment.Positive,
			Negative:  sentiment.Negative,
			Neutral:   sentiment.Neutral,
			Compound:  sentiment.Compound,
		}
		if err := pw.Write(pn); err != nil {
			return nil, err
		}
	}
	if err := pw.WriteStop(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
