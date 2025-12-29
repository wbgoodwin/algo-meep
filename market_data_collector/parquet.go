package main

import (
	"bytes"

	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata"
	"github.com/jonreiter/govader"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

// barsToParquet transforms bars to Parquet bytes optimized for S3/Athena
func barsToParquet(bars []marketdata.Bar, symbol string) ([]byte, error) {
	buf := new(bytes.Buffer)
	pw, err := writer.NewParquetWriterFromWriter(buf, new(ParquetBar), 1)
	if err != nil {
		return nil, err
	}

	// Set compression for better storage efficiency
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for _, bar := range bars {
		pb := ParquetBar{
			Symbol:     symbol,
			Timestamp:  bar.Timestamp.UnixMilli(), // Use milliseconds for better precision
			Open:       bar.Open,
			High:       bar.High,
			Low:        bar.Low,
			Close:      bar.Close,
			Volume:     int64(bar.Volume),
			TradeCount: 0,                                  // Alpaca doesn't provide trade count in bar data
			VWAP:       0,                                  // Alpaca doesn't provide VWAP in bar data
			Date:       bar.Timestamp.Format("2006-01-02"), // Add date partition field
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

// newsToParquet transforms news and sentiment to Parquet bytes optimized for S3/Athena
func newsToParquet(news []marketdata.News, sentiment govader.Sentiment, symbol string) ([]byte, error) {
	buf := new(bytes.Buffer)
	pw, err := writer.NewParquetWriterFromWriter(buf, new(ParquetNews), 1)
	if err != nil {
		return nil, err
	}

	// Set compression for better storage efficiency
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	analyzer := govader.NewSentimentIntensityAnalyzer()
	for _, newsItem := range news {
		itemSentiment := analyzer.PolarityScores(newsItem.Headline + " " + newsItem.Summary)
		pn := ParquetNews{
			Symbol:    symbol,
			Timestamp: newsItem.CreatedAt.UnixMilli(), // Use milliseconds for better precision
			Headline:  newsItem.Headline,
			Summary:   newsItem.Summary,
			Positive:  itemSentiment.Positive,
			Negative:  itemSentiment.Negative,
			Neutral:   itemSentiment.Neutral,
			Compound:  itemSentiment.Compound,
			Date:      newsItem.CreatedAt.Format("2006-01-02"), // Add date partition field
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
