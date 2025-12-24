package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jonreiter/govader"
	"github.com/xitongsys/parquet-go/writer"
)

var client *marketdata.Client
var analyzer *govader.SentimentIntensityAnalyzer

type Response struct {
	Message    string     `json:"message"`
	MarketData MarketData `json:"market_data"`
}

type MarketData struct {
	Bars          []marketdata.Bar
	NewsSentiment govader.Sentiment
}

type Time struct {
	Year   int `json:"year"`
	Month  int `json:"month"`
	Day    int `json:"day"`
	Hour   int `json:"hour,omitempty"`
	Minute int `json:"minute,omitempty"`
	Second int `json:"second,omitempty"`
}

type Input struct {
	Symbol string `json:"symbol"`
	Start  Time   `json:"start"`
	End    Time   `json:"end"`
}

func bars(input Input) []marketdata.Bar {
	bars, err := client.GetBars(input.Symbol, marketdata.GetBarsRequest{
		TimeFrame: marketdata.OneDay,
		Start:     time.Date(input.Start.Year, time.Month(input.Start.Month), input.Start.Day, input.Start.Hour, input.Start.Minute, input.Start.Second, 0, time.UTC),
		End:       time.Date(input.End.Year, time.Month(input.End.Month), input.End.Day, input.End.Hour, input.End.Minute, input.End.Second, 0, time.UTC),
		AsOf:      time.Now().Add(-30 * time.Hour).Format("2006-01-02"),
	})
	must(err)
	return bars
}

func news(input Input) []marketdata.News {
	news, err := client.GetNews(marketdata.GetNewsRequest{
		Symbols:    []string{input.Symbol},
		Start:      time.Date(input.Start.Year, time.Month(input.Start.Month), input.Start.Day, input.Start.Hour, input.Start.Minute, input.Start.Second, 0, time.UTC),
		End:        time.Date(input.End.Year, time.Month(input.End.Month), input.End.Day, input.End.Hour, input.End.Minute, input.End.Second, 0, time.UTC),
		TotalLimit: 2,
	})
	must(err)
	return news
}

// ParquetBar is a struct for Parquet serialization
type ParquetBar struct {
	Symbol     string  `parquet:"name=symbol, type=STRING"`
	Timestamp  int64   `parquet:"name=timestamp, type=INT64"`
	Open       float64 `parquet:"name=open, type=DOUBLE"`
	High       float64 `parquet:"name=high, type=DOUBLE"`
	Low        float64 `parquet:"name=low, type=DOUBLE"`
	Close      float64 `parquet:"name=close, type=DOUBLE"`
	Volume     int64   `parquet:"name=volume, type=INT64"`
	TradeCount int64   `parquet:"name=trade_count, type=INT64"`
	VWAP       float64 `parquet:"name=vwap, type=DOUBLE"`
}

// ParquetNews is a struct for news sentiment data in Parquet
type ParquetNews struct {
	Symbol    string  `parquet:"name=symbol, type=STRING"`
	Timestamp int64   `parquet:"name=timestamp, type=INT64"`
	Headline  string  `parquet:"name=headline, type=STRING"`
	Summary   string  `parquet:"name=summary, type=STRING"`
	Sentiment float64 `parquet:"name=sentiment, type=DOUBLE"`
	Positive  float64 `parquet:"name=positive, type=DOUBLE"`
	Negative  float64 `parquet:"name=negative, type=DOUBLE"`
	Neutral   float64 `parquet:"name=neutral, type=DOUBLE"`
	Compound  float64 `parquet:"name=compound, type=DOUBLE"`
}

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

func collectMarketData(input Input) MarketData {
	bars := bars(input)
	news := news(input)

	analyzer = govader.NewSentimentIntensityAnalyzer()
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

	return MarketData{Bars: bars, NewsSentiment: sentiment}
}

func quotes() {
	quotes, err := client.GetQuotes("AAPL", marketdata.GetQuotesRequest{
		Start:      time.Date(2021, 8, 9, 13, 30, 0, 0, time.UTC),
		TotalLimit: 30,
	})
	must(err)
	fmt.Println("AAPL quotes:")
	for _, quote := range quotes {
		fmt.Printf("%+v\n", quote)
	}
}

func handler(ctx context.Context, event json.RawMessage) (Response, error) {
	var input Input
	if err := json.Unmarshal(event, &input); err != nil {
		return Response{Message: "Failed to unmarshal event"}, err
	}
	if client == nil {
		keys, err := GetApiKeys()
		if err != nil {
			return Response{Message: "Failed to get the API Keys from SSM: " + err.Error()}, err
		}
		client = marketdata.NewClient(marketdata.ClientOpts{
			APIKey:    keys.ApiKey,
			APISecret: keys.ApiSecret,
			BaseURL:   "https://paper-api.alpaca.markets/v2",
		})
	}

	md := collectMarketData(input)
	return Response{Message: "Success", MarketData: md}, nil
}

func main() {
	lambda.Start(handler)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
