package main

import (
	"context"
	"encoding/json"

	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata"
	"github.com/aws/aws-lambda-go/lambda"
)

var client *marketdata.Client

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
			BaseURL:   "https://data.alpaca.markets",
		})
	}

	md, err := collectMarketData(input)
	if err != nil {
		return Response{Message: "Failed to collect market data: " + err.Error()}, err
	}
	return Response{Message: "Success", MarketData: md}, nil
}

func main() {
	lambda.Start(handler)
}
