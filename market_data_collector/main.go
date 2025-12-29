package main

import (
	"context"
	"encoding/json"

	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata"
	"github.com/aws/aws-lambda-go/lambda"
)

var client *marketdata.Client

func handler(ctx context.Context, event json.RawMessage) (Response, error) {
	logger.Debug("Starting Lambda handler", WithFunction("handler"))

	var input Input
	if err := json.Unmarshal(event, &input); err != nil {
		logger.Error("Failed to unmarshal event", err, WithFunction("handler"))
		return Response{Message: "Failed to unmarshal event"}, err
	}

	logger.Info("Processing market data request", WithSymbol(input.Symbol), WithFunction("handler"))

	if client == nil {
		logger.Debug("Initializing Alpaca client", WithFunction("handler"))
		keys, err := GetApiKeys()
		if err != nil {
			logger.Error("Failed to get API keys", err, WithFunction("handler"))
			return Response{Message: "Failed to get the API Keys from SSM: " + err.Error()}, err
		}
		client = marketdata.NewClient(marketdata.ClientOpts{
			APIKey:    keys.ApiKey,
			APISecret: keys.ApiSecret,
			BaseURL:   "https://data.alpaca.markets",
		})
		logger.Info("Successfully initialized Alpaca client", WithFunction("handler"))
	}

	md, err := collectMarketData(input)
	if err != nil {
		logger.Error("Failed to collect market data", err, WithSymbol(input.Symbol), WithFunction("handler"))
		return Response{Message: "Failed to collect market data: " + err.Error()}, err
	}

	logger.Info("Successfully processed market data request", WithSymbol(input.Symbol), WithFunction("handler"))
	return Response{Message: "Success", MarketData: md}, nil
}

func main() {
	lambda.Start(handler)
}
