package main

import (
	"fmt"
	"time"

	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata"
)

// bars fetches historical bar data for a symbol
func bars(input Input) ([]marketdata.Bar, error) {
	logger.Debug("Fetching bars data", WithSymbol(input.Symbol), WithFunction("bars"))

	bars, err := client.GetBars(input.Symbol, marketdata.GetBarsRequest{
		TimeFrame: marketdata.OneDay,
		Start:     time.Date(input.Start.Year, time.Month(input.Start.Month), input.Start.Day, input.Start.Hour, input.Start.Minute, input.Start.Second, 0, time.UTC),
		End:       time.Date(input.End.Year, time.Month(input.End.Month), input.End.Day, input.End.Hour, input.End.Minute, input.End.Second, 0, time.UTC),
	})
	if err != nil {
		logger.Error("Failed to fetch bars", err, WithSymbol(input.Symbol), WithFunction("bars"))
		return nil, fmt.Errorf("failed to fetch bars for %s: %w", input.Symbol, err)
	}

	logger.Info("Successfully fetched bars data", WithSymbol(input.Symbol), WithFunction("bars"))
	return bars, nil
}

// news fetches news data for a symbol
func news(input Input) ([]marketdata.News, error) {
	logger.Debug("Fetching news data", WithSymbol(input.Symbol), WithFunction("news"))

	news, err := client.GetNews(marketdata.GetNewsRequest{
		Symbols:    []string{input.Symbol},
		Start:      time.Date(input.Start.Year, time.Month(input.Start.Month), input.Start.Day, input.Start.Hour, input.Start.Minute, input.Start.Second, 0, time.UTC),
		End:        time.Date(input.End.Year, time.Month(input.End.Month), input.End.Day, input.End.Hour, input.End.Minute, input.End.Second, 0, time.UTC),
		TotalLimit: 2,
	})
	if err != nil {
		logger.Error("Failed to fetch news", err, WithSymbol(input.Symbol), WithFunction("news"))
		return nil, fmt.Errorf("failed to fetch news for %s: %w", input.Symbol, err)
	}

	logger.Info("Successfully fetched news data", WithSymbol(input.Symbol), WithFunction("news"))
	return news, nil
}
