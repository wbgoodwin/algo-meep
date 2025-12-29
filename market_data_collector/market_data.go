package main

import (
	"fmt"
	"time"

	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata"
)

// bars fetches historical bar data for a symbol
func bars(input Input) ([]marketdata.Bar, error) {
	bars, err := client.GetBars(input.Symbol, marketdata.GetBarsRequest{
		TimeFrame: marketdata.OneDay,
		Start:     time.Date(input.Start.Year, time.Month(input.Start.Month), input.Start.Day, input.Start.Hour, input.Start.Minute, input.Start.Second, 0, time.UTC),
		End:       time.Date(input.End.Year, time.Month(input.End.Month), input.End.Day, input.End.Hour, input.End.Minute, input.End.Second, 0, time.UTC),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch bars for %s: %w", input.Symbol, err)
	}
	return bars, nil
}

// news fetches news data for a symbol
func news(input Input) ([]marketdata.News, error) {
	news, err := client.GetNews(marketdata.GetNewsRequest{
		Symbols:    []string{input.Symbol},
		Start:      time.Date(input.Start.Year, time.Month(input.Start.Month), input.Start.Day, input.Start.Hour, input.Start.Minute, input.Start.Second, 0, time.UTC),
		End:        time.Date(input.End.Year, time.Month(input.End.Month), input.End.Day, input.End.Hour, input.End.Minute, input.End.Second, 0, time.UTC),
		TotalLimit: 2,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch news for %s: %w", input.Symbol, err)
	}
	return news, nil
}
