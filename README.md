# algo-meep

An algorithmic trading platform with efficient market data collection and storage capabilities.

## Market Data Collector

The market data collector is a serverless application that:
- Collects historical market data from Alpaca
- Performs sentiment analysis on news articles
- Stores data in Parquet format for efficient compression and querying
- Uploads data to S3 with intelligent partitioning

### Features

- **Efficient Storage**: Uses Parquet format for columnar storage and compression
- **Intelligent Partitioning**: Data is partitioned by symbol, year, month, and day for optimal query performance
- **Sentiment Analysis**: Integrates VADER sentiment analysis for news sentiment scoring
- **Serverless**: Built as an AWS Lambda function for scalability
- **Environment Configuration**: Uses environment variables for flexible deployment

### Data Schema

#### Bar Data (ParquetBar)
```
symbol: STRING     - Stock symbol (e.g., "AAPL")
timestamp: INT64   - Unix timestamp
open: DOUBLE       - Opening price
high: DOUBLE       - Highest price
low: DOUBLE        - Lowest price
close: DOUBLE      - Closing price
volume: INT64      - Trading volume
trade_count: INT64 - Number of trades (placeholder)
vwap: DOUBLE       - Volume-weighted average price (placeholder)
```

#### News Sentiment Data (ParquetNews)
```
symbol: STRING     - Stock symbol
timestamp: INT64   - News article creation time
headline: STRING   - News article headline
summary: STRING    - News article summary
sentiment: DOUBLE  - Overall sentiment score
positive: DOUBLE   - Positive sentiment score
negative: DOUBLE   - Negative sentiment score
neutral: DOUBLE    - Neutral sentiment score
compound: DOUBLE   - Compound sentiment score
```

### S3 Storage Structure

Data is stored in S3 with the following partitioning structure:
```
s3://your-bucket/market_data/bars/symbol=AAPL/year=2024/month=01/day=15/bars_20240115_143022.parquet
s3://your-bucket/market_data/news/symbol=AAPL/year=2024/month=01/day=15/news_20240115_143022.parquet
```

This partitioning enables efficient querying with tools like AWS Athena, Glue, or Spark.

### Environment Variables

- `S3_BUCKET`: The S3 bucket name where data will be stored
- AWS credentials are automatically handled via Lambda execution role

### API Usage

#### Request Format
```json
{
  "symbol": "AAPL",
  "start": {
    "year": 2024,
    "month": 1,
    "day": 15,
    "hour": 9,
    "minute": 30
  },
  "end": {
    "year": 2024,
    "month": 1,
    "day": 15,
    "hour": 16,
    "minute": 0
  }
}
```

#### Response Format
```json
{
  "message": "Success",
  "market_data": {
    "bars": [...],
    "news_sentiment": {
      "positive": 0.0,
      "negative": 0.0,
      "neutral": 1.0,
      "compound": 0.0
    }
  }
}
```

### Deployment

1. Set the `S3_BUCKET` environment variable in your Lambda configuration
2. Deploy using the Makefile:
   ```bash
   make deploy
   ```

### Querying Data

With the partitioned structure, you can efficiently query data using AWS Athena:

```sql
-- Query all AAPL data for January 2024
SELECT * FROM market_data_bars 
WHERE symbol = 'AAPL' 
  AND year = 2024 
  AND month = 1;

-- Query high volatility days (high price spread)
SELECT symbol, date, high - low as price_spread
FROM market_data_bars 
WHERE year = 2024 
  AND high - low > 10;
```

### Performance Benefits

- **Compression**: Parquet provides 10-100x better compression than CSV
- **Query Performance**: Columnar storage enables faster analytical queries
- **Partition Pruning**: S3 partitioning reduces data scanned in queries
- **Cost Efficiency**: Reduced data transfer and storage costs
