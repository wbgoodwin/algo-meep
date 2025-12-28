#!/bin/bash

# Build script for Market Data Collector
set -e

echo "📦 Building Market Data Collector..."

cd ./market_data_collector
GOOS=linux GOARCH=arm64 go build -o bootstrap .
cd ..

# Create zip file
echo "📁 Creating deployment package..."
zip -j ./market_data_collector/bootstrap.zip ./market_data_collector/bootstrap

# Move to infrastructure directory
mkdir -p ./infrastructure/cdk.out
mv ./market_data_collector/bootstrap.zip ./infrastructure/cdk.out/

echo "✅ Build complete!"
echo "✅ Artifact created: infrastructure/cdk.out/bootstrap.zip"
echo "✅ Ready for deployment"
