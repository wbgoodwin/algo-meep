#!/bin/bash

# Comprehensive hot reload development script for market_data_collector
# This script:
# 1. Starts LocalStack if not running
# 2. Builds and deploys the Lambda function
# 3. Watches for file changes and hot reloads the Lambda
# Usage: ./hot_reload_dev.sh

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MARKET_DATA_DIR="$PROJECT_ROOT/market_data_collector"
BUILD_DIR="$MARKET_DATA_DIR/build"
HOT_RELOAD_DIR="$BUILD_DIR/hot"
INFRASTRUCTURE_DIR="$PROJECT_ROOT/infrastructure"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Market Data Collector Hot Reload Development ===${NC}"
echo ""

# Step 1: Check dependencies
echo -e "${YELLOW}[1/5] Checking dependencies...${NC}"
if ! command -v go &> /dev/null; then
    echo -e "${RED}Error: Go is not installed${NC}"
    exit 1
fi

if ! command -v docker &> /dev/null; then
    echo -e "${RED}Error: Docker is not installed${NC}"
    exit 1
fi

if ! command -v gowatch &> /dev/null; then
    echo -e "${YELLOW}Installing gowatch...${NC}"
    go install github.com/silenceper/gowatch@latest
fi

echo -e "${GREEN}✓ Dependencies OK${NC}"
echo ""

# Step 2: Start LocalStack
echo -e "${YELLOW}[2/5] Starting LocalStack...${NC}"

echo "Starting LocalStack container..."
ACTIVATE_PRO=0 localstack start -d
echo "Waiting for LocalStack to be ready..."
sleep 3
# Wait for LocalStack health check
for i in {1..30}; do
    if curl -s http://localhost:4566/_localstack/health | grep -q '"services"'; then
        echo -e "${GREEN}✓ LocalStack is healthy${NC}"
        break
    fi
    echo "Waiting for LocalStack... ($i/30)"
    sleep 1
done
echo -e "${GREEN}✓ LocalStack started${NC}"
echo ""

# Step 3: Set environment variables for LocalStack
echo -e "${YELLOW}[3/5] Setting up environment...${NC}"
export STAGE=local
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_ENDPOINT_URL_S3=http://s3.localhost.localstack.cloud:4566
export CDK_DEFAULT_ACCOUNT=000000000000
export AWS_ACCOUNT_ID=000000000000
export CDK_DEFAULT_REGION=us-east-1
export LAMBDA_MOUNT_CWD="$HOT_RELOAD_DIR"
export ENVIRONMENT=local

echo -e "${GREEN}✓ Environment configured${NC}"
echo ""

# Step 4: Build and deploy Lambda
echo -e "${YELLOW}[4/5] Building and deploying Lambda...${NC}"

# Create hot reload directory
mkdir -p "$HOT_RELOAD_DIR"

# Build Lambda
echo "Building Lambda function..."
cd "$MARKET_DATA_DIR"
GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o "$BUILD_DIR/bootstrap" .

# Create zip for deployment
cd "$BUILD_DIR"
zip -r bootstrap.zip bootstrap

# Deploy with CDK
echo "Deploying with CDK..."
cd "$INFRASTRUCTURE_DIR"

# Bootstrap CDK (required for LocalStack)
echo "Bootstrapping CDK for LocalStack..."
cdklocal bootstrap aws://000000000000/us-east-1 --force 2>&1 | grep -v "UserWarning" || true

# Deploy the stack using local app
echo "Deploying Lambda stack..."
cdklocal deploy --app "python local_app.py" --require-approval=never 2>&1 | grep -v "UserWarning" || true

echo -e "${GREEN}✓ Lambda deployed${NC}"
echo ""

# Step 5: Start hot reload watcher
echo -e "${YELLOW}[5/5] Starting hot reload watcher...${NC}"
echo ""
echo -e "${GREEN}=== Hot Reload Active ===${NC}"
echo ""
echo "File changes in $MARKET_DATA_DIR will automatically:"
echo "  1. Rebuild the Go binary"
echo "  2. Create bootstrap.zip"
echo "  3. Update the Lambda function in LocalStack"
echo ""
echo "To test your Lambda:"
if command -v awslocal &> /dev/null; then
    echo "  awslocal lambda invoke --function-name algo-meep-market-data-collector --cli-binary-format raw-in-base64-out --profile localstack --payload '{\"symbol\": \"AAPL\", \"start\": {\"year\": 2025, \"month\": 11, \"day\": 5}, \"end\": {\"year\": 2025, \"month\": 11, \"day\": 6}}' response.json"
else
    echo "  aws --endpoint-url=http://localhost:4566 lambda invoke --function-name algo-meep-market-data-collector --cli-binary-format raw-in-base64-out --payload '{\"symbol\": \"AAPL\", \"start\": {\"year\": 2025, \"month\": 11, \"day\": 5}, \"end\": {\"year\": 2025, \"month\": 11, \"day\": 6}}' response.json"
fi

echo ""
echo "Press Ctrl+C to stop hot reload"
echo ""

# Use entr to watch Go source files and rebuild on changes
cd "$MARKET_DATA_DIR"
find . -name '*.go' | entr -d -r sh -c '
    echo "=== Rebuilding Lambda ==="
    cd "$MARKET_DATA_DIR"
    GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o build/bootstrap .
    cd build
    zip -qr bootstrap.zip bootstrap
    echo "=== Updating Lambda in LocalStack ==="
    cd ../../infrastructure
    cdklocal deploy --app "python local_app.py" --require-approval=never
    echo "=== Update complete ==="
'
