.PHONY: build build-api clean test test-ci test-cover test-cover-html tidy

# Build all Lambda binaries for ARM64 Linux (matching existing pipeline pattern)
build: build-api

build-api:
	GOOS=linux GOARCH=arm64 go build -o build/api/bootstrap ./cmd/api/

# Clean build artifacts
clean:
	rm -rf build/

# Run tests
test:
	go test ./... -v -count=1

# Run tests with race detection (mirrors CI)
test-ci:
	go test ./... -v -count=1 -race -timeout 5m

# Run tests with coverage report
test-cover:
	go test ./... -coverprofile=coverage.out -count=1
	go tool cover -func=coverage.out
	@echo "---"
	@go tool cover -func=coverage.out | tail -1

# Generate HTML coverage report
test-cover-html: test-cover
	go tool cover -html=coverage.out -o coverage.html
	@echo "Open coverage.html in a browser to view"

# Resolve dependencies
tidy:
	go mod tidy

# Zip for deployment (matches existing CodePipeline pattern)
zip: build
	cd build/api && zip -j ../../build/api-bootstrap.zip bootstrap

# Local development — run the handler locally (requires SAM CLI or similar)
local:
	go run ./cmd/api/
