#!/usr/bin/env python3
import os

import aws_cdk as cdk
from stacks.market_data_collector_stack import MarketDataCollectorStack

app = cdk.App()

# Create the market data collector stack for local development
MarketDataCollectorStack(app, "MarketDataCollectorStack", 
    env=cdk.Environment(
        account=os.getenv("AWS_ACCOUNT_ID", "000000000000"),
        region=os.getenv("CDK_DEFAULT_REGION", "us-east-1")
    ),
    environment_variables={
        "ALPACA_API_KEY": os.getenv("ALPACA_API_KEY"),
        "ALPACA_API_SECRET": os.getenv("ALPACA_API_SECRET"),
        "LOCALSTACK_ENDPOINT": "http://localhost:4566",
    }
)

app.synth()
