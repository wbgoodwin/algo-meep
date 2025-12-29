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
    )
)

app.synth()
