from aws_cdk import Stack
from constructs import Construct

from .market_data_collector_stack import MarketDataCollectorStack


class InfrastructureStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Deploy the Market Data Collector stack
        market_data_collector = MarketDataCollectorStack(
            self, "MarketDataCollector",
            **kwargs
        )