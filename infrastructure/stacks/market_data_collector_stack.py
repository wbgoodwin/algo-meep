from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_s3 as s3,
    RemovalPolicy,
    CfnOutput,
    Duration,
    aws_logs as _logs,
)
from constructs import Construct
import os


class MarketDataCollectorStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, bootstrap_artifact=None, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        self.bootstrap_artifact = bootstrap_artifact

        # Environment configuration for hot reload
        stage = os.getenv("STAGE", "dev")
        lambda_mount_cwd = os.getenv("LAMBDA_MOUNT_CWD", "")

        # S3 Bucket for market data storage
        market_data_bucket = s3.Bucket(
            self, "MarketDataBucket",
            bucket_name=f"algo-meep-market-data-{self.account}-{self.region}",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,  # For dev - change to RETAIN for prod
            auto_delete_objects=True,  # For dev - remove for prod
            versioned=True,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="ParquetDataTransition",
                    enabled=True,
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INFREQUENT_ACCESS,
                            transition_after=Duration.days(30)
                        ),
                        s3.Transition(
                            storage_class=s3.StorageClass.GLACIER,
                            transition_after=Duration.days(90)
                        ),
                        s3.Transition(
                            storage_class=s3.StorageClass.DEEP_ARCHIVE,
                            transition_after=Duration.days(365)
                        ),
                    ],
                ),
            ],
        )

        # IAM Role for Lambda function
        lambda_role = iam.Role(
            self, "MarketDataCollectorRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            inline_policies={
                "MarketDataCollectorPolicy": iam.PolicyDocument(
                    statements=[
                        # S3 permissions
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "s3:PutObject",
                                "s3:PutObjectAcl",
                                "s3:GetObject",
                                "s3:DeleteObject",
                            ],
                            resources=[f"{market_data_bucket.bucket_arn}/*"],
                        ),
                        # SSM permissions for API keys
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "ssm:GetParameter",
                                "ssm:GetParameters",
                            ],
                            resources=[
                                f"arn:aws:ssm:{self.region}:{self.account}:parameter/dev/algo-meep/alpaca/*"
                            ],
                        ),
                        # CloudWatch Logs permissions
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "logs:CreateLogGroup",
                                "logs:CreateLogStream",
                                "logs:PutLogEvents",
                            ],
                            resources=[
                                f"arn:aws:logs:{self.region}:{self.account}:log-group:/aws/lambda/*"
                            ],
                        ),
                    ]
                )
            },
        )
        lambda_name = 'algo-meep-market-data-collector'
        # CloudWatch Log Group
        log_group = _logs.LogGroup(
            self, "MarketDataCollectorLogGroup",
            log_group_name=f"/aws/lambda/{lambda_name}",
            removal_policy=RemovalPolicy.DESTROY,
        )

        # Lambda function for market data collection
        market_data_lambda = _lambda.Function(
            self, "MarketDataCollectorFunction",
            function_name=lambda_name,
            runtime=_lambda.Runtime.PROVIDED_AL2,
            handler="bootstrap",
            code=self._build_code_source(),
            role=lambda_role,
            timeout=Duration.minutes(15),
            memory_size=256,
            environment={
                "S3_BUCKET": market_data_bucket.bucket_name,
                "LOCALSTACK_ENDPOINT": "http://localhost:4566",
                "ALPACA_API_KEY": "your-api-key-here",
                "ALPACA_API_SECRET": "your-api-secret-here"
            },
            architecture=_lambda.Architecture.ARM_64,
            log_group=log_group,
        )

        # Grant Lambda function permissions to write to S3
        market_data_bucket.grant_put(market_data_lambda)

        # Outputs
        CfnOutput(
            self, "MarketDataBucketName",
            value=market_data_bucket.bucket_name,
            description="Name of the S3 bucket for market data",
        )

        CfnOutput(
            self, "MarketDataCollectorFunctionArn",
            value=market_data_lambda.function_arn,
            description="ARN of the market data collector Lambda function",
        )

        CfnOutput(
            self, "MarketDataCollectorFunctionName",
            value=market_data_lambda.function_name,
            description="Name of the market data collector Lambda function",
        )

    def _build_code_source(self):
        """
        Use bootstrap.zip from pipeline artifact if available, otherwise use local file.
        For hot reload, the gowatch script updates the Lambda function code directly.
        """
        if self.bootstrap_artifact:
            # Use the artifact from the pipeline
            return _lambda.Code.from_asset(self.bootstrap_artifact)
        else:
            # Fallback to local file for local development
            return _lambda.Code.from_asset("./infrastructure/bootstrap.zip")





