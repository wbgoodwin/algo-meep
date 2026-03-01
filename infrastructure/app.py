#!/usr/bin/env python3
import os

import aws_cdk as cdk
from aws_cdk.pipelines import CodePipeline, CodePipelineSource, ShellStep
from aws_cdk.aws_codebuild import LinuxArmBuildImage, ComputeType, BuildEnvironment
from aws_cdk.aws_iam import Effect, PolicyStatement

from stacks.market_data_collector_stack import MarketDataCollectorStack
from stacks.auth_stack import AuthStack
from stacks.data_stack import DataStack
from stacks.api_stack import ApiStack
from constants import APP_NAME, DEFAULT_REGION, REPO_NAME, GH_CONNECTION_ARN, DEFAULT_ACCOUNT


app = cdk.App()

# Create a stack for the pipeline
class PipelineStack(cdk.Stack):
    def __init__(self, scope, id, **kwargs):
        super().__init__(scope, id, **kwargs)
        
        # Create the pipeline
        self.pipeline = CodePipeline(self, f"{APP_NAME}-Pipeline",
            pipeline_name=f"{APP_NAME}-Pipeline",
            synth=ShellStep("Synth",
                input=CodePipelineSource.connection(REPO_NAME, "main", connection_arn=GH_CONNECTION_ARN),
                commands=[
                    # Build the market data collector Go Lambda
                    "cd market_data_collector",
                    "GOOS=linux GOARCH=arm64 go build -o bootstrap .",
                    "cd ..",
                    "zip -j market_data_collector/bootstrap.zip market_data_collector/bootstrap",
                    # Build the backend API Go Lambda
                    "cd backend",
                    "GOOS=linux GOARCH=arm64 go build -o build/api/bootstrap ./cmd/api/",
                    "cd ..",
                    "zip -j backend/api-bootstrap.zip backend/build/api/bootstrap",
                    # Move artifacts for CDK
                    "mkdir -p infrastructure/cdk.out",
                    "mv market_data_collector/bootstrap.zip infrastructure/cdk.out/",
                    "mv backend/api-bootstrap.zip infrastructure/cdk.out/",
                    # CDK synth
                    'export PATH="$HOME/.local/bin:$PATH"',
                    "source $HOME/.local/bin/env",
                    "cd infrastructure",
                    "uv sync",
                    "source .venv/bin/activate",
                    "cd ..",
                    "cdk synth"
                ],
            ),
            code_build_defaults=cdk.pipelines.CodeBuildOptions(
                build_environment=cdk.aws_codebuild.BuildEnvironment(
                    build_image=LinuxArmBuildImage.from_ecr_repository(
                        cdk.aws_ecr.Repository.from_repository_name(self, "BuildRepo", "algo-meep-build"),
                        "latest"

                    ),
                    compute_type=ComputeType.SMALL,
                ),
            )
        )

# Create the pipeline stack
pipeline_stack = PipelineStack(app, f"{APP_NAME}-PipelineStack",env=cdk.Environment(
                account=DEFAULT_ACCOUNT,
                region=DEFAULT_REGION
            ))


# Create the application stage with all stacks
class AlgoFlowStage(cdk.Stage):
    def __init__(self, scope, id, **kwargs):
        super().__init__(scope, id, **kwargs)

        # Existing: Market data collector
        MarketDataCollectorStack(self, "MarketDataCollectorStack", bootstrap_artifact=None)

        # New: Auth (Cognito)
        auth_stack = AuthStack(self, "AuthStack")

        # New: Data (DynamoDB + S3)
        data_stack = DataStack(self, "DataStack")

        # New: API (API Gateway + Lambda)
        ApiStack(
            self, "ApiStack",
            user_pool=auth_stack.user_pool,
            user_pool_client=auth_stack.user_pool_client,
            users_table_arn=data_stack.users_table.table_arn,
            sync_table_arn=data_stack.sync_table.table_arn,
            usage_table_arn=data_stack.usage_table.table_arn,
            sync_bucket_arn=data_stack.sync_bucket.bucket_arn,
        )


# Add the application stage to the pipeline
pipeline_stack.pipeline.add_stage(
    AlgoFlowStage(pipeline_stack, "AlgoFlowStage")
)

# ──────────────────────────────────────────────────────────────────
# Standalone stacks for direct `cdk deploy` (bypasses pipeline)
# Usage: cdk deploy AlgoFlow-Auth AlgoFlow-Data AlgoFlow-Api --profile <your-profile>
# ──────────────────────────────────────────────────────────────────
env = cdk.Environment(account=DEFAULT_ACCOUNT, region=DEFAULT_REGION)

auth_stack = AuthStack(app, "AlgoFlow-Auth", env=env)

data_stack = DataStack(app, "AlgoFlow-Data", env=env)

ApiStack(
    app, "AlgoFlow-Api",
    user_pool=auth_stack.user_pool,
    user_pool_client=auth_stack.user_pool_client,
    users_table_arn=data_stack.users_table.table_arn,
    sync_table_arn=data_stack.sync_table.table_arn,
    usage_table_arn=data_stack.usage_table.table_arn,
    sync_bucket_arn=data_stack.sync_bucket.bucket_arn,
    env=env,
)

app.synth()

