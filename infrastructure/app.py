#!/usr/bin/env python3
import os

import aws_cdk as cdk
from aws_cdk.pipelines import CodePipeline, CodePipelineSource, ShellStep
from aws_cdk.aws_codebuild import LinuxArmBuildImage, ComputeType, BuildEnvironment
from aws_cdk.aws_iam import Effect, PolicyStatement

from stacks.market_data_collector_stack import MarketDataCollectorStack
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
                    # Build the Go application first
                    "./market_data_collector/build_market_data_collector.sh",
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


# Create the application stage
class MarketDataCollectorStage(cdk.Stage):
    def __init__(self, scope, id, bootstrap_output, **kwargs):
        super().__init__(scope, id, **kwargs)
        
        # Deploy the market data collector stack with the build artifact
        MarketDataCollectorStack(self, "MarketDataCollectorStack", bootstrap_artifact=bootstrap_output)


# Add the application stage to the pipeline
pipeline_stack.pipeline.add_stage(
    MarketDataCollectorStage(pipeline_stack, "MarketDataCollectorStage", None)
)

app.synth()

