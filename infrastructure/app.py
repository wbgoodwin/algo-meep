#!/usr/bin/env python3
import aws_cdk as cdk

from stacks.auth_stack import AuthStack
from stacks.data_stack import DataStack
from stacks.api_stack import ApiStack
from constants import DEFAULT_REGION, DEFAULT_ACCOUNT


app = cdk.App()

# ──────────────────────────────────────────────────────────────────
# Stacks deployed via CircleCI CDK deploy (or direct `cdk deploy`)
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
    sync_bucket_name=data_stack.sync_bucket.bucket_name,
    token_encryption_key_arn=data_stack.token_encryption_key.key_arn,
    env=env,
)

app.synth()

