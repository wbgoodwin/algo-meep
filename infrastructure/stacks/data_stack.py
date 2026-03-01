from aws_cdk import (
    Stack,
    CfnOutput,
    RemovalPolicy,
    aws_dynamodb as dynamodb,
    aws_s3 as s3,
    Duration,
)
from constructs import Construct


class DataStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # --- DynamoDB Tables (on-demand = pay-per-request, scales to zero) ---

        # Users table
        self.users_table = dynamodb.Table(
            self, "UsersTable",
            table_name="algoflow-users",
            partition_key=dynamodb.Attribute(
                name="PK", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="SK", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True,
            ),
        )

        # Sync table
        self.sync_table = dynamodb.Table(
            self, "SyncTable",
            table_name="algoflow-sync",
            partition_key=dynamodb.Attribute(
                name="PK", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="SK", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # Usage table
        self.usage_table = dynamodb.Table(
            self, "UsageTable",
            table_name="algoflow-usage",
            partition_key=dynamodb.Attribute(
                name="PK", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="SK", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,  # Usage data is regenerable
        )

        # --- S3 Bucket for encrypted sync blobs ---

        self.sync_bucket = s3.Bucket(
            self, "SyncBucket",
            bucket_name=f"algoflow-sync-{self.account}-{self.region}",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.RETAIN,
            versioned=False,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="CleanupOldSyncs",
                    enabled=True,
                    expiration=Duration.days(90),
                ),
            ],
        )

        # --- Outputs ---

        CfnOutput(self, "UsersTableName", value=self.users_table.table_name,
                  export_name="AlgoFlowUsersTableName")
        CfnOutput(self, "UsersTableArn", value=self.users_table.table_arn,
                  export_name="AlgoFlowUsersTableArn")
        CfnOutput(self, "SyncTableName", value=self.sync_table.table_name,
                  export_name="AlgoFlowSyncTableName")
        CfnOutput(self, "SyncTableArn", value=self.sync_table.table_arn,
                  export_name="AlgoFlowSyncTableArn")
        CfnOutput(self, "UsageTableName", value=self.usage_table.table_name,
                  export_name="AlgoFlowUsageTableName")
        CfnOutput(self, "UsageTableArn", value=self.usage_table.table_arn,
                  export_name="AlgoFlowUsageTableArn")
        CfnOutput(self, "SyncBucketName", value=self.sync_bucket.bucket_name,
                  export_name="AlgoFlowSyncBucketName")
        CfnOutput(self, "SyncBucketArn", value=self.sync_bucket.bucket_arn,
                  export_name="AlgoFlowSyncBucketArn")
