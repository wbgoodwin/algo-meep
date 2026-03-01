from aws_cdk import (
    Stack,
    CfnOutput,
    Duration,
    RemovalPolicy,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_logs as logs,
    aws_apigatewayv2 as apigwv2,
    aws_cognito as cognito,
)
from constructs import Construct


class ApiStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        user_pool: cognito.IUserPool,
        user_pool_client: cognito.IUserPoolClient,
        users_table_arn: str,
        sync_table_arn: str,
        usage_table_arn: str,
        sync_bucket_arn: str,
        token_encryption_key_arn: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        lambda_name = "algoflow-api"

        # CloudWatch Log Group
        log_group = logs.LogGroup(
            self, "ApiLogGroup",
            log_group_name=f"/aws/lambda/{lambda_name}",
            removal_policy=RemovalPolicy.RETAIN,
            retention=logs.RetentionDays.TWO_WEEKS,
        )

        # IAM Role for the API Lambda
        lambda_role = iam.Role(
            self, "ApiLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            inline_policies={
                "ApiLambdaPolicy": iam.PolicyDocument(
                    statements=[
                        # CloudWatch Logs
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
                        # DynamoDB — users, sync, usage tables
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "dynamodb:GetItem",
                                "dynamodb:PutItem",
                                "dynamodb:UpdateItem",
                                "dynamodb:DeleteItem",
                                "dynamodb:Query",
                            ],
                            resources=[
                                users_table_arn,
                                sync_table_arn,
                                usage_table_arn,
                            ],
                        ),
                        # S3 — sync bucket
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "s3:GetObject",
                                "s3:PutObject",
                                "s3:DeleteObject",
                            ],
                            resources=[f"{sync_bucket_arn}/*"],
                        ),
                        # SSM — Teller certs and config
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "ssm:GetParameter",
                                "ssm:GetParameters",
                            ],
                            resources=[
                                f"arn:aws:ssm:{self.region}:{self.account}:parameter/algoflow/*"
                            ],
                        ),
                        # KMS — access token encryption
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "kms:Encrypt",
                                "kms:Decrypt",
                            ],
                            resources=[token_encryption_key_arn],
                        ),
                        # Cognito — user management
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "cognito-idp:SignUp",
                                "cognito-idp:InitiateAuth",
                                "cognito-idp:GetUser",
                                "cognito-idp:DeleteUser",
                            ],
                            resources=[user_pool.user_pool_arn],
                        ),
                    ]
                )
            },
        )

        # Lambda function — single binary, ARM64 (matches existing pipeline)
        api_lambda = _lambda.Function(
            self, "ApiFunction",
            function_name=lambda_name,
            runtime=_lambda.Runtime.PROVIDED_AL2023,
            handler="bootstrap",
            code=self._build_code_source(),
            role=lambda_role,
            timeout=Duration.seconds(30),
            memory_size=256,
            architecture=_lambda.Architecture.ARM_64,
            log_group=log_group,
            environment={
                "COGNITO_USER_POOL_ID": user_pool.user_pool_id,
                "COGNITO_CLIENT_ID": user_pool_client.user_pool_client_id,
                "TELLER_ENV": "sandbox",
                "KMS_KEY_ARN": token_encryption_key_arn,
                "ALLOWED_IPS": "141.152.50.56",  # IP allowlist (comma-separated)
                # TELLER_API_KEY, TELLER_CERT_PEM, TELLER_KEY_PEM loaded from SSM at runtime
            },
        )

        # HTTP API (v2) — ~70% cheaper than REST API
        http_api = apigwv2.CfnApi(
            self, "HttpApi",
            name="algoflow-api",
            protocol_type="HTTP",
            cors_configuration=apigwv2.CfnApi.CorsProperty(
                allow_origins=["*"],
                allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
                allow_headers=["Content-Type", "Authorization"],
                max_age=3600,
            ),
        )

        # JWT Authorizer using Cognito
        authorizer = apigwv2.CfnAuthorizer(
            self, "JwtAuthorizer",
            api_id=http_api.ref,
            authorizer_type="JWT",
            name="cognito-jwt",
            identity_source=["$request.header.Authorization"],
            jwt_configuration=apigwv2.CfnAuthorizer.JWTConfigurationProperty(
                audience=[user_pool_client.user_pool_client_id],
                issuer=f"https://cognito-idp.{self.region}.amazonaws.com/{user_pool.user_pool_id}",
            ),
        )

        # Lambda integration
        integration = apigwv2.CfnIntegration(
            self, "LambdaIntegration",
            api_id=http_api.ref,
            integration_type="AWS_PROXY",
            integration_uri=api_lambda.function_arn,
            payload_format_version="2.0",
        )

        # Public routes (no auth)
        public_routes = [
            ("GetHealth", "GET /health"),
            ("PostAuthRegister", "POST /auth/register"),
            ("PostAuthLogin", "POST /auth/login"),
            ("PostAuthRefresh", "POST /auth/refresh"),
        ]

        for route_id, route_key in public_routes:
            apigwv2.CfnRoute(
                self, route_id,
                api_id=http_api.ref,
                route_key=route_key,
                target=f"integrations/{integration.ref}",
            )

        # Authenticated routes
        auth_routes = [
            ("DeleteAuthAccount", "DELETE /auth/account"),
            ("GetUserProfile", "GET /user/profile"),
            ("PutUserProfile", "PUT /user/profile"),
            ("GetUserUsage", "GET /user/usage"),
            ("PostBankEnroll", "POST /bank/enroll"),
            ("PostBankExchangeToken", "POST /bank/exchange-token"),
            ("PostBankAccounts", "POST /bank/accounts"),
            ("PostBankSyncTransactions", "POST /bank/sync-transactions"),
            ("GetBankProviders", "GET /bank/providers"),
        ]

        for route_id, route_key in auth_routes:
            apigwv2.CfnRoute(
                self, route_id,
                api_id=http_api.ref,
                route_key=route_key,
                target=f"integrations/{integration.ref}",
                authorization_type="JWT",
                authorizer_id=authorizer.ref,
            )

        # Auto-deploy stage with throttling to prevent cost abuse
        stage = apigwv2.CfnStage(
            self, "DefaultStage",
            api_id=http_api.ref,
            stage_name="$default",
            auto_deploy=True,
            default_route_settings=apigwv2.CfnStage.RouteSettingsProperty(
                throttling_burst_limit=100,
                throttling_rate_limit=10,  # 10 requests/sec default
            ),
        )

        # Grant API Gateway permission to invoke Lambda
        api_lambda.add_permission(
            "ApiGatewayInvoke",
            principal=iam.ServicePrincipal("apigateway.amazonaws.com"),
            source_arn=f"arn:aws:execute-api:{self.region}:{self.account}:{http_api.ref}/*",
        )

        # Outputs
        CfnOutput(
            self, "ApiUrl",
            value=f"https://{http_api.ref}.execute-api.{self.region}.amazonaws.com",
            description="API Gateway URL",
            export_name="AlgoFlowApiUrl",
        )

        CfnOutput(
            self, "ApiLambdaArn",
            value=api_lambda.function_arn,
            description="API Lambda ARN",
            export_name="AlgoFlowApiLambdaArn",
        )

    def _build_code_source(self):
        """Use pipeline artifact or local build."""
        import os
        if os.getenv("ENVIRONMENT") == "local":
            return _lambda.Code.from_asset("../backend/build/api/bootstrap.zip")
        else:
            return _lambda.Code.from_asset("./cdk.out/api-bootstrap.zip")
