from aws_cdk import (
    Stack,
    CfnOutput,
    RemovalPolicy,
    Duration,
    aws_cognito as cognito,
)
from constructs import Construct


class AuthStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Cognito User Pool — free up to 50k MAU
        self.user_pool = cognito.UserPool(
            self, "AlgoFlowUserPool",
            user_pool_name="algoflow-users",
            self_sign_up_enabled=True,
            sign_in_aliases=cognito.SignInAliases(email=True),
            auto_verify=cognito.AutoVerifiedAttrs(email=True),
            standard_attributes=cognito.StandardAttributes(
                email=cognito.StandardAttribute(required=True, mutable=True),
            ),
            password_policy=cognito.PasswordPolicy(
                min_length=8,
                require_lowercase=True,
                require_uppercase=True,
                require_digits=True,
                require_symbols=False,
            ),
            account_recovery=cognito.AccountRecovery.EMAIL_ONLY,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # App client — no secret (public client for desktop/mobile app)
        self.user_pool_client = self.user_pool.add_client(
            "AlgoFlowAppClient",
            user_pool_client_name="algoflow-app",
            auth_flows=cognito.AuthFlow(
                user_password=True,
                user_srp=True,
            ),
            access_token_validity=Duration.hours(1),
            id_token_validity=Duration.hours(1),
            refresh_token_validity=Duration.days(30),
            prevent_user_existence_errors=True,
        )

        # Outputs
        CfnOutput(
            self, "UserPoolId",
            value=self.user_pool.user_pool_id,
            description="Cognito User Pool ID",
            export_name="AlgoFlowUserPoolId",
        )

        CfnOutput(
            self, "UserPoolClientId",
            value=self.user_pool_client.user_pool_client_id,
            description="Cognito User Pool Client ID",
            export_name="AlgoFlowUserPoolClientId",
        )

        CfnOutput(
            self, "UserPoolArn",
            value=self.user_pool.user_pool_arn,
            description="Cognito User Pool ARN",
            export_name="AlgoFlowUserPoolArn",
        )
