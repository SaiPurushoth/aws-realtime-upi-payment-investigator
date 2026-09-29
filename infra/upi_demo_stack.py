from aws_cdk import CfnOutput, Duration, RemovalPolicy, Stack
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_kinesis as kinesis
from aws_cdk import aws_s3 as s3
from constructs import Construct


class UpiDemoStack(Stack):
    """Foundational storage and streams for the synthetic UPI meetup demo."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        payment_stream = kinesis.Stream(
            self,
            "PaymentEvents",
            stream_name="upi-payment-events",
            stream_mode=kinesis.StreamMode.ON_DEMAND,
            retention_period=Duration.hours(24),
        )

        investigation_stream = kinesis.Stream(
            self,
            "Investigations",
            stream_name="upi-investigations",
            stream_mode=kinesis.StreamMode.ON_DEMAND,
            retention_period=Duration.hours(24),
        )

        transaction_table = dynamodb.Table(
            self,
            "Transactions",
            table_name="upi-demo-transactions",
            partition_key=dynamodb.Attribute(
                name="transaction_id",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        artifact_bucket = s3.Bucket(
            self,
            "FlinkArtifactBucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            versioned=True,
            removal_policy=RemovalPolicy.RETAIN,
        )

        CfnOutput(
            self,
            "PaymentStreamName",
            value=payment_stream.stream_name,
            description="Name of the synthetic payment event stream.",
        )
        CfnOutput(
            self,
            "InvestigationStreamName",
            value=investigation_stream.stream_name,
            description="Name of the investigation result stream.",
        )
        CfnOutput(
            self,
            "TransactionTableName",
            value=transaction_table.table_name,
            description="Name of the synthetic transaction table.",
        )
        CfnOutput(
            self,
            "ArtifactBucketName",
            value=artifact_bucket.bucket_name,
            description="Bucket for future Managed Flink application artifacts.",
        )
