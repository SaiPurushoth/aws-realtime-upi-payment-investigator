"""Deploy the parallel Kinesis Streaming Tables historical path.

This script intentionally leaves the real-time topology unchanged. It creates
the S3 Tables, Glue Schema Registry, DLQ, delivery roles, and native Kinesis
delivery channels. The only live-path configuration change is replacing the
unsupported AWS-managed stream key with one customer-managed KMS key.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
PAYMENT_STREAM = "upi-payment-events"
INVESTIGATION_STREAM = "upi-investigations"
FLINK_ROLE = "upi-flink-execution-role"
LAMBDA_ROLE = "upi-investigator-trigger-role"
KMS_ALIAS = "alias/upi-demo-kinesis-streams"
REGISTRY_NAME = "upi-demo-streaming-tables"
NAMESPACE = "upi_demo"
FRESHNESS_SECONDS = 300

HERE = Path(__file__).resolve().parent


def _json_file(name: str) -> str:
    value = json.loads((HERE / name).read_text(encoding="utf-8"))
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _not_found(error: ClientError, *codes: str) -> bool:
    return error.response["Error"]["Code"] in codes


def _stream_arn(account_id: str, stream_name: str) -> str:
    return f"arn:aws:kinesis:{REGION}:{account_id}:stream/{stream_name}"


def ensure_kms_key(kms: Any, account_id: str) -> str:
    aliases = kms.list_aliases()["Aliases"]
    alias = next((item for item in aliases if item["AliasName"] == KMS_ALIAS), None)
    if alias and alias.get("TargetKeyId"):
        return kms.describe_key(KeyId=alias["TargetKeyId"])["KeyMetadata"]["Arn"]

    key = kms.create_key(
        Description="Synthetic UPI demo Kinesis streams for Streaming Tables",
        KeyUsage="ENCRYPT_DECRYPT",
        KeySpec="SYMMETRIC_DEFAULT",
        Origin="AWS_KMS",
        Tags=[
            {"TagKey": "Project", "TagValue": "upi-payment-investigator"},
            {"TagKey": "Purpose", "TagValue": "kinesis-stream-encryption"},
        ],
    )["KeyMetadata"]
    kms.create_alias(AliasName=KMS_ALIAS, TargetKeyId=key["KeyId"])
    return key["Arn"]


def grant_live_path_kms(iam: Any, key_arn: str, account_id: str) -> None:
    payment_arn = _stream_arn(account_id, PAYMENT_STREAM)
    investigation_arn = _stream_arn(account_id, INVESTIGATION_STREAM)
    flink_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "DecryptPaymentEvents",
                "Effect": "Allow",
                "Action": "kms:Decrypt",
                "Resource": key_arn,
                "Condition": {
                    "StringEquals": {
                        "kms:ViaService": f"kinesis.{REGION}.amazonaws.com",
                        "kms:EncryptionContext:aws:kinesis:arn": payment_arn,
                    },
                },
            },
            {
                "Sid": "EncryptInvestigations",
                "Effect": "Allow",
                "Action": "kms:GenerateDataKey",
                "Resource": key_arn,
                "Condition": {
                    "StringEquals": {
                        "kms:ViaService": f"kinesis.{REGION}.amazonaws.com",
                        "kms:EncryptionContext:aws:kinesis:arn": investigation_arn,
                    }
                },
            },
        ],
    }
    lambda_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "DecryptInvestigationEvents",
                "Effect": "Allow",
                "Action": "kms:Decrypt",
                "Resource": key_arn,
                "Condition": {
                    "StringEquals": {
                        "kms:ViaService": f"kinesis.{REGION}.amazonaws.com",
                        "kms:EncryptionContext:aws:kinesis:arn": investigation_arn,
                    }
                },
            }
        ],
    }
    iam.put_role_policy(
        RoleName=FLINK_ROLE,
        PolicyName="UpiKinesisCustomerKeyAccess",
        PolicyDocument=json.dumps(flink_policy),
    )
    iam.put_role_policy(
        RoleName=LAMBDA_ROLE,
        PolicyName="UpiKinesisCustomerKeyAccess",
        PolicyDocument=json.dumps(lambda_policy),
    )


def use_customer_key(kinesis: Any, stream_name: str, key_arn: str) -> None:
    current = kinesis.describe_stream_summary(StreamName=stream_name)[
        "StreamDescriptionSummary"
    ]
    if current.get("KeyId") == key_arn:
        return
    kinesis.start_stream_encryption(
        StreamName=stream_name,
        EncryptionType="KMS",
        KeyId=key_arn,
    )
    deadline = time.monotonic() + 300
    while time.monotonic() < deadline:
        current = kinesis.describe_stream_summary(StreamName=stream_name)[
            "StreamDescriptionSummary"
        ]
        if current["StreamStatus"] == "ACTIVE" and current.get("KeyId") == key_arn:
            return
        time.sleep(5)
    raise TimeoutError(f"stream {stream_name} did not adopt the customer key")


def ensure_dlq(s3: Any, bucket_name: str) -> str:
    try:
        s3.head_bucket(Bucket=bucket_name)
    except ClientError as error:
        if not _not_found(error, "404", "NoSuchBucket", "NotFound"):
            raise
        s3.create_bucket(Bucket=bucket_name)

    s3.put_public_access_block(
        Bucket=bucket_name,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        },
    )
    s3.put_bucket_encryption(
        Bucket=bucket_name,
        ServerSideEncryptionConfiguration={
            "Rules": [
                {
                    "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"},
                    "BucketKeyEnabled": False,
                }
            ]
        },
    )
    return f"arn:aws:s3:::{bucket_name}"


def ensure_table_bucket(s3tables: Any, bucket_name: str) -> str:
    for bucket in s3tables.list_table_buckets()["tableBuckets"]:
        if bucket["name"] == bucket_name:
            return bucket["arn"]
    return s3tables.create_table_bucket(
        name=bucket_name,
        encryptionConfiguration={"sseAlgorithm": "AES256"},
        storageClassConfiguration={"storageClass": "STANDARD"},
        tags={"Project": "upi-payment-investigator", "Purpose": "historical-audit"},
    )["arn"]


def ensure_namespace(s3tables: Any, table_bucket_arn: str) -> None:
    try:
        s3tables.get_namespace(tableBucketARN=table_bucket_arn, namespace=NAMESPACE)
    except ClientError as error:
        if not _not_found(error, "NotFoundException", "ResourceNotFoundException"):
            raise
        s3tables.create_namespace(
            tableBucketARN=table_bucket_arn,
            namespace=[NAMESPACE],
        )


def ensure_registry(glue: Any) -> str:
    try:
        registry = glue.get_registry(RegistryId={"RegistryName": REGISTRY_NAME})
    except ClientError as error:
        if not _not_found(error, "EntityNotFoundException"):
            raise
        registry = glue.create_registry(
            RegistryName=REGISTRY_NAME,
            Description="Schemas for synthetic UPI Kinesis Streaming Tables",
            Tags={"Project": "upi-payment-investigator"},
        )
    return registry["RegistryArn"]


def ensure_schema(glue: Any, schema_name: str, definition_file: str) -> str:
    try:
        schema = glue.get_schema(
            SchemaId={"RegistryName": REGISTRY_NAME, "SchemaName": schema_name}
        )
        return schema["SchemaArn"]
    except ClientError as error:
        if not _not_found(error, "EntityNotFoundException"):
            raise

    schema = glue.create_schema(
        RegistryId={"RegistryName": REGISTRY_NAME},
        SchemaName=schema_name,
        DataFormat="JSON",
        Compatibility="NONE",
        Description=f"Synthetic UPI {schema_name.replace('_', ' ')}",
        Tags={"Project": "upi-payment-investigator"},
        SchemaDefinition=_json_file(definition_file),
    )
    return schema["SchemaArn"]


def trust_policy(account_id: str, source_arn: str) -> dict[str, Any]:
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "kinesis.amazonaws.com"},
                "Action": "sts:AssumeRole",
                "Condition": {
                    "StringEquals": {"aws:SourceAccount": account_id},
                    "ArnLike": {"aws:SourceArn": source_arn},
                },
            }
        ],
    }


def ensure_delivery_role(
    iam: Any,
    *,
    role_name: str,
    account_id: str,
    table_bucket_arn: str,
    table_name: str,
    dlq_arn: str,
    dlq_prefix: str,
    registry_arn: str,
    schema_arn: str,
    key_arn: str,
    source_stream_arn: str,
) -> str:
    wildcard_channel = f"arn:aws:kinesis:{REGION}:{account_id}:channel/*"
    try:
        role = iam.get_role(RoleName=role_name)["Role"]
    except ClientError as error:
        if not _not_found(error, "NoSuchEntity"):
            raise
        role = iam.create_role(
            RoleName=role_name,
            Description=f"Kinesis delivery role for {table_name}",
            AssumeRolePolicyDocument=json.dumps(
                trust_policy(account_id, wildcard_channel)
            ),
            Tags=[{"Key": "Project", "Value": "upi-payment-investigator"}],
        )["Role"]

    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "S3TablesAccess",
                "Effect": "Allow",
                "Action": [
                    "s3tables:GetTable",
                    "s3tables:GetTableBucket",
                    "s3tables:GetTableMetadataLocation",
                    "s3tables:UpdateTableMetadataLocation",
                    "s3tables:CreateTable",
                    "s3tables:CreateNamespace",
                    "s3tables:PutTableData",
                    "s3tables:GetTableData",
                    "s3tables:TagResource",
                    "s3tables:PutTableRecordExpirationConfiguration",
                ],
                "Resource": [table_bucket_arn, f"{table_bucket_arn}/table/*"],
            },
            {
                "Sid": "ListDLQ",
                "Effect": "Allow",
                "Action": ["s3:ListBucket", "s3:ListBucketMultipartUploads"],
                "Resource": dlq_arn,
                "Condition": {"StringEquals": {"aws:ResourceAccount": account_id}},
            },
            {
                "Sid": "WriteDLQ",
                "Effect": "Allow",
                "Action": "s3:PutObject",
                "Resource": f"{dlq_arn}/{dlq_prefix}*",
                "Condition": {"StringEquals": {"aws:ResourceAccount": account_id}},
            },
            {
                "Sid": "ReadGlueSchema",
                "Effect": "Allow",
                "Action": "glue:GetSchemaVersion",
                "Resource": [registry_arn, schema_arn],
            },
            {
                "Sid": "ValidateSourceKMSKey",
                "Effect": "Allow",
                "Action": ["kms:Decrypt", "kms:GenerateDataKey"],
                "Resource": key_arn,
                "Condition": {
                    "StringEqualsIfExists": {
                        "kms:ViaService": f"kinesis.{REGION}.amazonaws.com"
                    }
                },
            },
            {
                "Sid": "DecryptSourceRecords",
                "Effect": "Allow",
                "Action": "kms:Decrypt",
                "Resource": key_arn,
                "Condition": {
                    "StringEqualsIfExists": {
                        "kms:ViaService": f"kinesis.{REGION}.amazonaws.com"
                    },
                    "StringEquals": {
                        "kms:EncryptionContext:aws:kinesis:arn": source_stream_arn
                    },
                },
            },
        ],
    }
    iam.put_role_policy(
        RoleName=role_name,
        PolicyName="KinesisStreamingTablesDelivery",
        PolicyDocument=json.dumps(policy),
    )
    return role["Arn"]


def ensure_channel(
    kinesis: Any,
    *,
    channel_name: str,
    role_arn: str,
    stream_arn: str,
    schema_arn: str,
    table_bucket_arn: str,
    table_name: str,
    timestamp_field: str,
    dlq_arn: str,
    dlq_prefix: str,
    account_id: str,
) -> str:
    for item in kinesis.list_channels()["ChannelSummaries"]:
        if item["ChannelName"] == channel_name:
            return item["ChannelARN"]

    response = kinesis.create_channel(
        ChannelName=channel_name,
        ServiceExecutionRoleARN=role_arn,
        StreamConfigurationList=[
            {
                "StreamARN": stream_arn,
                "RecordConfiguration": {
                    "RecordFormatType": "JSON",
                    "GSRSchemaARN": schema_arn,
                },
            }
        ],
        S3TablesDestinationConfiguration={
            "DataFreshnessInSeconds": FRESHNESS_SECONDS,
            "DeadLetterQueueS3Configuration": {
                "BucketARN": dlq_arn,
                "ExpectedBucketOwner": account_id,
                "ErrorOutputPrefix": dlq_prefix,
            },
            "S3TablesConfigurationList": [
                {
                    "TableBucketARN": table_bucket_arn,
                    "Namespace": NAMESPACE,
                    "TableName": table_name,
                    "CompressionType": "ZSTD",
                    "PartitionSpec": {
                        "PartitionFields": [
                            {"Transform": "TIME_HOUR", "SourceName": timestamp_field}
                        ]
                    },
                }
            ],
        },
        Tags={"Project": "upi-payment-investigator", "Purpose": "historical-audit"},
        LoggingConfiguration={"CloudWatchLogs": {"Enabled": False}},
    )
    return response["ChannelDescription"]["ChannelARN"]


def wait_for_channel(kinesis: Any, channel_arn: str) -> dict[str, Any]:
    deadline = time.monotonic() + 600
    while time.monotonic() < deadline:
        description = kinesis.describe_channel(ChannelARN=channel_arn)[
            "ChannelDescription"
        ]
        status = description["ChannelStatus"]
        print(f"{description['ChannelName']}: {status}", flush=True)
        if status == "ACTIVE":
            return description
        if status in {"FAILED", "DELETING"}:
            raise RuntimeError(
                f"{description['ChannelName']} is {status}: "
                f"{description.get('ChannelStatusReason', 'no reason provided')}"
            )
        time.sleep(10)
    raise TimeoutError(f"channel {channel_arn} did not become ACTIVE")


def main() -> int:
    session = boto3.Session(region_name=REGION)
    account_id = session.client("sts").get_caller_identity()["Account"]
    table_bucket_name = f"upi-demo-history-{account_id}-{REGION}"
    dlq_bucket_name = f"upi-demo-streaming-tables-dlq-{account_id}-{REGION}"

    kms = session.client("kms")
    iam = session.client("iam")
    kinesis = session.client("kinesis")
    s3 = session.client("s3")
    s3tables = session.client("s3tables")
    glue = session.client("glue")

    key_arn = ensure_kms_key(kms, account_id)
    grant_live_path_kms(iam, key_arn, account_id)
    use_customer_key(kinesis, PAYMENT_STREAM, key_arn)
    use_customer_key(kinesis, INVESTIGATION_STREAM, key_arn)

    dlq_arn = ensure_dlq(s3, dlq_bucket_name)
    table_bucket_arn = ensure_table_bucket(s3tables, table_bucket_name)
    ensure_namespace(s3tables, table_bucket_arn)
    registry_arn = ensure_registry(glue)

    configs = [
        {
            "channel_name": "upi-payment-events-history",
            "role_name": "upi-payment-events-history-role",
            "stream_name": PAYMENT_STREAM,
            "schema_name": "payment_events",
            "schema_file": "payment_events.schema.json",
            "table_name": "payment_events",
            "timestamp_field": "event_time",
            "dlq_prefix": "payment-events/",
        },
        {
            "channel_name": "upi-investigations-history",
            "role_name": "upi-investigations-history-role",
            "stream_name": INVESTIGATION_STREAM,
            "schema_name": "investigation_events",
            "schema_file": "investigation_events.schema.json",
            "table_name": "investigation_events",
            "timestamp_field": "detected_at",
            "dlq_prefix": "investigation-events/",
        },
    ]

    results = []
    for config in configs:
        schema_arn = ensure_schema(glue, config["schema_name"], config["schema_file"])
        stream_arn = _stream_arn(account_id, config["stream_name"])
        role_arn = ensure_delivery_role(
            iam,
            role_name=config["role_name"],
            account_id=account_id,
            table_bucket_arn=table_bucket_arn,
            table_name=config["table_name"],
            dlq_arn=dlq_arn,
            dlq_prefix=config["dlq_prefix"],
            registry_arn=registry_arn,
            schema_arn=schema_arn,
            key_arn=key_arn,
            source_stream_arn=stream_arn,
        )
        # IAM role propagation before the service performs create-time validation.
        time.sleep(10)
        channel_arn = ensure_channel(
            kinesis,
            channel_name=config["channel_name"],
            role_arn=role_arn,
            stream_arn=stream_arn,
            schema_arn=schema_arn,
            table_bucket_arn=table_bucket_arn,
            table_name=config["table_name"],
            timestamp_field=config["timestamp_field"],
            dlq_arn=dlq_arn,
            dlq_prefix=config["dlq_prefix"],
            account_id=account_id,
        )
        iam.update_assume_role_policy(
            RoleName=config["role_name"],
            PolicyDocument=json.dumps(trust_policy(account_id, channel_arn)),
        )
        results.append(
            {
                "channel_arn": channel_arn,
                "schema_arn": schema_arn,
                "table_name": config["table_name"],
            }
        )

    descriptions = [wait_for_channel(kinesis, item["channel_arn"]) for item in results]
    print(
        json.dumps(
            {
                "kms_key_arn": key_arn,
                "table_bucket_arn": table_bucket_arn,
                "namespace": NAMESPACE,
                "dlq": f"s3://{dlq_bucket_name}/",
                "schemas": [item["schema_arn"] for item in results],
                "channels": [
                    {
                        "name": item["ChannelName"],
                        "arn": item["ChannelARN"],
                        "status": item["ChannelStatus"],
                    }
                    for item in descriptions
                ],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
