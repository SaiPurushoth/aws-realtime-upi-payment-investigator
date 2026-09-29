from __future__ import annotations

import json
import time
from typing import Any

import boto3

REGION = "us-east-1"
ACCOUNT_ID = "163856954803"
PAYMENT_STREAM = "upi-payment-events"
INVESTIGATION_STREAM = "upi-investigations"
FLINK_APPLICATION = "upi-payment-investigator"
LAMBDA_FUNCTION = "upi-investigator-trigger"
TRANSACTION_TABLE = "upi-demo-transactions"
AGENT_RUNTIME_ID = "UpIPaymentInvestigator_upi_payment_investigator-mI4LjW3s5e"
TABLE_BUCKET_NAME = f"upi-demo-history-{ACCOUNT_ID}-{REGION}"
TABLE_BUCKET_ARN = (
    f"arn:aws:s3tables:{REGION}:{ACCOUNT_ID}:bucket/{TABLE_BUCKET_NAME}"
)
DLQ_BUCKET = f"upi-demo-streaming-tables-dlq-{ACCOUNT_ID}-{REGION}"
ARTIFACT_BUCKET = "upipaymentinvestigatorsta-flinkartifactbucket68f13-qpddfojbwfq4"
CHANNELS = ("upi-payment-events-history", "upi-investigations-history")


def _athena_count(session: boto3.Session) -> tuple[bool, str]:
    client = session.client("athena")
    query_id = client.start_query_execution(
        QueryString="SELECT count(*) FROM payment_events",
        QueryExecutionContext={
            "Catalog": f"s3tablescatalog/{TABLE_BUCKET_NAME}",
            "Database": "upi_demo",
        },
        ResultConfiguration={
            "OutputLocation": f"s3://{ARTIFACT_BUCKET}/athena-results/"
        },
        WorkGroup="primary",
    )["QueryExecutionId"]
    for _ in range(30):
        execution = client.get_query_execution(QueryExecutionId=query_id)[
            "QueryExecution"
        ]
        state = execution["Status"]["State"]
        if state == "SUCCEEDED":
            result = client.get_query_results(QueryExecutionId=query_id)
            count = result["ResultSet"]["Rows"][1]["Data"][0]["VarCharValue"]
            return True, f"query {query_id} returned {count} payment rows"
        if state in {"FAILED", "CANCELLED"}:
            reason = execution["Status"].get("StateChangeReason", state)
            return False, f"query {query_id}: {reason}"
        time.sleep(1)
    return False, f"query {query_id} did not finish within 30 seconds"


def main() -> int:
    session = boto3.Session(region_name=REGION)
    results: dict[str, Any] = {}
    blockers: list[str] = []

    kinesis = session.client("kinesis")
    stream_status = {
        name: kinesis.describe_stream_summary(StreamName=name)[
            "StreamDescriptionSummary"
        ]["StreamStatus"]
        for name in (PAYMENT_STREAM, INVESTIGATION_STREAM)
    }
    results["streams"] = stream_status

    flink_status = session.client("kinesisanalyticsv2").describe_application(
        ApplicationName=FLINK_APPLICATION
    )["ApplicationDetail"]["ApplicationStatus"]
    results["flink"] = flink_status

    lambda_client = session.client("lambda")
    mappings = lambda_client.list_event_source_mappings(
        FunctionName=LAMBDA_FUNCTION
    )["EventSourceMappings"]
    lambda_state = lambda_client.get_function_configuration(
        FunctionName=LAMBDA_FUNCTION
    )["State"]
    results["lambda"] = {
        "state": lambda_state,
        "mappings": [
            {
                "state": mapping["State"],
                "last_result": mapping.get("LastProcessingResult"),
            }
            for mapping in mappings
        ],
    }

    runtime = session.client("bedrock-agentcore-control").get_agent_runtime(
        agentRuntimeId=AGENT_RUNTIME_ID
    )
    results["agentcore"] = runtime["status"]
    results["dynamodb"] = session.client("dynamodb").describe_table(
        TableName=TRANSACTION_TABLE
    )["Table"]["TableStatus"]

    real_time_ready = (
        all(status == "ACTIVE" for status in stream_status.values())
        and flink_status == "RUNNING"
        and lambda_state == "Active"
        and bool(mappings)
        and all(mapping["State"] == "Enabled" for mapping in mappings)
        and runtime["status"] == "READY"
        and results["dynamodb"] == "ACTIVE"
    )
    if not real_time_ready:
        blockers.append("one or more real-time AWS components are not ready")

    channel_summaries = {
        item["ChannelName"]: item for item in kinesis.list_channels()["ChannelSummaries"]
    }
    channel_status = {
        name: channel_summaries.get(name, {}).get("ChannelStatus", "MISSING")
        for name in CHANNELS
    }
    results["historical_channels"] = channel_status

    s3tables = session.client("s3tables")
    table_bucket = s3tables.get_table_bucket(tableBucketARN=TABLE_BUCKET_ARN)
    tables = s3tables.list_tables(
        tableBucketARN=TABLE_BUCKET_ARN,
        namespace="upi_demo",
    )["tables"]
    table_names = {table["name"] for table in tables}
    results["table_bucket"] = table_bucket["arn"]
    results["tables"] = sorted(table_names)

    glue = session.client("glue")
    schema_results: dict[str, bool] = {}
    for schema_name, timestamp_name in (
        ("payment_events", "event_time"),
        ("investigation_events", "detected_at"),
    ):
        metadata = glue.get_schema(
            SchemaId={
                "RegistryName": "upi-demo-streaming-tables",
                "SchemaName": schema_name,
            }
        )
        version = glue.get_schema_version(
            SchemaId={
                "RegistryName": "upi-demo-streaming-tables",
                "SchemaName": schema_name,
            },
            SchemaVersionNumber={"LatestVersion": True},
        )
        definition = json.loads(version["SchemaDefinition"])
        schema_results[schema_name] = (
            metadata["SchemaStatus"] == "AVAILABLE"
            and definition["properties"][timestamp_name]
            == {"format": "date-time", "type": "string"}
        )
    results["schemas"] = schema_results

    dlq = session.client("s3").list_objects_v2(Bucket=DLQ_BUCKET)
    dlq_empty = dlq.get("KeyCount", 0) == 0
    results["dlq_empty"] = dlq_empty

    athena_ok, athena_detail = _athena_count(session)
    results["athena"] = athena_detail

    historical_ready = (
        all(status == "ACTIVE" for status in channel_status.values())
        and {"payment_events", "investigation_events"} <= table_names
        and all(schema_results.values())
        and dlq_empty
        and athena_ok
    )
    if not athena_ok:
        blockers.append(f"Athena cannot query the S3 Tables catalog: {athena_detail}")

    print(json.dumps(results, indent=2, default=str))
    print(f"REAL-TIME DEMO READY = {'YES' if real_time_ready else 'NO'}")
    print(f"HISTORICAL ANALYTICS READY = {'YES' if historical_ready else 'NO'}")
    if blockers:
        print("BLOCKERS")
        for blocker in blockers:
            print(f"- {blocker}")
    return 0 if real_time_ready and historical_ready else 1


if __name__ == "__main__":
    raise SystemExit(main())
