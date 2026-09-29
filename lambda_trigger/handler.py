from __future__ import annotations

import base64
import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any

INCIDENT_TYPE = "MERCHANT_CALLBACK_FAILURE"
RESULT_FIELDS = (
    "transaction_id",
    "incident_id",
    "classification",
    "root_cause",
    "evidence",
    "recommended_action",
    "customer_action",
)


def decode_record(record: dict[str, Any]) -> dict[str, Any]:
    encoded = record["kinesis"]["data"]
    incident = json.loads(base64.b64decode(encoded).decode("utf-8"))
    if not isinstance(incident, dict):
        raise ValueError("investigation record must be a JSON object")
    return incident


def validate_incident(incident: dict[str, Any]) -> dict[str, str]:
    transaction_id = incident.get("transaction_id")
    incident_id = incident.get("incident_id")
    incident_type = incident.get("incident_type")
    if not isinstance(transaction_id, str) or not transaction_id.startswith("txn-demo-"):
        raise ValueError("refusing a transaction not marked as synthetic demo data")
    if incident_type != INCIDENT_TYPE:
        raise ValueError(f"unsupported incident_type: {incident_type}")
    if incident_id != f"{transaction_id}-{INCIDENT_TYPE}":
        raise ValueError("incident_id is not deterministic for the transaction")
    return {
        "incident_id": incident_id,
        "transaction_id": transaction_id,
        "incident_type": incident_type,
    }


def _response_bytes(response_body: Any) -> bytes:
    if hasattr(response_body, "read"):
        return response_body.read()
    if isinstance(response_body, bytes):
        return response_body
    if isinstance(response_body, str):
        return response_body.encode("utf-8")
    return b"".join(response_body)


def invoke_agent(
    client: Any, runtime_arn: str, incident: dict[str, Any]
) -> dict[str, Any]:
    payload = validate_incident(incident)
    response = client.invoke_agent_runtime(
        agentRuntimeArn=runtime_arn,
        runtimeSessionId=str(uuid.uuid4()),
        qualifier="DEFAULT",
        contentType="application/json",
        accept="application/json",
        payload=json.dumps(payload).encode("utf-8"),
    )
    if response.get("statusCode", 200) >= 300:
        raise RuntimeError(f"AgentCore returned status {response['statusCode']}")

    result = json.loads(_response_bytes(response["response"]).decode("utf-8"))
    if not isinstance(result, dict) or set(result) != set(RESULT_FIELDS):
        raise ValueError("AgentCore response does not match the required result schema")
    if result["transaction_id"] != payload["transaction_id"]:
        raise ValueError("AgentCore response changed transaction_id")
    if result["incident_id"] != payload["incident_id"]:
        raise ValueError("AgentCore response changed incident_id")
    return result


def store_result(
    table: Any, incident: dict[str, Any], result: dict[str, Any]
) -> None:
    table.update_item(
        Key={"transaction_id": incident["transaction_id"]},
        UpdateExpression=(
            "SET investigation_status = :completed, incident_id = :incident_id, "
            "agent_classification = :classification, root_cause = :root_cause, "
            "agent_evidence = :evidence, recommended_action = :recommended_action, "
            "customer_action = :customer_action, investigated_at = :investigated_at"
        ),
        ConditionExpression="attribute_exists(transaction_id)",
        ExpressionAttributeValues={
            ":completed": "COMPLETED",
            ":incident_id": result["incident_id"],
            ":classification": result["classification"],
            ":root_cause": result["root_cause"],
            ":evidence": result["evidence"],
            ":recommended_action": result["recommended_action"],
            ":customer_action": result["customer_action"],
            ":investigated_at": datetime.now(timezone.utc).isoformat(),
        },
    )


def process_records(
    event: dict[str, Any], client: Any, runtime_arn: str, table: Any
) -> dict[str, list[dict[str, str]]]:
    failures: list[dict[str, str]] = []
    for record in event.get("Records", []):
        sequence = record.get("kinesis", {}).get("sequenceNumber", "unknown")
        try:
            incident = decode_record(record)
            result = invoke_agent(client, runtime_arn, incident)
            store_result(table, incident, result)
        except Exception as exc:  # noqa: BLE001 - every record error must be reported.
            print(f"record {sequence} failed: {exc}")
            failures.append({"itemIdentifier": sequence})
    return {"batchItemFailures": failures}


def handler(event: dict[str, Any], context: Any) -> dict[str, list[dict[str, str]]]:
    del context
    import boto3

    runtime_arn = os.environ["AGENT_RUNTIME_ARN"]
    table_name = os.getenv("TABLE_NAME", "upi-demo-transactions")
    return process_records(
        event,
        boto3.client("bedrock-agentcore"),
        runtime_arn,
        boto3.resource("dynamodb").Table(table_name),
    )
