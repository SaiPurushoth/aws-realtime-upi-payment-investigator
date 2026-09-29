from __future__ import annotations

import argparse
import json
import uuid
from collections.abc import Callable, Sequence
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any

PAYMENT_STREAM_NAME = "upi-payment-events"
TRANSACTION_TABLE_NAME = "upi-demo-transactions"
DEFAULT_AMOUNT = Decimal("499.00")
DEFAULT_MERCHANT_ID = "merchant-demo-001"

SUCCESSFUL_EVENT_TYPES = (
    "PAYMENT_INITIATED",
    "PAYER_DEBIT_SUCCESS",
    "NETWORK_SUCCESS",
    "BENEFICIARY_CREDIT_SUCCESS",
    "MERCHANT_CALLBACK_SUCCESS",
)
CALLBACK_FAILURE_EVENT_TYPES = (
    "PAYMENT_INITIATED",
    "PAYER_DEBIT_SUCCESS",
    "NETWORK_SUCCESS",
    "BENEFICIARY_CREDIT_SUCCESS",
    "MERCHANT_CALLBACK_FAILED",
)

Clock = Callable[[], datetime]
IdFactory = Callable[[], uuid.UUID]


class Scenario(str, Enum):
    NORMAL = "NORMAL"
    MERCHANT_CALLBACK_FAILURE = "MERCHANT_CALLBACK_FAILURE"


CLI_SCENARIOS = {
    "normal": Scenario.NORMAL,
    "callback-failure": Scenario.MERCHANT_CALLBACK_FAILURE,
}


def event_status(event_type: str) -> str:
    if event_type == "PAYMENT_INITIATED":
        return "PENDING"
    if event_type == "MERCHANT_CALLBACK_FAILED":
        return "FAILED"
    return "SUCCESS"


def build_events(
    scenario: Scenario,
    *,
    clock: Clock | None = None,
    id_factory: IdFactory | None = None,
    amount: Decimal = DEFAULT_AMOUNT,
    merchant_id: str = DEFAULT_MERCHANT_ID,
) -> list[dict[str, Any]]:
    """Build one synthetic transaction's ordered payment lifecycle."""
    if not isinstance(scenario, Scenario):
        raise TypeError("scenario must be a Scenario")
    if amount <= 0:
        raise ValueError("amount must be positive")

    now = clock or (lambda: datetime.now(timezone.utc))
    new_id = id_factory or uuid.uuid4
    transaction_id = f"txn-demo-{new_id()}"
    order_id = f"order-demo-{new_id()}"
    event_types = (
        SUCCESSFUL_EVENT_TYPES
        if scenario is Scenario.NORMAL
        else CALLBACK_FAILURE_EVENT_TYPES
    )

    events: list[dict[str, Any]] = []
    for event_type in event_types:
        metadata: dict[str, Any] = {
            "synthetic": True,
            "scenario": scenario.value,
        }
        if event_type == "MERCHANT_CALLBACK_FAILED":
            metadata.update(
                {
                    "http_status": 500,
                    "error": "Merchant callback endpoint unavailable",
                }
            )
        events.append(
            {
                "transaction_id": transaction_id,
                "event_type": event_type,
                "event_time": now().isoformat(),
                "amount": amount,
                "merchant_id": merchant_id,
                "order_id": order_id,
                "status": event_status(event_type),
                "metadata": metadata,
            }
        )
    return events


def _json_default(value: object) -> float:
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(f"cannot encode {type(value).__name__}")


def publish_events(kinesis_client: Any, events: list[dict[str, Any]]) -> list[dict]:
    """Publish events in order using the transaction ID as the partition key."""
    responses = []
    for event in events:
        responses.append(
            kinesis_client.put_record(
                StreamName=PAYMENT_STREAM_NAME,
                PartitionKey=event["transaction_id"],
                Data=json.dumps(
                    event,
                    default=_json_default,
                    separators=(",", ":"),
                ).encode("utf-8"),
            )
        )
    return responses


def build_evidence(scenario: Scenario, events: list[dict[str, Any]]) -> dict[str, Any]:
    """Create a DynamoDB-safe evidence item containing the emitted event trail."""
    if not events:
        raise ValueError("events cannot be empty")
    first, last = events[0], events[-1]
    return {
        "transaction_id": first["transaction_id"],
        "scenario": scenario.value,
        "amount": first["amount"],
        "merchant_id": first["merchant_id"],
        "order_id": first["order_id"],
        "status": last["status"],
        "event_count": Decimal(len(events)),
        "created_at": first["event_time"],
        "updated_at": last["event_time"],
        "synthetic": True,
        "evidence": events,
    }


def produce_transaction(
    kinesis_client: Any,
    transaction_table: Any,
    scenario: Scenario,
    *,
    clock: Clock | None = None,
    id_factory: IdFactory | None = None,
) -> dict[str, Any]:
    events = build_events(scenario, clock=clock, id_factory=id_factory)
    responses = publish_events(kinesis_client, events)
    evidence = build_evidence(scenario, events)
    transaction_table.put_item(Item=evidence)
    return {
        "transaction_id": evidence["transaction_id"],
        "scenario": scenario.value,
        "status": evidence["status"],
        "records_sent": len(responses),
        "sequence_numbers": [response.get("SequenceNumber") for response in responses],
    }


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Send one synthetic payment lifecycle to AWS."
    )
    result.add_argument("--scenario", required=True, choices=tuple(CLI_SCENARIOS))
    return result


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)

    import boto3

    session = boto3.Session()
    receipt = produce_transaction(
        session.client("kinesis"),
        session.resource("dynamodb").Table(TRANSACTION_TABLE_NAME),
        CLI_SCENARIOS[args.scenario],
    )
    print(json.dumps(receipt, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
