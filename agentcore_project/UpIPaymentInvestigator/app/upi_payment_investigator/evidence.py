from __future__ import annotations

from decimal import Decimal
from typing import Any

EVIDENCE_FIELDS = (
    "transaction_id",
    "amount",
    "payer_debit_status",
    "network_status",
    "beneficiary_credit_status",
    "merchant_callback_status",
    "callback_http_status",
    "callback_error",
)


def _number(value: object) -> int | float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, (int, float)):
        return value
    return float(str(value))


def read_transaction_evidence(table: Any, transaction_id: str) -> dict[str, Any]:
    if not isinstance(transaction_id, str) or not transaction_id.strip():
        raise ValueError("transaction_id must be a non-empty string")

    response = table.get_item(
        Key={"transaction_id": transaction_id.strip()},
        ConsistentRead=True,
    )
    item = response.get("Item")
    if not isinstance(item, dict):
        raise ValueError("transaction evidence was not found")
    if item.get("synthetic") is not True:
        raise ValueError("transaction evidence is not marked synthetic")

    statuses = {
        "payer_debit_status": None,
        "network_status": None,
        "beneficiary_credit_status": None,
        "merchant_callback_status": None,
    }
    callback_http_status = None
    callback_error = None

    for event in item.get("evidence", []):
        if not isinstance(event, dict):
            continue
        event_type = event.get("event_type")
        status = event.get("status")
        if event_type == "PAYER_DEBIT_SUCCESS":
            statuses["payer_debit_status"] = status
        elif event_type == "NETWORK_SUCCESS":
            statuses["network_status"] = status
        elif event_type == "BENEFICIARY_CREDIT_SUCCESS":
            statuses["beneficiary_credit_status"] = status
        elif event_type in {"MERCHANT_CALLBACK_SUCCESS", "MERCHANT_CALLBACK_FAILED"}:
            statuses["merchant_callback_status"] = status
            metadata = event.get("metadata", {})
            if isinstance(metadata, dict):
                callback_http_status = _number(metadata.get("http_status"))
                callback_error = metadata.get("error")

    evidence = {
        "transaction_id": str(item["transaction_id"]),
        "amount": _number(item.get("amount")),
        **statuses,
        "callback_http_status": callback_http_status,
        "callback_error": callback_error,
    }
    return {field: evidence[field] for field in EVIDENCE_FIELDS}
