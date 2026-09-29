from __future__ import annotations

from decimal import Decimal
from typing import Any


def _amount(value: object) -> float | object:
    return float(value) if isinstance(value, Decimal) else value


def _event_statuses(item: dict[str, Any]) -> dict[str, str]:
    return {
        str(event.get("event_type")): str(event.get("status", "PENDING"))
        for event in item.get("evidence", [])
        if isinstance(event, dict)
    }


def build_view_model(item: dict[str, Any]) -> dict[str, Any]:
    """Map one DynamoDB transaction item into the dashboard's display fields."""
    statuses = _event_statuses(item)
    callback_status = statuses.get("MERCHANT_CALLBACK_SUCCESS") or statuses.get(
        "MERCHANT_CALLBACK_FAILED", "PENDING"
    )
    incident_id = item.get("incident_id")
    scenario = item.get("scenario")
    if item.get("investigation_status"):
        investigation_status = item["investigation_status"]
    elif scenario == "MERCHANT_CALLBACK_FAILURE":
        investigation_status = "PENDING"
    else:
        investigation_status = "NOT REQUIRED"

    return {
        "transaction_id": item.get("transaction_id", "—"),
        "amount": _amount(item.get("amount", "—")),
        "merchant_id": item.get("merchant_id", "—"),
        "order_id": item.get("order_id", "—"),
        "payer_debit_status": statuses.get("PAYER_DEBIT_SUCCESS", "PENDING"),
        "network_status": statuses.get("NETWORK_SUCCESS", "PENDING"),
        "beneficiary_credit_status": statuses.get(
            "BENEFICIARY_CREDIT_SUCCESS", "PENDING"
        ),
        "merchant_callback_status": callback_status,
        "incident_id": incident_id or "—",
        "incident_type": scenario if incident_id else "—",
        "investigation_status": investigation_status,
        "root_cause": item.get("root_cause", "—"),
        "agent_evidence": item.get("agent_evidence", []),
        "recommended_action": item.get("recommended_action", "—"),
        "customer_action": item.get("customer_action", "—"),
    }
