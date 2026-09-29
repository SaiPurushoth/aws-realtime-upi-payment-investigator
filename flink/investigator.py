from __future__ import annotations

from dataclasses import dataclass
from typing import Any

INCIDENT_TYPE = "MERCHANT_CALLBACK_FAILURE"
ERROR_CODE = "MERCHANT_CALLBACK_500"
ERROR_MESSAGE = "Merchant callback endpoint unavailable"


@dataclass
class TransactionState:
    """The complete keyed state retained for one transaction."""

    payer_debit_status: str | None = None
    network_status: str | None = None
    beneficiary_credit_status: str | None = None
    merchant_callback_status: str | None = None
    callback_http_status: int | None = None
    incident_emitted: bool = False


def apply_event(
    state: TransactionState,
    event: dict[str, Any],
    *,
    detected_at: str,
) -> dict[str, Any] | None:
    """Apply one event and return at most one deterministic incident."""
    event_type = event["event_type"]
    status = event["status"]

    if event_type == "PAYER_DEBIT_SUCCESS":
        state.payer_debit_status = status
    elif event_type == "NETWORK_SUCCESS":
        state.network_status = status
    elif event_type == "BENEFICIARY_CREDIT_SUCCESS":
        state.beneficiary_credit_status = status
    elif event_type == "MERCHANT_CALLBACK_SUCCESS":
        state.merchant_callback_status = status
    elif event_type == "MERCHANT_CALLBACK_FAILED":
        state.merchant_callback_status = status
        metadata = event.get("metadata") or {}
        state.callback_http_status = int(metadata.get("http_status", 500))

    callback_failed_after_completed_payment = (
        state.payer_debit_status == "SUCCESS"
        and state.network_status == "SUCCESS"
        and state.beneficiary_credit_status == "SUCCESS"
        and state.merchant_callback_status == "FAILED"
    )
    if state.incident_emitted or not callback_failed_after_completed_payment:
        return None

    state.incident_emitted = True
    transaction_id = str(event["transaction_id"])
    return {
        "incident_id": f"{transaction_id}-{INCIDENT_TYPE}",
        "transaction_id": transaction_id,
        "incident_type": INCIDENT_TYPE,
        "detected_at": detected_at,
        "amount": event["amount"],
        "merchant_id": event["merchant_id"],
        "order_id": event["order_id"],
        "payer_debit_status": state.payer_debit_status,
        "network_status": state.network_status,
        "beneficiary_credit_status": state.beneficiary_credit_status,
        "merchant_callback_status": state.merchant_callback_status,
        "callback_http_status": state.callback_http_status,
        "error_code": ERROR_CODE,
        "error_message": ERROR_MESSAGE,
    }
