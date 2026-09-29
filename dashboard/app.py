from __future__ import annotations

import os
from typing import Any

import boto3
import streamlit as st

from dashboard.view_model import build_view_model
from simulator.producer import Scenario, produce_transaction

TABLE_NAME = os.getenv("TABLE_NAME", "upi-demo-transactions")

st.set_page_config(
    page_title="Real-Time UPI Payment Investigator",
    page_icon="🔎",
    layout="wide",
)


@st.cache_resource
def aws_resources() -> tuple[Any, Any]:
    """Use boto3's normal profile and credential resolution chain."""
    session = boto3.Session()
    return (
        session.client("kinesis"),
        session.resource("dynamodb").Table(TABLE_NAME),
    )


def get_transaction(table: Any, transaction_id: str) -> dict[str, Any] | None:
    response = table.get_item(
        Key={"transaction_id": transaction_id},
        ConsistentRead=True,
    )
    return response.get("Item")


def generate(scenario: Scenario) -> None:
    kinesis, table = aws_resources()
    with st.spinner("Sending synthetic payment events…"):
        receipt = produce_transaction(kinesis, table, scenario)
    st.session_state.transaction_id = receipt["transaction_id"]
    st.success(f"Created {receipt['transaction_id']}")


def status_text(value: object) -> str:
    status = str(value)
    if status in {"SUCCESS", "COMPLETED"}:
        return f"✅ {status}"
    if status == "FAILED":
        return f"❌ {status}"
    if status == "PENDING":
        return f"⏳ {status}"
    return status


def show_field(label: str, value: object) -> None:
    st.caption(label)
    st.write(value)


@st.fragment(run_every="2s")
def live_transaction() -> None:
    transaction_id = st.session_state.get("transaction_id")
    if not transaction_id:
        st.info("Generate a synthetic payment to begin.")
        return

    _, table = aws_resources()
    try:
        item = get_transaction(table, transaction_id)
    except Exception as exc:  # noqa: BLE001 - show AWS profile/config errors locally.
        st.error(f"Could not read {TABLE_NAME}: {exc}")
        return
    if not item:
        st.warning("The transaction has not appeared in DynamoDB yet.")
        return

    view = build_view_model(item)

    st.subheader("TRANSACTION")
    transaction_columns = st.columns(4)
    with transaction_columns[0]:
        show_field("Transaction ID", view["transaction_id"])
    with transaction_columns[1]:
        amount = view["amount"]
        show_field("Amount", f"₹{amount:,.2f}" if isinstance(amount, float) else amount)
    with transaction_columns[2]:
        show_field("Merchant", view["merchant_id"])
    with transaction_columns[3]:
        show_field("Order", view["order_id"])

    st.subheader("PAYMENT JOURNEY")
    journey_columns = st.columns(4)
    journey = (
        ("Customer Debit", view["payer_debit_status"]),
        ("Network", view["network_status"]),
        ("Beneficiary Credit", view["beneficiary_credit_status"]),
        ("Merchant Callback", view["merchant_callback_status"]),
    )
    for column, (label, value) in zip(journey_columns, journey, strict=True):
        with column:
            show_field(label, status_text(value))

    st.subheader("STREAMING DETECTION")
    detection_columns = st.columns(2)
    with detection_columns[0]:
        show_field("Incident ID", view["incident_id"])
    with detection_columns[1]:
        show_field("Incident Type", view["incident_type"])

    st.subheader("AI INVESTIGATION")
    show_field("Investigation Status", status_text(view["investigation_status"]))
    show_field("Root Cause", view["root_cause"])
    st.caption("Evidence")
    evidence = view["agent_evidence"]
    if evidence:
        for fact in evidence:
            st.markdown(f"- {fact}")
    else:
        st.write("—")
    action_columns = st.columns(2)
    with action_columns[0]:
        show_field("Recommended Action", view["recommended_action"])
    with action_columns[1]:
        show_field("Customer Action", view["customer_action"])


st.title("Real-Time UPI Payment Investigator")
st.caption(
    "Synthetic AWS meetup demo. No real UPI, bank, NPCI, BHIM, or payment "
    "infrastructure is used."
)

normal_column, failure_column, _ = st.columns([1, 1, 2])
with normal_column:
    if st.button("Generate Normal Payment", type="primary", use_container_width=True):
        try:
            generate(Scenario.NORMAL)
        except Exception as exc:  # noqa: BLE001 - make AWS errors visible in the demo.
            st.error(f"Could not generate payment: {exc}")
with failure_column:
    if st.button("Inject Merchant Callback Failure", use_container_width=True):
        try:
            generate(Scenario.MERCHANT_CALLBACK_FAILURE)
        except Exception as exc:  # noqa: BLE001 - make AWS errors visible in the demo.
            st.error(f"Could not generate payment: {exc}")

st.divider()
live_transaction()

st.divider()
st.subheader("Historical Analytics")
st.caption(
    "Payment and investigation events are asynchronously archived to Amazon S3 "
    "Tables for Iceberg/Athena analytics."
)
