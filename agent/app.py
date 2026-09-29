from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any

import boto3
from bedrock_agentcore.runtime import BedrockAgentCoreApp
from strands import Agent, tool
from strands.models import BedrockModel

try:
    from .domain import (
        build_prompt,
        evidence_tool_was_used,
        parse_result,
        validate_incident,
    )
    from .evidence import read_transaction_evidence
except ImportError:  # Direct execution from the agent directory.
    from domain import (
        build_prompt,
        evidence_tool_was_used,
        parse_result,
        validate_incident,
    )
    from evidence import read_transaction_evidence

AGENT_NAME = "upi-payment-investigator"
TABLE_NAME = os.getenv("TABLE_NAME", "upi-demo-transactions")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
MODEL_ID = os.getenv("BEDROCK_MODEL_ID", "amazon.nova-lite-v1:0")

SYSTEM_PROMPT = """You are a payment operations investigator.

You are investigating synthetic UPI-like transactions.

Always inspect available transaction evidence before forming a conclusion.

Never fabricate transaction or banking status.

For the merchant callback failure scenario determine whether:

- payer debit succeeded
- payment network succeeded
- beneficiary credit succeeded
- merchant callback failed

Never execute refunds, reversals or financial transactions.

Return strict JSON:

{
  "transaction_id": "...",
  "incident_id": "...",
  "classification": "...",
  "root_cause": "...",
  "evidence": ["..."],
  "recommended_action": "...",
  "customer_action": "..."
}

If beneficiary credit succeeded but merchant callback failed, the issue is
with merchant notification/application state rather than the actual transfer.

The recommended action should be to retry/reconcile merchant notification.

The customer must not be asked to make another payment."""

app = BedrockAgentCoreApp()


def _transaction_table() -> Any:
    return boto3.resource("dynamodb", region_name=AWS_REGION).Table(TABLE_NAME)


@tool
def get_transaction_evidence(transaction_id: str) -> dict[str, Any]:
    """Read the synthetic transaction evidence needed for an investigation.

    Args:
        transaction_id: Synthetic transaction identifier from the incident.
    """
    return read_transaction_evidence(_transaction_table(), transaction_id)


def create_agent() -> Agent:
    model = BedrockModel(
        model_id=MODEL_ID,
        region_name=AWS_REGION,
        temperature=0.0,
        max_tokens=700,
    )
    return Agent(
        name=AGENT_NAME,
        model=model,
        system_prompt=SYSTEM_PROMPT,
        tools=[get_transaction_evidence],
        callback_handler=None,
    )


def run_investigation(
    payload: object,
    agent_factory: Callable[[], Any] = create_agent,
) -> dict[str, Any]:
    incident = validate_incident(payload)
    agent = agent_factory()
    result = agent(build_prompt(incident))
    if not evidence_tool_was_used(agent.messages, incident["transaction_id"]):
        raise ValueError("agent did not inspect transaction evidence")
    return parse_result(result, incident)


@app.entrypoint
def investigate(payload: dict[str, Any], context: Any = None) -> dict[str, Any]:
    del context
    return run_investigation(payload)


if __name__ == "__main__":
    app.run()
