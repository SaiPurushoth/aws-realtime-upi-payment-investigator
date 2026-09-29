from __future__ import annotations

import json
from typing import Any

INCIDENT_TYPE = "MERCHANT_CALLBACK_FAILURE"
INCIDENT_FIELDS = ("incident_id", "transaction_id", "incident_type")
RESULT_FIELDS = (
    "transaction_id",
    "incident_id",
    "classification",
    "root_cause",
    "evidence",
    "recommended_action",
    "customer_action",
)


def validate_incident(payload: object) -> dict[str, str]:
    if not isinstance(payload, dict):
        raise TypeError("incident payload must be an object")

    incident: dict[str, str] = {}
    for field in INCIDENT_FIELDS:
        value = payload.get(field)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{field} must be a non-empty string")
        incident[field] = value.strip()

    if incident["incident_type"] != INCIDENT_TYPE:
        raise ValueError(f"unsupported incident_type: {incident['incident_type']}")
    return incident


def build_prompt(incident: dict[str, str]) -> str:
    return (
        "Investigate this incident. You must call get_transaction_evidence with "
        "the supplied transaction_id before reaching a conclusion. Return only "
        "the strict JSON object required by the system prompt. Incident: "
        + json.dumps(incident, sort_keys=True)
    )


def result_text(result: object) -> str:
    message = getattr(result, "message", None)
    if isinstance(message, dict):
        content = message.get("content", [])
        text = "".join(
            block.get("text", "")
            for block in content
            if isinstance(block, dict) and isinstance(block.get("text"), str)
        )
        if text:
            return text
    return str(result)


def evidence_tool_was_used(messages: object, transaction_id: str) -> bool:
    if not isinstance(messages, list):
        return False
    for message in messages:
        if not isinstance(message, dict):
            continue
        for block in message.get("content", []):
            if not isinstance(block, dict):
                continue
            tool_use = block.get("toolUse")
            if not isinstance(tool_use, dict):
                continue
            tool_input = tool_use.get("input", {})
            if (
                tool_use.get("name") == "get_transaction_evidence"
                and isinstance(tool_input, dict)
                and tool_input.get("transaction_id") == transaction_id
            ):
                return True
    return False


def parse_result(raw_result: object, incident: dict[str, str]) -> dict[str, Any]:
    text = result_text(raw_result).strip()
    if text.startswith("```json") and text.endswith("```"):
        text = text[7:-3].strip()
    elif text.startswith("```") and text.endswith("```"):
        text = text[3:-3].strip()

    try:
        result = json.loads(text)
    except json.JSONDecodeError as first_error:
        result = None
        decoder = json.JSONDecoder()
        for index in range(len(text) - 1, -1, -1):
            if text[index] != "{":
                continue
            try:
                candidate, end = decoder.raw_decode(text[index:])
            except json.JSONDecodeError:
                continue
            if not text[index + end :].strip():
                result = candidate
                break
        if result is None:
            raise ValueError("agent response was not valid JSON") from first_error

    if not isinstance(result, dict):
        raise ValueError("agent response must be a JSON object")
    if set(result) != set(RESULT_FIELDS):
        raise ValueError("agent response did not match the required JSON fields")
    if result["transaction_id"] != incident["transaction_id"]:
        raise ValueError("agent response changed transaction_id")
    if result["incident_id"] != incident["incident_id"]:
        raise ValueError("agent response changed incident_id")
    if not isinstance(result["evidence"], list) or not all(
        isinstance(item, str) and item.strip() for item in result["evidence"]
    ):
        raise ValueError("agent evidence must be a non-empty list of strings")
    if not result["evidence"]:
        raise ValueError("agent evidence cannot be empty")

    for field in RESULT_FIELDS:
        if field == "evidence":
            continue
        if not isinstance(result[field], str) or not result[field].strip():
            raise ValueError(f"agent response field {field} must be a string")
    return result
