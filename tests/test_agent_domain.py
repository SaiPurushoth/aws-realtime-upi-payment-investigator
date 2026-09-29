import json
import unittest
from decimal import Decimal

from agent.domain import (
    build_prompt,
    evidence_tool_was_used,
    parse_result,
    validate_incident,
)
from agent.evidence import EVIDENCE_FIELDS, read_transaction_evidence

INCIDENT = {
    "incident_id": "txn-demo-123-MERCHANT_CALLBACK_FAILURE",
    "transaction_id": "txn-demo-123",
    "incident_type": "MERCHANT_CALLBACK_FAILURE",
}


class Table:
    def __init__(self, item):
        self.item = item
        self.calls = []

    def get_item(self, **kwargs):
        self.calls.append(kwargs)
        return {"Item": self.item} if self.item is not None else {}


def callback_failure_item():
    return {
        "transaction_id": "txn-demo-123",
        "amount": Decimal("780.00"),
        "synthetic": True,
        "evidence": [
            {"event_type": "PAYMENT_INITIATED", "status": "PENDING"},
            {"event_type": "PAYER_DEBIT_SUCCESS", "status": "SUCCESS"},
            {"event_type": "NETWORK_SUCCESS", "status": "SUCCESS"},
            {"event_type": "BENEFICIARY_CREDIT_SUCCESS", "status": "SUCCESS"},
            {
                "event_type": "MERCHANT_CALLBACK_FAILED",
                "status": "FAILED",
                "metadata": {
                    "http_status": Decimal(500),
                    "error": "Merchant callback endpoint unavailable",
                },
            },
        ],
    }


class AgentDomainTests(unittest.TestCase):
    def test_validates_three_field_incident_and_builds_tool_prompt(self) -> None:
        incident = validate_incident({**INCIDENT, "ignored": "value"})
        self.assertEqual(incident, INCIDENT)
        prompt = build_prompt(incident)
        self.assertIn("get_transaction_evidence", prompt)
        self.assertIn(INCIDENT["transaction_id"], prompt)

    def test_rejects_unsupported_incident(self) -> None:
        with self.assertRaises(ValueError):
            validate_incident({**INCIDENT, "incident_type": "OTHER"})

    def test_confirms_matching_evidence_tool_call(self) -> None:
        messages = [
            {
                "role": "assistant",
                "content": [
                    {
                        "toolUse": {
                            "name": "get_transaction_evidence",
                            "input": {"transaction_id": INCIDENT["transaction_id"]},
                        }
                    }
                ],
            }
        ]
        self.assertTrue(evidence_tool_was_used(messages, INCIDENT["transaction_id"]))
        self.assertFalse(evidence_tool_was_used(messages, "another-transaction"))

    def test_reads_only_requested_transaction_evidence(self) -> None:
        table = Table(callback_failure_item())
        evidence = read_transaction_evidence(table, INCIDENT["transaction_id"])

        self.assertEqual(tuple(evidence), EVIDENCE_FIELDS)
        self.assertEqual(evidence["amount"], 780)
        self.assertEqual(evidence["payer_debit_status"], "SUCCESS")
        self.assertEqual(evidence["network_status"], "SUCCESS")
        self.assertEqual(evidence["beneficiary_credit_status"], "SUCCESS")
        self.assertEqual(evidence["merchant_callback_status"], "FAILED")
        self.assertEqual(evidence["callback_http_status"], 500)
        self.assertEqual(
            evidence["callback_error"],
            "Merchant callback endpoint unavailable",
        )
        self.assertEqual(
            table.calls,
            [
                {
                    "Key": {"transaction_id": INCIDENT["transaction_id"]},
                    "ConsistentRead": True,
                }
            ],
        )

    def test_rejects_missing_or_non_synthetic_evidence(self) -> None:
        with self.assertRaises(ValueError):
            read_transaction_evidence(Table(None), INCIDENT["transaction_id"])
        item = callback_failure_item()
        item["synthetic"] = False
        with self.assertRaises(ValueError):
            read_transaction_evidence(Table(item), INCIDENT["transaction_id"])

    def test_parses_exact_strict_json_result(self) -> None:
        response = {
            "transaction_id": INCIDENT["transaction_id"],
            "incident_id": INCIDENT["incident_id"],
            "classification": "MERCHANT_NOTIFICATION_FAILURE",
            "root_cause": "Transfer succeeded; merchant callback failed.",
            "evidence": ["Beneficiary credit status is SUCCESS."],
            "recommended_action": "Retry and reconcile merchant notification.",
            "customer_action": "Do not make another payment.",
        }
        self.assertEqual(parse_result(json.dumps(response), INCIDENT), response)
        with_reasoning = "<thinking>Used transaction evidence.</thinking>\n" + json.dumps(
            response
        )
        self.assertEqual(parse_result(with_reasoning, INCIDENT), response)

    def test_rejects_result_that_changes_transaction_context(self) -> None:
        response = {
            "transaction_id": "invented",
            "incident_id": INCIDENT["incident_id"],
            "classification": "failure",
            "root_cause": "failure",
            "evidence": ["evidence"],
            "recommended_action": "retry callback",
            "customer_action": "do not pay again",
        }
        with self.assertRaises(ValueError):
            parse_result(json.dumps(response), INCIDENT)


if __name__ == "__main__":
    unittest.main()
