import base64
import json
import unittest

from lambda_trigger.handler import process_records

TRANSACTION_ID = "txn-demo-123"
INCIDENT_ID = f"{TRANSACTION_ID}-MERCHANT_CALLBACK_FAILURE"
INCIDENT = {
    "incident_id": INCIDENT_ID,
    "transaction_id": TRANSACTION_ID,
    "incident_type": "MERCHANT_CALLBACK_FAILURE",
}
RESULT = {
    "transaction_id": TRANSACTION_ID,
    "incident_id": INCIDENT_ID,
    "classification": "MERCHANT_CALLBACK_FAILURE",
    "root_cause": "Merchant callback endpoint unavailable",
    "evidence": ["Beneficiary credit succeeded", "Merchant callback failed: HTTP 500"],
    "recommended_action": "Retry/reconcile merchant notification",
    "customer_action": "No action required from the customer",
}


class Body:
    def read(self) -> bytes:
        return json.dumps(RESULT).encode("utf-8")


class AgentCoreClient:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.calls = []

    def invoke_agent_runtime(self, **kwargs):
        self.calls.append(kwargs)
        if self.fail:
            raise RuntimeError("synthetic failure")
        return {"statusCode": 200, "response": Body()}


class Table:
    def __init__(self) -> None:
        self.calls = []

    def update_item(self, **kwargs):
        self.calls.append(kwargs)


def record(payload: dict, sequence: str = "101") -> dict:
    return {
        "kinesis": {
            "sequenceNumber": sequence,
            "data": base64.b64encode(json.dumps(payload).encode()).decode(),
        }
    }


class HandlerTests(unittest.TestCase):
    def test_invokes_agent_and_stores_completed_result(self) -> None:
        client = AgentCoreClient()
        table = Table()
        result = process_records(
            {"Records": [record(INCIDENT)]}, client, "arn:demo", table
        )

        self.assertEqual(result, {"batchItemFailures": []})
        payload = json.loads(client.calls[0]["payload"])
        self.assertEqual(payload, INCIDENT)
        self.assertGreaterEqual(len(client.calls[0]["runtimeSessionId"]), 33)
        self.assertEqual(table.calls[0]["Key"], {"transaction_id": TRANSACTION_ID})
        values = table.calls[0]["ExpressionAttributeValues"]
        self.assertEqual(values[":completed"], "COMPLETED")
        self.assertEqual(values[":classification"], "MERCHANT_CALLBACK_FAILURE")

    def test_rejects_non_synthetic_record_as_partial_failure(self) -> None:
        client = AgentCoreClient()
        table = Table()
        invalid = {**INCIDENT, "transaction_id": "unknown"}
        result = process_records(
            {"Records": [record(invalid, "202")]}, client, "arn:demo", table
        )

        self.assertEqual(result, {"batchItemFailures": [{"itemIdentifier": "202"}]})
        self.assertEqual(client.calls, [])
        self.assertEqual(table.calls, [])

    def test_agent_error_is_reported_as_partial_failure(self) -> None:
        result = process_records(
            {"Records": [record(INCIDENT, "303")]},
            AgentCoreClient(fail=True),
            "arn:demo",
            Table(),
        )
        self.assertEqual(result, {"batchItemFailures": [{"itemIdentifier": "303"}]})


if __name__ == "__main__":
    unittest.main()
