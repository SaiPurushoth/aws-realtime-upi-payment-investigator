import json
import unittest
import uuid
from datetime import datetime, timezone
from decimal import Decimal

from simulator.producer import (
    CALLBACK_FAILURE_EVENT_TYPES,
    PAYMENT_STREAM_NAME,
    SUCCESSFUL_EVENT_TYPES,
    Scenario,
    build_events,
    produce_transaction,
)

FIXED_TIME = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)
FIXED_IDS = iter(
    [
        uuid.UUID("00000000-0000-0000-0000-000000000001"),
        uuid.UUID("00000000-0000-0000-0000-000000000002"),
    ]
)
REQUIRED_EVENT_FIELDS = {
    "transaction_id",
    "event_type",
    "event_time",
    "amount",
    "merchant_id",
    "order_id",
    "status",
    "metadata",
}


def fixed_id() -> uuid.UUID:
    return next(FIXED_IDS)


class FakeKinesisClient:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def put_record(self, **kwargs) -> dict[str, str]:
        self.calls.append(kwargs)
        return {
            "ShardId": "shardId-000000000000",
            "SequenceNumber": str(len(self.calls)),
        }


class FakeTable:
    def __init__(self) -> None:
        self.items: list[dict] = []

    def put_item(self, *, Item: dict) -> dict:
        self.items.append(Item)
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}


class SimulatorTests(unittest.TestCase):
    def setUp(self) -> None:
        global FIXED_IDS
        FIXED_IDS = iter(
            [
                uuid.UUID("00000000-0000-0000-0000-000000000001"),
                uuid.UUID("00000000-0000-0000-0000-000000000002"),
            ]
        )

    def test_normal_scenario_has_exact_successful_lifecycle(self) -> None:
        events = build_events(
            Scenario.NORMAL,
            clock=lambda: FIXED_TIME,
            id_factory=fixed_id,
        )

        self.assertEqual(
            tuple(event["event_type"] for event in events), SUCCESSFUL_EVENT_TYPES
        )
        self.assertEqual(events[-1]["status"], "SUCCESS")
        self.assertTrue(all(set(event) == REQUIRED_EVENT_FIELDS for event in events))
        self.assertTrue(all(event["metadata"]["synthetic"] for event in events))

    def test_callback_failure_has_required_metadata(self) -> None:
        events = build_events(
            Scenario.MERCHANT_CALLBACK_FAILURE,
            clock=lambda: FIXED_TIME,
            id_factory=fixed_id,
        )

        self.assertEqual(
            tuple(event["event_type"] for event in events),
            CALLBACK_FAILURE_EVENT_TYPES,
        )
        callback = events[-1]
        self.assertEqual(callback["status"], "FAILED")
        self.assertEqual(callback["metadata"]["http_status"], 500)
        self.assertEqual(
            callback["metadata"]["error"],
            "Merchant callback endpoint unavailable",
        )

    def test_produce_transaction_publishes_and_writes_evidence(self) -> None:
        kinesis = FakeKinesisClient()
        table = FakeTable()

        receipt = produce_transaction(
            kinesis,
            table,
            Scenario.NORMAL,
            clock=lambda: FIXED_TIME,
            id_factory=fixed_id,
        )

        self.assertEqual(receipt["records_sent"], 5)
        self.assertEqual(len(kinesis.calls), 5)
        self.assertEqual(len(table.items), 1)
        self.assertTrue(
            all(call["StreamName"] == PAYMENT_STREAM_NAME for call in kinesis.calls)
        )
        self.assertEqual(len({call["PartitionKey"] for call in kinesis.calls}), 1)
        payloads = [json.loads(call["Data"]) for call in kinesis.calls]
        self.assertEqual(
            tuple(payload["event_type"] for payload in payloads),
            SUCCESSFUL_EVENT_TYPES,
        )
        evidence = table.items[0]
        self.assertEqual(evidence["event_count"], Decimal(5))
        self.assertEqual(evidence["transaction_id"], receipt["transaction_id"])
        self.assertEqual(len(evidence["evidence"]), 5)

    def test_only_declared_scenarios_are_accepted(self) -> None:
        with self.assertRaises(TypeError):
            build_events("normal")  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
