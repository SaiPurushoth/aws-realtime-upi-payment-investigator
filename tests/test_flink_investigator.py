import unittest
from dataclasses import fields
from datetime import datetime, timezone
from decimal import Decimal

from flink.investigator import TransactionState, apply_event
from flink.main import create_sink, create_source
from simulator.producer import Scenario, build_events

DETECTED_AT = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc).isoformat()
EXPECTED_STATE_FIELDS = {
    "payer_debit_status",
    "network_status",
    "beneficiary_credit_status",
    "merchant_callback_status",
    "callback_http_status",
    "incident_emitted",
}


def investigate(events: list[dict], duplicate_failure: bool = False) -> list[dict]:
    state = TransactionState()
    stream = list(events)
    if duplicate_failure:
        stream.append(events[-1].copy())
    incidents = []
    for event in stream:
        incident = apply_event(state, event, detected_at=DETECTED_AT)
        if incident is not None:
            incidents.append(incident)
    return incidents


class FlinkInvestigatorTests(unittest.TestCase):
    def test_state_contains_only_requested_fields(self) -> None:
        self.assertEqual(
            {field.name for field in fields(TransactionState)}, EXPECTED_STATE_FIELDS
        )

    def test_normal_emits_zero_incidents(self) -> None:
        events = build_events(Scenario.NORMAL)
        self.assertEqual(investigate(events), [])

    def test_callback_failure_emits_exactly_one_flat_incident(self) -> None:
        events = build_events(
            Scenario.MERCHANT_CALLBACK_FAILURE,
            amount=Decimal("780.00"),
            merchant_id="merchant-demo-780",
        )

        incidents = investigate(events)

        self.assertEqual(len(incidents), 1)
        incident = incidents[0]
        transaction_id = events[0]["transaction_id"]
        self.assertEqual(
            incident["incident_id"],
            f"{transaction_id}-MERCHANT_CALLBACK_FAILURE",
        )
        self.assertEqual(incident["transaction_id"], transaction_id)
        self.assertEqual(incident["incident_type"], "MERCHANT_CALLBACK_FAILURE")
        self.assertEqual(incident["detected_at"], DETECTED_AT)
        self.assertEqual(incident["amount"], Decimal("780.00"))
        self.assertEqual(incident["merchant_id"], "merchant-demo-780")
        self.assertEqual(incident["order_id"], events[0]["order_id"])
        self.assertEqual(incident["payer_debit_status"], "SUCCESS")
        self.assertEqual(incident["network_status"], "SUCCESS")
        self.assertEqual(incident["beneficiary_credit_status"], "SUCCESS")
        self.assertEqual(incident["merchant_callback_status"], "FAILED")
        self.assertEqual(incident["callback_http_status"], 500)
        self.assertEqual(incident["error_code"], "MERCHANT_CALLBACK_500")
        self.assertEqual(
            incident["error_message"],
            "Merchant callback endpoint unavailable",
        )
        self.assertTrue(all(not isinstance(value, dict) for value in incident.values()))

    def test_duplicate_failure_still_emits_one_incident(self) -> None:
        events = build_events(Scenario.MERCHANT_CALLBACK_FAILURE)
        self.assertEqual(len(investigate(events, duplicate_failure=True)), 1)

    def test_kinesis_tables_use_expected_streams_and_transaction_partition(
        self,
    ) -> None:
        source_arn = (
            "arn:aws:kinesis:us-east-1:123456789012:stream/upi-payment-events"
        )
        sink_arn = (
            "arn:aws:kinesis:us-east-1:123456789012:stream/upi-investigations"
        )
        source = create_source(source_arn, "us-east-1")
        sink = create_sink(sink_arn, "us-east-1")
        self.assertIn(f"'stream.arn' = '{source_arn}'", source)
        self.assertIn(f"'stream.arn' = '{sink_arn}'", sink)
        self.assertIn("'source.init.position' = 'LATEST'", source)
        self.assertIn("PARTITIONED BY (transaction_id)", source)
        self.assertIn("PARTITIONED BY (transaction_id)", sink)


if __name__ == "__main__":
    unittest.main()
