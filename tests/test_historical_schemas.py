import json
import unittest
from pathlib import Path

from flink.investigator import TransactionState, apply_event
from simulator.producer import Scenario, build_events


SCHEMA_DIR = Path(__file__).parents[1] / "infra" / "historical"


class HistoricalSchemaTests(unittest.TestCase):
    def test_payment_schema_matches_producer_and_timestamp_is_date_time(self):
        schema = json.loads((SCHEMA_DIR / "payment_events.schema.json").read_text())
        event = build_events(Scenario.MERCHANT_CALLBACK_FAILURE)[-1]

        self.assertEqual(set(event), set(schema["properties"]))
        self.assertEqual("date-time", schema["properties"]["event_time"]["format"])
        self.assertTrue(set(event["metadata"]) <= set(schema["properties"]["metadata"]["properties"]))

    def test_investigation_schema_matches_flink_and_timestamp_is_date_time(self):
        events = build_events(Scenario.MERCHANT_CALLBACK_FAILURE)
        state = TransactionState()
        incident = None
        for event in events:
            incident = apply_event(state, event, detected_at="2026-09-29T00:00:00+00:00") or incident
        schema = json.loads(
            (SCHEMA_DIR / "investigation_events.schema.json").read_text()
        )

        self.assertIsNotNone(incident)
        self.assertEqual(set(incident), set(schema["properties"]))
        self.assertEqual("date-time", schema["properties"]["detected_at"]["format"])


if __name__ == "__main__":
    unittest.main()
