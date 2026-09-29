import unittest
from decimal import Decimal

from dashboard.view_model import build_view_model


def transaction_item(*, callback_failed: bool) -> dict:
    callback_type = (
        "MERCHANT_CALLBACK_FAILED" if callback_failed else "MERCHANT_CALLBACK_SUCCESS"
    )
    callback_status = "FAILED" if callback_failed else "SUCCESS"
    scenario = "MERCHANT_CALLBACK_FAILURE" if callback_failed else "NORMAL"
    event_types = [
        ("PAYMENT_INITIATED", "PENDING"),
        ("PAYER_DEBIT_SUCCESS", "SUCCESS"),
        ("NETWORK_SUCCESS", "SUCCESS"),
        ("BENEFICIARY_CREDIT_SUCCESS", "SUCCESS"),
        (callback_type, callback_status),
    ]
    return {
        "transaction_id": "txn-demo-dashboard",
        "scenario": scenario,
        "amount": Decimal("499.00"),
        "merchant_id": "merchant-demo-001",
        "order_id": "order-demo-dashboard",
        "evidence": [
            {"event_type": event_type, "status": status}
            for event_type, status in event_types
        ],
    }


class DashboardViewModelTests(unittest.TestCase):
    def test_normal_payment_shows_successful_journey_without_investigation(self):
        view = build_view_model(transaction_item(callback_failed=False))

        self.assertEqual(view["amount"], 499.0)
        self.assertEqual(view["payer_debit_status"], "SUCCESS")
        self.assertEqual(view["network_status"], "SUCCESS")
        self.assertEqual(view["beneficiary_credit_status"], "SUCCESS")
        self.assertEqual(view["merchant_callback_status"], "SUCCESS")
        self.assertEqual(view["incident_id"], "—")
        self.assertEqual(view["investigation_status"], "NOT REQUIRED")

    def test_callback_failure_shows_pending_until_agent_result_is_stored(self):
        view = build_view_model(transaction_item(callback_failed=True))

        self.assertEqual(view["merchant_callback_status"], "FAILED")
        self.assertEqual(view["incident_id"], "—")
        self.assertEqual(view["investigation_status"], "PENDING")

    def test_completed_investigation_uses_dynamodb_result_fields(self):
        item = transaction_item(callback_failed=True)
        item.update(
            {
                "incident_id": "txn-demo-dashboard-MERCHANT_CALLBACK_FAILURE",
                "investigation_status": "COMPLETED",
                "root_cause": "Merchant callback endpoint unavailable",
                "agent_evidence": ["Beneficiary credit succeeded"],
                "recommended_action": "Retry merchant notification",
                "customer_action": "Do not make another payment",
            }
        )

        view = build_view_model(item)

        self.assertEqual(view["incident_type"], "MERCHANT_CALLBACK_FAILURE")
        self.assertEqual(view["investigation_status"], "COMPLETED")
        self.assertEqual(view["agent_evidence"], ["Beneficiary credit succeeded"])
        self.assertEqual(view["customer_action"], "Do not make another payment")


if __name__ == "__main__":
    unittest.main()
