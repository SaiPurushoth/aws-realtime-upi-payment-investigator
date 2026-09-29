from __future__ import annotations

import argparse
import json
from collections.abc import Sequence

try:
    from .app import run_investigation
except ImportError:  # python agent/local_test.py
    from app import run_investigation


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Run the UPI investigator locally against existing AWS evidence."
    )
    result.add_argument("--transaction-id", required=True)
    result.add_argument("--incident-id")
    return result


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    incident_id = args.incident_id or (
        f"{args.transaction_id}-MERCHANT_CALLBACK_FAILURE"
    )
    result = run_investigation(
        {
            "incident_id": incident_id,
            "transaction_id": args.transaction_id,
            "incident_type": "MERCHANT_CALLBACK_FAILURE",
        }
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
