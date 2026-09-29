from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from .investigator import TransactionState, apply_event
except ImportError:  # Managed Flink executes main.py from the archive root.
    from investigator import TransactionState, apply_event


PROPERTIES_PATH = Path("/etc/flink/application_properties.json")

INCIDENT_FIELDS = (
    "incident_id",
    "transaction_id",
    "incident_type",
    "detected_at",
    "amount",
    "merchant_id",
    "order_id",
    "payer_debit_status",
    "network_status",
    "beneficiary_credit_status",
    "merchant_callback_status",
    "callback_http_status",
    "error_code",
    "error_message",
)


def _property_group(document: object, group_id: str) -> dict[str, str]:
    if isinstance(document, list):
        groups = document
    elif isinstance(document, dict):
        groups = document.get("PropertyGroups", document.get("propertyGroups", []))
    else:
        groups = []
    for group in groups:
        if group.get("PropertyGroupId") == group_id:
            return group.get("PropertyMap", {})
    return {}


def configuration() -> dict[str, str]:
    values = {
        "input.stream.arn": os.getenv(
            "INPUT_STREAM_ARN",
            "arn:aws:kinesis:us-east-1:163856954803:stream/upi-payment-events",
        ),
        "output.stream.arn": os.getenv(
            "OUTPUT_STREAM_ARN",
            "arn:aws:kinesis:us-east-1:163856954803:stream/upi-investigations",
        ),
        "aws.region": os.getenv("AWS_REGION", "us-east-1"),
    }
    if PROPERTIES_PATH.exists():
        values.update(
            _property_group(json.loads(PROPERTIES_PATH.read_text()), "demo.config")
        )
    return values


def create_source(stream_arn: str, region: str) -> str:
    return f"""
        CREATE TABLE payment_events (
          transaction_id STRING,
          event_type STRING,
          event_time STRING,
          amount DOUBLE,
          merchant_id STRING,
          order_id STRING,
          `status` STRING,
          metadata ROW<
            synthetic BOOLEAN,
            scenario STRING,
            http_status INT,
            error STRING
          >
        ) PARTITIONED BY (transaction_id)
        WITH (
          'connector' = 'kinesis',
          'stream.arn' = '{stream_arn}',
          'aws.region' = '{region}',
          'source.init.position' = 'LATEST',
          'format' = 'json',
          'json.ignore-parse-errors' = 'true'
        )
    """


def create_sink(stream_arn: str, region: str) -> str:
    return f"""
        CREATE TABLE investigations (
          incident_id STRING,
          transaction_id STRING,
          incident_type STRING,
          detected_at STRING,
          amount DOUBLE,
          merchant_id STRING,
          order_id STRING,
          payer_debit_status STRING,
          network_status STRING,
          beneficiary_credit_status STRING,
          merchant_callback_status STRING,
          callback_http_status INT,
          error_code STRING,
          error_message STRING
        ) PARTITIONED BY (transaction_id)
        WITH (
          'connector' = 'kinesis',
          'stream.arn' = '{stream_arn}',
          'aws.region' = '{region}',
          'format' = 'json',
          'json.timestamp-format.standard' = 'ISO-8601'
        )
    """


def _metadata_http_status(metadata: Any) -> int | None:
    if metadata is None:
        return None
    if isinstance(metadata, dict):
        value = metadata.get("http_status")
    else:
        value = metadata[2]
    return int(value) if value is not None else None


def _process_function() -> Any:
    from pyflink.common import Row
    from pyflink.common.typeinfo import Types
    from pyflink.datastream.functions import KeyedProcessFunction
    from pyflink.datastream.state import ValueStateDescriptor

    class MerchantCallbackInvestigator(KeyedProcessFunction):
        def open(self, runtime_context: Any) -> None:
            self.payer_debit_status = runtime_context.get_state(
                ValueStateDescriptor("payer_debit_status", Types.STRING())
            )
            self.network_status = runtime_context.get_state(
                ValueStateDescriptor("network_status", Types.STRING())
            )
            self.beneficiary_credit_status = runtime_context.get_state(
                ValueStateDescriptor("beneficiary_credit_status", Types.STRING())
            )
            self.merchant_callback_status = runtime_context.get_state(
                ValueStateDescriptor("merchant_callback_status", Types.STRING())
            )
            self.callback_http_status = runtime_context.get_state(
                ValueStateDescriptor("callback_http_status", Types.INT())
            )
            self.incident_emitted = runtime_context.get_state(
                ValueStateDescriptor("incident_emitted", Types.BOOLEAN())
            )

        def process_element(self, value: Any, ctx: Any):
            del ctx
            state = TransactionState(
                payer_debit_status=self.payer_debit_status.value(),
                network_status=self.network_status.value(),
                beneficiary_credit_status=self.beneficiary_credit_status.value(),
                merchant_callback_status=self.merchant_callback_status.value(),
                callback_http_status=self.callback_http_status.value(),
                incident_emitted=self.incident_emitted.value() or False,
            )
            callback_http_status = _metadata_http_status(value[7])
            metadata = (
                {"http_status": callback_http_status}
                if callback_http_status is not None
                else {}
            )
            event = {
                "transaction_id": value[0],
                "event_type": value[1],
                "event_time": value[2],
                "amount": value[3],
                "merchant_id": value[4],
                "order_id": value[5],
                "status": value[6],
                "metadata": metadata,
            }
            incident = apply_event(
                state,
                event,
                detected_at=datetime.now(timezone.utc).isoformat(),
            )

            if state.payer_debit_status is not None:
                self.payer_debit_status.update(state.payer_debit_status)
            if state.network_status is not None:
                self.network_status.update(state.network_status)
            if state.beneficiary_credit_status is not None:
                self.beneficiary_credit_status.update(state.beneficiary_credit_status)
            if state.merchant_callback_status is not None:
                self.merchant_callback_status.update(state.merchant_callback_status)
            if state.callback_http_status is not None:
                self.callback_http_status.update(state.callback_http_status)
            self.incident_emitted.update(state.incident_emitted)

            if incident is not None:
                yield Row(*(incident[field] for field in INCIDENT_FIELDS))

    return MerchantCallbackInvestigator()


def _incident_type_info() -> Any:
    from pyflink.common.typeinfo import Types

    return Types.ROW_NAMED(
        list(INCIDENT_FIELDS),
        [
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.DOUBLE(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.INT(),
            Types.STRING(),
            Types.STRING(),
        ],
    )


def main() -> None:
    from pyflink.common.typeinfo import Types
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.table import StreamTableEnvironment

    config = configuration()
    env = StreamExecutionEnvironment.get_execution_environment()
    table_env = StreamTableEnvironment.create(stream_execution_environment=env)
    table_env.execute_sql(
        create_source(config["input.stream.arn"], config["aws.region"])
    )
    table_env.execute_sql(
        create_sink(config["output.stream.arn"], config["aws.region"])
    )

    payment_events = table_env.to_data_stream(table_env.from_path("payment_events"))
    incidents = (
        payment_events.key_by(lambda event: event[0], key_type=Types.STRING())
        .process(_process_function(), output_type=_incident_type_info())
        .name("merchant-callback-failure-investigator")
    )
    table_env.from_data_stream(incidents).execute_insert("investigations")


if __name__ == "__main__":
    main()
