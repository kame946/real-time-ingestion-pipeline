import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows
import os
import logging
from datetime import datetime


class ParseAndValidate(beam.DoFn):

    def process(self, element):
        import json

        try:
            record = json.loads(element.decode("utf-8"))

            required_fields = ["order_id", "user_id", "product_id", "amount", "event_time"]

            for field in required_fields:
                if field not in record:
                    raise ValueError(f"Missing field {field}")

            if float(record["amount"]) <= 0:
                raise ValueError("Invalid amount")

            yield record

        except Exception:
            yield beam.pvalue.TaggedOutput("dead_letter", element)


class FormatTimestamp(beam.DoFn):

    def process(self, record):

        try:
            record["event_time"] = datetime.fromisoformat(record["event_time"])
            yield record
        except Exception:
            yield beam.pvalue.TaggedOutput("dead_letter", record)


# Configuration
PROJECT = os.environ.get("PROJECT", "YOUR_PROJECT_ID")
REGION = os.environ.get("REGION", "us-central1")
TEMP_BUCKET = os.environ.get("TEMP_BUCKET", "YOUR_BUCKET_NAME")
JOB_NAME = os.environ.get("JOB_NAME", "orders-streaming-jobs")
PUBSUB_SUBSCRIPTION = os.environ.get(
    "PUBSUB_SUBSCRIPTION",
    f"projects/{PROJECT}/subscriptions/orders-sub"
)

BQ_TABLE = os.environ.get(
    "BQ_TABLE",
    f"{PROJECT}:streaming_demo.raw_orders"
)

AGG_TABLE = f"{PROJECT}:streaming_demo.product_sales_1min"

DLQ_TOPIC = f"projects/{PROJECT}/topics/orders-dead-letter"


options = PipelineOptions(
    streaming=True,
    project=PROJECT,
    region=REGION,
    runner="DataflowRunner",
    temp_location=f"gs://{TEMP_BUCKET}/temp",
    staging_location=f"gs://{TEMP_BUCKET}/staging",
    job_name=JOB_NAME
)

logging.getLogger().setLevel(logging.INFO)


with beam.Pipeline(options=options) as p:

    parsed = (
        p
        | "Read from PubSub" >> beam.io.ReadFromPubSub(
            subscription=PUBSUB_SUBSCRIPTION
        )
        | "Parse & Validate JSON"
        >> beam.ParDo(ParseAndValidate()).with_outputs(
            "dead_letter", main="valid"
        )
    )

    valid_records = parsed.valid
    dead_records = parsed.dead_letter

    formatted = (
        valid_records
        | "Format Timestamp"
        >> beam.ParDo(FormatTimestamp()).with_outputs(
            "dead_letter", main="formatted"
        )
    )

    formatted_records = formatted.formatted
    dead_records2 = formatted.dead_letter

    dead_records_all = (
        (dead_records, dead_records2)
        | "Merge DLQ" >> beam.Flatten()
    )

    # Write dead letters
    dead_records_all | "Write DLQ" >> beam.io.WriteToPubSub(
        topic=DLQ_TOPIC
    )

    # Write raw events
    formatted_records | "Write Raw Orders" >> beam.io.WriteToBigQuery(
        table=BQ_TABLE,
        schema="order_id:STRING,user_id:STRING,product_id:STRING,amount:FLOAT,event_time:TIMESTAMP",
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
    )

    # 1-minute aggregation
    aggregated = (
        formatted_records
        | "Add Event Timestamp"
        >> beam.Map(
            lambda x: beam.window.TimestampedValue(
                x, x["event_time"].timestamp()
            )
        )
        | "1 Minute Window"
        >> beam.WindowInto(FixedWindows(60), allowed_lateness=60)
        | "Key by Product"
        >> beam.Map(lambda x: (x["product_id"], x["amount"]))
        | "Sum Sales"
        >> beam.CombinePerKey(sum)
        | "Format Aggregates"
        >> beam.Map(
            lambda x, w=beam.DoFn.WindowParam: {
                "product_id": x[0],
                "window_start": w.start.to_utc_datetime(),
                "total_sales": x[1],
                "order_count": 1
            }
        )
    )

    aggregated | "Write Aggregates" >> beam.io.WriteToBigQuery(
        table=AGG_TABLE,
        schema="product_id:STRING,window_start:TIMESTAMP,total_sales:FLOAT,order_count:INTEGER",
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
    )