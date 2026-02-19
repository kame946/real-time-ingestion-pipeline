import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os


class ParseJSON(beam.DoFn):
    def process(self, element):
        import json
        yield json.loads(element.decode("utf-8"))


# Configuration via environment variables (with sensible defaults)
PROJECT = os.environ.get("PROJECT", "data-engineering-487506")
REGION = os.environ.get("REGION", "us-central1")
TEMP_BUCKET = os.environ.get("TEMP_BUCKET", "data-engineering-487506-temp")
JOB_NAME = os.environ.get("JOB_NAME", "orders-streaming-jobs")
PUBSUB_SUBSCRIPTION = os.environ.get("PUBSUB_SUBSCRIPTION", f"projects/{PROJECT}/subscriptions/orders-sub")
BQ_TABLE = os.environ.get("BQ_TABLE", f"{PROJECT}:streaming_demo.raw_orders")

options = PipelineOptions(
    streaming=True,
    project=PROJECT,
    region=REGION,
    runner="DataflowRunner",
    temp_location=f"gs://{TEMP_BUCKET}/temp",
    staging_location=f"gs://{TEMP_BUCKET}/staging",
    job_name=JOB_NAME
)


with beam.Pipeline(options=options) as p:
    (
        p
        | "Read from PubSub" >> beam.io.ReadFromPubSub(
            subscription=PUBSUB_SUBSCRIPTION
        )
        | "Parse JSON" >> beam.ParDo(ParseJSON())
        | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            table=BQ_TABLE,
            schema="order_id:STRING,user_id:STRING,product_id:STRING,amount:FLOAT,event_time:TIMESTAMP",
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
        )
    )
