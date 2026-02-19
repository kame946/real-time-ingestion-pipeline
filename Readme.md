## Near Real-Time Analytics Platform (GCP)

## Overview

This project implements a near real-time streaming analytics pipeline on Google Cloud Platform using Pub/Sub, Dataflow (Apache Beam), and BigQuery.

The system simulates e-commerce order events and processes them in streaming mode with sub-minute latency.

---

## Architecture

Event Generator (Python)
↓
Pub/Sub Topic
↓
Dataflow (Apache Beam - Streaming)
↓
BigQuery (Partitioned + Clustered Table)


### Components

- **Pub/Sub** – Event ingestion layer  
- **Dataflow (Apache Beam)** – Streaming data processing  
- **BigQuery** – Analytics storage layer  
- **Cloud Storage (GCS)** – Dataflow staging and temp storage  

---

## Features

- Real-time event ingestion  
- Streaming pipeline deployment using DataflowRunner  
- Automatic scaling via Dataflow Streaming Engine  
- Partitioned BigQuery table (by event_time)  
- Clustered BigQuery table (by product_id)  
- IAM-based secure service account configuration  
- Manual subscription management (production-style setup)  

---

## Project Structure



realtime_ingestion_sim/
│
├── publisher.py # Simulates order events
├── pipeline.py # Apache Beam streaming pipeline
├── requirements.txt
└── README.md


---

# Setup Instructions

## 1. Enable Required APIs

```bash
gcloud services enable dataflow.googleapis.com
gcloud services enable pubsub.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable storage.googleapis.com
```
## 2. Create Pub/Sub Topic
```bash
gcloud pubsub topics create orders-topic
```
## 3. Create Subscription (Recommended Approach)
```bash
gcloud pubsub subscriptions create orders-sub \
  --topic=orders-topic
```

This avoids permission issues caused by automatic subscription creation.

## 4. Create BigQuery Dataset
bq mk streaming_demo

## 5. Create Partitioned & Clustered Table
```bash CREATE TABLE streaming_demo.raw_orders (
  order_id STRING,
  user_id STRING,
  product_id STRING,
  amount FLOAT64,
  event_time TIMESTAMP
)
PARTITION BY DATE(event_time)
CLUSTER BY product_id;
```

## 6. Create Regional GCS Bucket (Must Match Dataflow Region)
```bash 
gsutil mb -l us-central1 gs://YOUR_BUCKET_NAME 
```


#### Verify region:
```bash
gsutil ls -L -b gs://YOUR_BUCKET_NAME
```

Location constraint must equal Dataflow region.

## 7. Grant Required IAM Roles to Dataflow Service Account

#### Identify project number:
```bash
gcloud projects describe YOUR_PROJECT_ID --format='value(projectNumber)'
```

#### Service account format:

PROJECT_NUMBER-compute@developer.gserviceaccount.com


Grant roles:
```bash
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:PROJECT_NUMBER-compute@developer.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:PROJECT_NUMBER-compute@developer.gserviceaccount.com" \
  --role="roles/dataflow.worker"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:PROJECT_NUMBER-compute@developer.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:PROJECT_NUMBER-compute@developer.gserviceaccount.com" \
  --role="roles/pubsub.subscriber"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:PROJECT_NUMBER-compute@developer.gserviceaccount.com" \
  --role="roles/pubsub.viewer"
```
### Running the Event Publisher

Install dependencies:
```bash
pip install google-cloud-pubsub faker
```

Authenticate:
```bash
gcloud auth application-default login
```

Run:
```bash
python publisher.py
```

This continuously publishes simulated order events to Pub/Sub.

Deploying the Streaming Pipeline

Install Beam:
```bash
pip install apache-beam[gcp]
```

Ensure Python 3.10 or 3.11 is recommended for stability.

Deploy:
```bash
python pipeline.py
```

### The pipeline will:

1. Read from Pub/Sub subscription

2. Parse JSON events

3. Write to BigQuery

4. Run in streaming mode

5. Monitoring the Pipeline

6. Go to Google Cloud Console

7. Navigate to Dataflow → Jobs

8. Verify job status is Running

9. Monitor worker scaling and throughput

10. Verifying Data in BigQuery

#### Run:
```bash
SELECT COUNT(*) FROM streaming_demo.raw_orders;
```

Rows should continuously increase while publisher is running.

## Common Deployment Issues & Fixes
1. Staging File Error (pipeline.pb)

Cause:

Bucket region mismatch

Missing storage permissions

Fix:

Ensure bucket region matches Dataflow region

Grant roles/storage.objectAdmin

2. Pub/Sub Permission Errors

Cause:

Worker service account lacks subscription access

Fix:

Grant roles/pubsub.subscriber

Use manual subscription instead of topic auto-creation

3. Preflight Validation Failure

Cause:

Missing IAM permissions

Fix:

Verify required roles assigned to compute service account

Performance & Design Decisions

Used partitioning on event_time for query efficiency

Used clustering on product_id for analytical filtering

Used manual subscription for IAM clarity and production realism

Leveraged Dataflow Streaming Engine for autoscaling

Designed for sub-60 second processing latency

Next Enhancements

Event-time windowing (1-minute fixed windows)

Aggregations (sales per product per minute)

Dead-letter queue for malformed events

Late data handling

Data quality validation

Materialized views for BI dashboards

## Resume-Ready Summary

Designed and deployed a real-time streaming analytics platform on GCP using Pub/Sub, Dataflow (Apache Beam), and BigQuery. Implemented partitioned and clustered warehouse tables with secure IAM configuration and achieved near real-time ingestion and processing.