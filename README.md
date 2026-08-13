# AWS NYC TLC Data Engineering Pipeline

An end-to-end serverless data engineering pipeline on AWS that ingests, transforms, and aggregates NYC Taxi & Limousine Commission (TLC) HVFHV trip data using AWS Glue, Step Functions, Lambda, S3, and SNS.

---

## Architecture Overview

```
NYC TLC CloudFront
        │
        ▼
┌─────────────────┐
│  Glue Python    │  Step 1 — Ingestion
│  Shell Job      │  Downloads HVFHV parquet files to S3 raw layer
│  (Step 1)       │  Creates ingestion.done trigger file
└────────┬────────┘
         │ ingestion.done lands in S3
         ▼
┌─────────────────┐
│  S3 Event       │  Event-Driven Trigger
│  Notification   │──► Lambda ──► Step Functions
└─────────────────┘
         │
         ▼
┌─────────────────┐
│  Glue PySpark   │  Step 2 — Raw → Curated
│  Job            │  Transforms, validates, enriches raw data
│  (Step 2)       │  Partitioned by year/month/day
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Glue PySpark   │  Step 3 — Curated → Aggregated
│  Job            │  3 aggregation tables: borough, zone, service
│  (Step 3)       │  Snappy compressed parquet
└────────┬────────┘
         │
         ▼
    SNS Notification (success or failure)
```

**S3 Data Lake Layout:**
```
s3://mission-deh-hof-nyc-tlc-{account_id}/
├── raw/
│   ├── hvfhv/year={yyyy}/month={mm}/   ← HVFHV parquet files
│   ├── zone/taxi_zone_lookup.csv        ← Zone reference data
│   └── ingestion.done                  ← Pipeline trigger file
├── curated/
│   └── hvfhv/year={yyyy}/month={mm}/day={dd}/
└── aggregated/
    ├── daily_borough/year={yyyy}/month={mm}/
    ├── daily_zone/year={yyyy}/month={mm}/day={dd}/
    └── daily_service/year={yyyy}/month={mm}/
```

---

## Repository Structure

```
aws-nyc-tlc-data-engineering-pipeline/
├── glue-jobs/
│   ├── 01-ingestion/
│   │   └── glue_download_nyc_tlc_data_Step_1.py
│   ├── 02-raw-to-curated/
│   │   ├── mission-deh-hof-nyc-tlc-raw-curated-script.py
│   │   └── mission-deh-hof-nyc-tlc-raw-curated-script-job-bookmark.py
│   └── 03-curated-to-aggregated/
│       ├── mission-deh-hof-nyc-tlc-curated-aggregated-script.py
│       └── mission-deh-hof-nyc-tlc-curated-aggregated-script-job-bookmark.py
├── step-functions/
│   └── mission-deh-hof-nyc-tlc-step-function-event-driven.json
├── lambda/
│   └── lambda_trigger_step_function.py
├── infrastructure/
│   ├── hackathon-create-s3-bucket.sh
│   └── mission-deh-hof-event-driven-pipeline-cft.yaml
├── .gitignore
├── README.md
└── architecture.md
```

---

## Glue Jobs

### Step 1 — Ingestion (`glue-jobs/01-ingestion/`)
- **Type:** Glue Python Shell
- **Modes:** `historical` (2024 full year + partial current year) or `current` (latest month only)
- **Output:** Partitioned parquet files in `s3://.../raw/hvfhv/year=.../month=.../`
- **Trigger:** Creates `raw/ingestion.done` to kick off the event-driven pipeline

### Step 2 — Raw to Curated (`glue-jobs/02-raw-to-curated/`)
- **Type:** Glue PySpark
- **Transformations:** Type casting, null handling, boolean flag conversion, derived fields (trip_date, day_of_week, is_weekend, trip_duration_minutes, total_fare, total_amount, avg_speed_mph, is_airport_trip)
- **Validation:** Filters invalid records (null timestamps, out-of-range values, invalid license numbers)
- **Two versions:** Standard (overwrite) and job-bookmark (incremental via Glue Catalog)

### Step 3 — Curated to Aggregated (`glue-jobs/03-curated-to-aggregated/`)
- **Type:** Glue PySpark
- **Output:** 3 aggregation tables joined with zone lookup data:
  - `daily_borough` — trip counts and revenue by pickup/dropoff borough
  - `daily_zone` — trip counts and revenue by pickup/dropoff zone ID
  - `daily_service` — trip counts by shared ride and WAV flags
- **Two versions:** Standard (supports optional `target_year`/`target_month` params) and job-bookmark

---

## Step Functions

### Event-Driven Pipeline (`step-functions/`)
- **Trigger:** Lambda invoked by S3 event when `ingestion.done` lands
- **States:** RawToCurated → CuratedToAggregated → PipelineSuccess (or failure notifications)
- **Retry:** 2 retries with 30s interval and 2x backoff on each Glue job
- **Notifications:** SNS alerts on both success and failure

---

## Lambda

### `lambda_trigger_step_function.py`
- Triggered by S3 PUT event on `raw/ingestion.done`
- Dynamically resolves account ID via STS (no hardcoded ARNs)
- Starts the event-driven Step Functions state machine

---

## Infrastructure

### `hackathon-create-s3-bucket.sh`
Shell script to create the S3 data bucket and Athena results bucket.

### `mission-deh-hof-event-driven-pipeline-cft.yaml`
CloudFormation template that provisions:
- SNS Topic with email subscription
- IAM Role for Step Functions (Glue + SNS permissions)
- Step Functions State Machine (event-driven pipeline)
- IAM Role for Lambda (Step Functions + S3 permissions)
- Lambda Function
- Lambda Permission for S3 invocation

> **Note:** S3 Event Notification must be configured manually after stack deployment. See the `ManualStepRequired` output in the CloudFormation stack for instructions.

---

## Deployment

### 1. Create S3 Buckets
```bash
chmod +x infrastructure/hackathon-create-s3-bucket.sh
./infrastructure/hackathon-create-s3-bucket.sh
```

### 2. Deploy CloudFormation Stack
```bash
aws cloudformation deploy \
  --template-file infrastructure/mission-deh-hof-event-driven-pipeline-cft.yaml \
  --stack-name mission-deh-hof-nyc-tlc-pipeline \
  --parameter-overrides EmailAddress=your@email.com \
  --capabilities CAPABILITY_NAMED_IAM \
  --region us-east-1
```

### 3. Upload Glue Scripts to S3
```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
BUCKET="mission-deh-hof-nyc-tlc-${ACCOUNT_ID}"

aws s3 cp glue-jobs/01-ingestion/glue_download_nyc_tlc_data_Step_1.py s3://${BUCKET}/scripts/
aws s3 cp glue-jobs/02-raw-to-curated/mission-deh-hof-nyc-tlc-raw-curated-script-job-bookmark.py s3://${BUCKET}/scripts/
aws s3 cp glue-jobs/03-curated-to-aggregated/mission-deh-hof-nyc-tlc-curated-aggregated-script-job-bookmark.py s3://${BUCKET}/scripts/
```

### 4. Run Historical Ingestion
```bash
aws glue start-job-run \
  --job-name mission-deh-hof-nyc-tlc-ingestion \
  --arguments '{"--jobtype": "historical"}' \
  --region us-east-1
```

---

## AWS Resources Naming Convention

All resources follow the prefix: `mission-deh-hof-`

| Resource | Name |
|---|---|
| S3 Data Bucket | `mission-deh-hof-nyc-tlc-{account_id}` |
| Glue Database | `mission-deh-hof-nyc-tlc` |
| Step Function | `mission-deh-hof-nyc-tlc-data-pipeline-event-driven` |
| Lambda | `mission-deh-hof-trigger-step-function` |
| SNS Topic | `mission-deh-hof` |

---

## Author

**Noor** — [github.com/noordataai](https://github.com/noordataai)
