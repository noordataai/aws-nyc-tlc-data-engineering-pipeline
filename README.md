# AWS NYC TLC Data Engineering Pipeline

![AWS](https://img.shields.io/badge/AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white)
![Python](https://img.shields.io/badge/python-3.11-blue.svg?style=for-the-badge&logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C.svg?style=for-the-badge&logo=apachespark&logoColor=white)
![AWS Glue](https://img.shields.io/badge/AWS%20Glue-FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white)
![Step Functions](https://img.shields.io/badge/Step%20Functions-FF4F8B.svg?style=for-the-badge&logo=amazon-aws&logoColor=white)
![CloudFormation](https://img.shields.io/badge/CloudFormation-FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white)
![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge)

---

An end-to-end **serverless data engineering pipeline** on AWS that ingests, transforms, and aggregates NYC Taxi & Limousine Commission (TLC) HVFHV (High Volume For-Hire Vehicle) trip data — covering Uber, Lyft, Via, and Juno rides across New York City.

The pipeline is fully **event-driven**, **incremental**, and **production-ready** with retry logic, SNS failure notifications, and job bookmark support for efficient incremental processing.

---

## Table of Contents

- [Architecture](#architecture)
- [Pipeline Overview](#pipeline-overview)
- [Repository Structure](#repository-structure)
- [Data Schema](#data-schema)
- [Aggregation Tables](#aggregation-tables)
- [Prerequisites](#prerequisites)
- [Deployment](#deployment)
- [AWS Resources](#aws-resources)
- [Author](#author)

---

## Architecture

```
NYC TLC CloudFront (Public Dataset)
            │
            ▼
┌───────────────────────┐
│   AWS Glue            │  Step 1 — Data Ingestion
│   Python Shell Job    │  • Downloads HVFHV parquet files
│                       │  • Supports historical & current modes
│                       │  • Partitions by year/month in S3
│                       │  • Creates ingestion.done trigger file
└──────────┬────────────┘
           │
           │  ingestion.done lands in S3 raw/
           ▼
┌───────────────────────┐
│   S3 Event            │  Event-Driven Trigger
│   Notification        │──────► AWS Lambda
└───────────────────────┘            │
                                     ▼
                          ┌─────────────────────┐
                          │  AWS Step Functions  │
                          │  State Machine       │
                          └──────────┬──────────┘
                                     │
                    ┌────────────────┴────────────────┐
                    ▼                                 ▼
        ┌───────────────────┐             ┌───────────────────┐
        │  AWS Glue         │  Step 2     │  AWS Glue         │  Step 3
        │  PySpark Job      │ ──────────► │  PySpark Job      │
        │  Raw → Curated    │             │  Curated → Agg    │
        └───────────────────┘             └───────────────────┘
                                                     │
                                                     ▼
                                          ┌─────────────────────┐
                                          │  Amazon SNS         │
                                          │  Success / Failure  │
                                          │  Notification       │
                                          └─────────────────────┘
```

**S3 Data Lake Layout:**
```
s3://mission-deh-hof-nyc-tlc-{account_id}/
├── raw/
│   ├── hvfhv/
│   │   └── year={yyyy}/month={mm}/fhvhv_tripdata_{yyyy}-{mm}.parquet
│   ├── zone/
│   │   └── taxi_zone_lookup.csv
│   └── ingestion.done
├── curated/
│   └── hvfhv/
│       └── year={yyyy}/month={mm}/day={dd}/
└── aggregated/
    ├── daily_borough/year={yyyy}/month={mm}/
    ├── daily_zone/year={yyyy}/month={mm}/day={dd}/
    └── daily_service/year={yyyy}/month={mm}/
```

---

## Pipeline Overview

### Step 1 — Data Ingestion (Glue Python Shell)
- Downloads HVFHV parquet files from NYC TLC CloudFront
- Two modes:
  - `historical` — downloads full 2024 year + all available months of current year (except latest)
  - `current` — downloads only the latest available month (for scheduled monthly runs)
- Automatically detects the latest available month via HTTP HEAD requests
- Skips files already in S3 (idempotent)
- Downloads taxi zone lookup CSV reference data
- Creates `raw/ingestion.done` JSON file to trigger the event-driven pipeline

### Step 2 — Raw to Curated (Glue PySpark)
- Reads raw parquet data from S3
- Applies type casting, null handling, and boolean flag conversions
- Derives new fields: `trip_date`, `pickup_hour`, `day_of_week`, `is_weekend`, `trip_duration_minutes`, `total_fare`, `total_amount`, `avg_speed_mph`, `is_airport_trip`
- Validates data quality (filters invalid records)
- Writes partitioned parquet to curated layer
- **Job bookmark version** available for incremental processing

### Step 3 — Curated to Aggregated (Glue PySpark)
- Reads curated data and joins with taxi zone lookup
- Produces 3 denormalized aggregation tables
- Snappy compressed parquet output
- Supports optional `target_year` / `target_month` parameters for targeted reprocessing
- **Job bookmark version** available for incremental processing

---

## Repository Structure

```
aws-nyc-tlc-data-engineering-pipeline/
├── glue-jobs/
│   ├── 01-ingestion/
│   │   └── glue_download_nyc_tlc_data_Step_1.py     # Glue Python Shell ingestion job
│   ├── 02-raw-to-curated/
│   │   ├── mission-deh-hof-nyc-tlc-raw-curated-script.py               # Standard version
│   │   └── mission-deh-hof-nyc-tlc-raw-curated-script-job-bookmark.py  # Incremental version
│   └── 03-curated-to-aggregated/
│       ├── mission-deh-hof-nyc-tlc-curated-aggregated-script.py               # Standard version
│       └── mission-deh-hof-nyc-tlc-curated-aggregated-script-job-bookmark.py  # Incremental version
├── step-functions/
│   └── mission-deh-hof-nyc-tlc-step-function-event-driven.json  # ASL definition
├── lambda/
│   └── lambda_trigger_step_function.py   # S3 event trigger for Step Functions
├── infrastructure/
│   ├── hackathon-create-s3-bucket.sh                    # S3 bucket creation script
│   └── mission-deh-hof-event-driven-pipeline-cft.yaml  # CloudFormation template
├── .gitignore
├── architecture.md
├── LICENSE
└── README.md
```

---

## Data Schema

### Raw Layer
Original HVFHV fields from NYC TLC:

| Column | Type | Description |
|---|---|---|
| `hvfhs_license_num` | string | License number (HV0002-HV0005) |
| `dispatching_base_num` | string | Dispatching base number |
| `originating_base_num` | string | Originating base number |
| `request_datetime` | timestamp | When ride was requested |
| `pickup_datetime` | timestamp | Actual pickup time |
| `dropoff_datetime` | timestamp | Actual dropoff time |
| `PULocationID` | integer | Pickup taxi zone ID |
| `DOLocationID` | integer | Dropoff taxi zone ID |
| `trip_miles` | decimal | Trip distance in miles |
| `trip_time` | integer | Trip duration in seconds |
| `base_passenger_fare` | decimal | Base fare amount |
| `tolls` | decimal | Toll charges |
| `bcf` | decimal | Black car fund fee |
| `sales_tax` | decimal | Sales tax |
| `congestion_surcharge` | decimal | Congestion surcharge |
| `airport_fee` | decimal | Airport fee |
| `tips` | decimal | Tip amount |
| `driver_pay` | decimal | Driver pay amount |
| `shared_request_flag` | string | Y/N shared ride requested |
| `wav_request_flag` | string | Y/N wheelchair accessible requested |

### Curated Layer
Transformed and enriched fields:

| Column | Type | Description |
|---|---|---|
| All raw fields | — | Retained with type corrections |
| `trip_time_seconds` | integer | Renamed from `trip_time` |
| `shared_request_flag` | boolean | Converted from Y/N |
| `wav_request_flag` | boolean | Converted from Y/N |
| `cbd_congestion_fee` | decimal | New field from Jan 2025 (default 0.0) |
| `trip_date` | date | Derived from `pickup_datetime` |
| `pickup_hour` | integer | Hour of pickup (0-23) |
| `day_of_week` | string | Monday–Sunday |
| `is_weekend` | boolean | True for Saturday/Sunday |
| `trip_duration_minutes` | decimal | `trip_time / 60` |
| `total_fare` | decimal | Sum of all charges excluding tips |
| `total_amount` | decimal | `total_fare + tips` |
| `avg_speed_mph` | decimal | `trip_miles / (trip_time / 3600)` |
| `is_airport_trip` | boolean | True if `airport_fee > 0` |
| `processing_timestamp` | timestamp | When record was processed |
| `year` / `month` / `day` | integer | Partition columns |

---

## Aggregation Tables

### Table 1 — `daily_borough`
Grain: one row per **day × pickup borough × dropoff borough × weekend flag**

| Column | Type |
|---|---|
| `trip_date` | date |
| `day_of_week` | string |
| `is_weekend` | boolean |
| `pickup_borough` | string |
| `dropoff_borough` | string |
| `trip_count` | long |
| `total_revenue` | decimal |
| `total_distance` | decimal |
| `total_duration` | long |
| `avg_borough_distance` | decimal |
| `avg_borough_duration` | decimal |

### Table 2 — `daily_zone`
Grain: one row per **day × pickup zone × dropoff zone × airport flag**

| Column | Type |
|---|---|
| `trip_date` | date |
| `PULocationID` | integer |
| `DOLocationID` | integer |
| `is_airport_trip` | boolean |
| `pickup_zone` / `pickup_borough` | string |
| `dropoff_zone` / `dropoff_borough` | string |
| `trip_count` | long |
| `total_revenue` | decimal |
| `avg_zone_distance` | decimal |
| `avg_fare_per_trip` | decimal |
| `total_driver_pay` | decimal |

### Table 3 — `daily_service`
Grain: one row per **day × shared flag × WAV flag × weekend flag**

| Column | Type |
|---|---|
| `trip_date` | date |
| `day_of_week` | string |
| `is_weekend` | boolean |
| `shared_request_flag` | boolean |
| `wav_request_flag` | boolean |
| `trip_count` | long |
| `total_revenue` | decimal |
| `shared_matched_count` | long |
| `wav_matched_count` | long |

---

## Prerequisites

- AWS Account with permissions for: S3, Glue, Step Functions, Lambda, SNS, IAM, CloudFormation
- AWS CLI configured (`aws configure`)
- Python 3.11+

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

### 4. Configure S3 Event Notification
After stack deployment, configure the S3 trigger manually:
1. Go to S3 bucket → **Properties** → **Event notifications** → **Create event notification**
2. Event name: `mission-deh-hof-trigger-pipeline`
3. Event types: **All object create events**
4. Prefix: `raw/` — Suffix: `ingestion.done`
5. Destination: **Lambda function** → `mission-deh-hof-trigger-step-function-cft`

### 5. Run Historical Ingestion
```bash
aws glue start-job-run \
  --job-name mission-deh-hof-nyc-tlc-ingestion \
  --arguments '{"--jobtype": "historical"}' \
  --region us-east-1
```

### 6. Subsequent Monthly Runs
Schedule the ingestion job with `--jobtype current` via EventBridge Scheduler to run monthly and pick up only the latest available month automatically.

---

## AWS Resources

All resources follow the naming prefix: `mission-deh-hof-`

| Resource | Name |
|---|---|
| S3 Data Bucket | `mission-deh-hof-nyc-tlc-{account_id}` |
| S3 Athena Bucket | `mission-deh-hof-nyc-tlc-athena-{account_id}` |
| Glue Database | `mission-deh-hof-nyc-tlc` |
| Glue Job — Ingestion | `mission-deh-hof-nyc-tlc-ingestion` |
| Glue Job — Raw to Curated | `mission-deh-hof-nyc-tlc-raw-curated-script` |
| Glue Job — Curated to Agg | `mission-deh-hof-nyc-tlc-curated-aggregated-script` |
| Step Functions | `mission-deh-hof-nyc-tlc-data-pipeline-event-driven-cft` |
| Lambda | `mission-deh-hof-trigger-step-function-cft` |
| SNS Topic | `mission-deh-hof-cft` |
| CloudFormation Stack | `mission-deh-hof-nyc-tlc-pipeline` |

---

## Author

**Noor** — [github.com/noordataai](https://github.com/noordataai)

---

*Built on AWS — Glue · Step Functions · Lambda · S3 · SNS · CloudFormation*
