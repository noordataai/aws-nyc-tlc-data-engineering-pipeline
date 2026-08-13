# Architecture Notes

## Pipeline Design

### Two Pipeline Modes

**Sequential Pipeline (manual / scheduled)**
- Starts at DataIngestion Glue job
- 8 states: DataIngestion → RawToCurated → CuratedToAggregated → Success/Failure notifications
- Suitable for scheduled runs via EventBridge Scheduler

**Event-Driven Pipeline (production)**
- Triggered automatically when `raw/ingestion.done` lands in S3
- Flow: S3 Event → Lambda → Step Functions (starts at RawToCurated)
- 6 states: RawToCurated → CuratedToAggregated → Success/Failure notifications
- Decouples ingestion from transformation — ingestion job can run independently

---

## Data Flow

### Raw Layer
- Source: NYC TLC CloudFront (`https://d37ci6vzurychx.cloudfront.net/trip-data/`)
- Dataset: HVFHV (High Volume For-Hire Vehicle) — covers Uber, Lyft, Via, Juno
- Format: Parquet, partitioned by `year=/month=`
- Idempotent: skips files already in S3

### Curated Layer
Key transformations applied:
- Y/N string flags → boolean
- `trip_time` (seconds) → `trip_time_seconds` + `trip_duration_minutes`
- Derived: `trip_date`, `pickup_hour`, `day_of_week`, `is_weekend`
- Derived: `total_fare` (excl. tips), `total_amount` (incl. tips)
- Derived: `avg_speed_mph` (null-safe division)
- Derived: `is_airport_trip` (airport_fee > 0)
- Added: `cbd_congestion_fee` (new field from Jan 5, 2025, defaulted to 0.0)
- Validation filters: null timestamps, invalid location IDs, out-of-range trip_miles/fare, dropoff before pickup, invalid license numbers

### Aggregated Layer
Three denormalized aggregation tables joined with `taxi_zone_lookup.csv`:

| Table | Grain | Partition |
|---|---|---|
| `daily_borough` | day × pickup_borough × dropoff_borough × is_weekend | year/month |
| `daily_zone` | day × PULocationID × DOLocationID × is_airport_trip | year/month/day |
| `daily_service` | day × shared_request_flag × wav_request_flag × is_weekend | year/month |

---

## Job Bookmark Strategy

Two versions of each PySpark job exist:
- **Standard version** — reads directly from S3 path, supports full reprocessing
- **Job bookmark version** — reads from Glue Data Catalog with `transformation_ctx`, enables incremental processing

The job bookmark version requires:
1. Glue Crawler to have run and created catalog tables
2. `MSCK REPAIR TABLE` to discover new partitions before each run

---

## Error Handling

- Each Glue job state has 2 retries with 30s interval and 2x exponential backoff
- On final failure, pipeline routes to SNS failure notification then `Fail` state
- Lambda is idempotent — multiple S3 events for the same file will each trigger a new execution (Step Functions handles deduplication via execution names if needed)

---

## Security Notes

- No hardcoded account IDs anywhere — all resolved dynamically via `boto3 STS get_caller_identity()`
- S3 bucket names include account ID suffix to ensure global uniqueness
- IAM roles follow least-privilege: Glue role has only S3 + Glue permissions, Lambda role has only Step Functions start + S3 get
- CloudFormation template uses `${AWS::AccountId}` for all ARN references
