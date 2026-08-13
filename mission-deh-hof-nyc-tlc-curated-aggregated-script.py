import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# Get job parameters (optional: target_year and target_month)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

if '--target_year' in sys.argv and '--target_month' in sys.argv:
    optional_args = getResolvedOptions(sys.argv, ['target_year', 'target_month'])
    TARGET_YEAR = optional_args['target_year']
    TARGET_MONTH = optional_args['target_month']
    PROCESS_ALL = False
else:
    TARGET_YEAR = None
    TARGET_MONTH = None
    PROCESS_ALL = True

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger = glueContext.get_logger()
logger.info("Starting Curated to Aggregated ETL Job")

account_id = boto3.client('sts').get_caller_identity()['Account']
S3_BUCKET = f"mission-deh-hof-nyc-tlc-{account_id}"
CURATED_PATH = f"s3://{S3_BUCKET}/curated/hvfhv/"
ZONE_PATH = f"s3://{S3_BUCKET}/raw/zone/taxi_zone_lookup.csv"
AGGREGATED_PATH = f"s3://{S3_BUCKET}/aggregated/"

logger.info(f"Account ID: {account_id}")
if PROCESS_ALL:
    logger.info(f"Processing ALL data from curated folder: {CURATED_PATH}")
else:
    logger.info(f"Processing specific partition: {TARGET_YEAR}-{TARGET_MONTH}")

try:
    if PROCESS_ALL:
        logger.info("Loading ALL curated HVFHV data...")
        curated_df = spark.read.parquet(CURATED_PATH)
    else:
        logger.info(f"Loading curated HVFHV data for {TARGET_YEAR}-{TARGET_MONTH}...")
        specific_path = f"{CURATED_PATH}year={TARGET_YEAR}/month={TARGET_MONTH}/"
        curated_df = spark.read.parquet(specific_path)

    curated_count = curated_df.count()
    logger.info(f"Loaded {curated_count:,} curated records")

    if curated_count == 0:
        raise ValueError("No data found in curated path")
except Exception as e:
    logger.error(f"Failed to load curated data: {str(e)}")
    raise

try:
    logger.info("Loading zone lookup table...")
    zone_df = spark.read.option("header", "true").option("inferSchema", "true").csv(ZONE_PATH)
    zone_count = zone_df.count()
    logger.info(f"Loaded {zone_count:,} zone records")

    if zone_count == 0:
        raise ValueError(f"No zone data found in {ZONE_PATH}")
except Exception as e:
    logger.error(f"Failed to load zone data: {str(e)}")
    raise

logger.info("Enriching data with zone information...")

pickup_zones = zone_df.alias("pu_zone")
dropoff_zones = zone_df.alias("do_zone")

enriched_df = curated_df \
    .join(pickup_zones, col("PULocationID") == col("pu_zone.LocationID"), "left") \
    .join(dropoff_zones, col("DOLocationID") == col("do_zone.LocationID"), "left") \
    .select(
        col("trip_date"),
        col("day_of_week"),
        col("is_weekend"),
        col("PULocationID"),
        col("DOLocationID"),
        col("shared_request_flag"),
        col("shared_match_flag"),
        col("wav_request_flag"),
        col("wav_match_flag"),
        col("is_airport_trip"),
        col("total_amount"),
        col("trip_miles"),
        col("trip_duration_minutes"),
        col("driver_pay"),
        col("pu_zone.Borough").alias("pickup_borough"),
        col("pu_zone.Zone").alias("pickup_zone"),
        col("pu_zone.service_zone").alias("pickup_service_zone"),
        col("do_zone.Borough").alias("dropoff_borough"),
        col("do_zone.Zone").alias("dropoff_zone"),
        col("do_zone.service_zone").alias("dropoff_service_zone")
    )

enriched_count = enriched_df.count()
logger.info(f"Enriched dataset created with {enriched_count:,} records")

logger.info("Creating Table 1: aggregated_trips_daily_borough")
borough_agg = enriched_df.groupBy(
    "trip_date", "day_of_week", "is_weekend", "pickup_borough", "dropoff_borough"
).agg(
    count("*").alias("trip_count"),
    sum("total_amount").cast("decimal(12,2)").alias("total_revenue"),
    sum("trip_miles").cast("decimal(12,2)").alias("total_distance"),
    sum("trip_duration_minutes").cast("bigint").alias("total_duration"),
    avg("trip_miles").cast("decimal(10,2)").alias("avg_borough_distance"),
    avg("trip_duration_minutes").cast("decimal(10,2)").alias("avg_borough_duration")
).withColumn("year", year("trip_date")) \
 .withColumn("month", month("trip_date"))

borough_count = borough_agg.count()
logger.info(f"Borough aggregation created with {borough_count:,} records")

logger.info("Creating Table 2: aggregated_trips_daily_zone")
zone_agg = enriched_df.groupBy(
    "trip_date", "PULocationID", "DOLocationID", "is_airport_trip"
).agg(
    count("*").alias("trip_count"),
    sum("total_amount").cast("decimal(12,2)").alias("total_revenue"),
    avg("trip_miles").cast("decimal(10,2)").alias("avg_zone_distance"),
    avg("trip_duration_minutes").cast("decimal(10,2)").alias("avg_zone_duration"),
    avg("total_amount").cast("decimal(10,2)").alias("avg_fare_per_trip"),
    sum("driver_pay").cast("decimal(12,2)").alias("total_driver_pay"),
    max("pickup_zone").alias("pickup_zone"),
    max("pickup_borough").alias("pickup_borough"),
    max("pickup_service_zone").alias("pickup_service_zone"),
    max("dropoff_zone").alias("dropoff_zone"),
    max("dropoff_borough").alias("dropoff_borough"),
    max("dropoff_service_zone").alias("dropoff_service_zone")
).withColumn("year", year("trip_date")) \
 .withColumn("month", month("trip_date")) \
 .withColumn("day", dayofmonth("trip_date"))

zone_agg_count = zone_agg.count()
logger.info(f"Zone aggregation created with {zone_agg_count:,} records")

logger.info("Creating Table 3: aggregated_trips_daily_service")
service_agg = enriched_df.groupBy(
    "trip_date", "day_of_week", "is_weekend", "shared_request_flag", "wav_request_flag"
).agg(
    count("*").alias("trip_count"),
    sum("total_amount").cast("decimal(12,2)").alias("total_revenue"),
    sum(when(col("shared_match_flag") == lit(True), lit(1)).otherwise(lit(0))).alias("shared_matched_count"),
    sum(when(col("wav_match_flag") == lit(True), lit(1)).otherwise(lit(0))).alias("wav_matched_count")
).withColumn("year", year("trip_date")) \
 .withColumn("month", month("trip_date"))

service_count = service_agg.count()
logger.info(f"Service aggregation created with {service_count:,} records")

logger.info("Writing aggregated tables to S3...")

try:
    logger.info("Writing aggregated_trips_daily_borough...")
    borough_agg.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .option("compression", "snappy") \
        .parquet(f"{AGGREGATED_PATH}daily_borough/")
    logger.info("Borough aggregation saved successfully")

    logger.info("Writing aggregated_trips_daily_zone...")
    zone_agg.write \
        .mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .option("compression", "snappy") \
        .parquet(f"{AGGREGATED_PATH}daily_zone/")
    logger.info("Zone aggregation saved successfully")

    logger.info("Writing aggregated_trips_daily_service...")
    service_agg.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .option("compression", "snappy") \
        .parquet(f"{AGGREGATED_PATH}daily_service/")
    logger.info("Service aggregation saved successfully")

except Exception as e:
    logger.error(f"Failed to write aggregated data: {str(e)}")
    raise

logger.info("=" * 50)
if PROCESS_ALL:
    logger.info("AGGREGATION PIPELINE SUMMARY - ALL DATA")
else:
    logger.info(f"AGGREGATION PIPELINE SUMMARY - {TARGET_YEAR}-{TARGET_MONTH}")
logger.info("=" * 50)
logger.info(f"Original curated records: {curated_count:,}")
logger.info(f"Borough aggregation: {borough_count:,} records ({(borough_count/curated_count)*100:.4f}% of original)")
logger.info(f"Zone aggregation: {zone_agg_count:,} records ({(zone_agg_count/curated_count)*100:.2f}% of original)")
logger.info(f"Service aggregation: {service_count:,} records ({(service_count/curated_count)*100:.4f}% of original)")
logger.info(f"Output location: {AGGREGATED_PATH}")
logger.info("Curated to Aggregated pipeline completed successfully")

job.commit()
