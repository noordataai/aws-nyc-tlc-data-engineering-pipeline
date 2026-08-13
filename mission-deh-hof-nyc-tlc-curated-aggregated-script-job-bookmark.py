import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import boto3

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

account_id = boto3.client('sts').get_caller_identity()['Account']
S3_BUCKET = f"mission-deh-hof-nyc-tlc-{account_id}"
ZONE_PATH = f"s3://{S3_BUCKET}/raw/zone/taxi_zone_lookup.csv"
AGGREGATED_PATH = f"s3://{S3_BUCKET}/aggregated/"

print(f"Account ID: {account_id}")
print(f"Processing curated HVFHV data with job bookmarks...")

# Run MSCK REPAIR to discover new partitions in curated table
print("Running MSCK REPAIR to discover new curated partitions...")
try:
    spark.sql("MSCK REPAIR TABLE `mission-deh-hof-nyc-tlc`.`curatedhvfhv`")
    print("MSCK REPAIR for curated table completed successfully!")
except Exception as e:
    print(f"MSCK REPAIR failed: {str(e)} - Continuing with existing partitions...")

# Load curated HVFHV data using Glue Catalog with job bookmarks
print("Loading curated HVFHV data with job bookmarks...")
curated_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="mission-deh-hof-nyc-tlc",
    table_name="curatedhvfhv",
    transformation_ctx="curated_hvfhv_source"
)

curated_df = curated_dynamic_frame.toDF()
print("Data loaded successfully with job bookmarks enabled!")

initial_count = curated_df.count()
print(f"Initial curated record count: {initial_count:,}")

if initial_count == 0:
    print("No new curated data to process (job bookmarks filtered all data)")
    job.commit()
    print("Curated to aggregated pipeline completed successfully!")
else:
    print("Loading zone lookup table...")
    zone_df = spark.read.option("header", "true").option("inferSchema", "true").csv(ZONE_PATH)
    print(f"Loaded {zone_df.count():,} zone records")

    print("Enriching data with zone information...")
    pickup_zones = zone_df.alias("pu_zone")
    dropoff_zones = zone_df.alias("do_zone")

    enriched_df = curated_df \
        .join(pickup_zones, col("pulocationid") == col("pu_zone.LocationID"), "left") \
        .join(dropoff_zones, col("dolocationid") == col("do_zone.LocationID"), "left") \
        .select(
            col("trip_date"),
            col("day_of_week"),
            col("is_weekend"),
            col("pulocationid").alias("PULocationID"),
            col("dolocationid").alias("DOLocationID"),
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

    print("Creating aggregation tables...")

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

    zone_agg = enriched_df.groupBy(
        "trip_date", "pulocationid", "dolocationid", "is_airport_trip"
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

    service_agg = enriched_df.groupBy(
        "trip_date", "day_of_week", "is_weekend", "shared_request_flag", "wav_request_flag"
    ).agg(
        count("*").alias("trip_count"),
        sum("total_amount").cast("decimal(12,2)").alias("total_revenue"),
        sum(when(col("shared_match_flag") == lit(True), lit(1)).otherwise(lit(0))).alias("shared_matched_count"),
        sum(when(col("wav_match_flag") == lit(True), lit(1)).otherwise(lit(0))).alias("wav_matched_count")
    ).withColumn("year", year("trip_date")) \
     .withColumn("month", month("trip_date"))

    print("Writing aggregated tables to S3...")

    borough_agg.write.mode("append").partitionBy("year", "month").parquet(f"{AGGREGATED_PATH}daily_borough/")
    zone_agg.write.mode("append").partitionBy("year", "month", "day").parquet(f"{AGGREGATED_PATH}daily_zone/")
    service_agg.write.mode("append").partitionBy("year", "month").parquet(f"{AGGREGATED_PATH}daily_service/")

    print(f"All aggregation tables successfully created!")
    print(f"Records processed: {initial_count:,}")
    print(f"Borough aggregation: {borough_agg.count():,} records")
    print(f"Zone aggregation: {zone_agg.count():,} records")
    print(f"Service aggregation: {service_agg.count():,} records")

job.commit()
