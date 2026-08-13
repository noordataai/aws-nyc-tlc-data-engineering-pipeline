import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Initialize Glue job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Get AWS account ID and set paths
account_id = boto3.client('sts').get_caller_identity()['Account']
S3_BUCKET = f"mission-deh-hof-nyc-tlc-{account_id}"
RAW_PATH = f"s3://{S3_BUCKET}/raw/hvfhv/"
CURATED_PATH = f"s3://{S3_BUCKET}/curated/hvfhv/"

print(f"Loading all data from: {RAW_PATH}")

# Load raw data
try:
    raw_df = spark.read.parquet(RAW_PATH)
    print("Data loaded successfully!")
    print(f"Total records: {raw_df.count():,}")
    print(f"Total columns: {len(raw_df.columns)}")
    raw_df.printSchema()
    raw_df.show(5, truncate=False)

    license_counts = raw_df.groupBy("hvfhs_license_num").count().orderBy(desc("count"))
    print("License Distribution:")
    license_counts.show()

except Exception as e:
    print(f"Error loading data: {str(e)}")
    raise


def transform_raw_to_curated(raw_df):
    """Transform raw HVFHV data to curated layer per mapping requirements"""
    print("Starting core transformations...")

    curated_df = raw_df.select(
        col("hvfhs_license_num"),
        col("dispatching_base_num"),
        col("originating_base_num"),
        col("request_datetime").cast("timestamp").alias("request_datetime"),
        col("on_scene_datetime").cast("timestamp").alias("on_scene_datetime"),
        col("pickup_datetime").cast("timestamp").alias("pickup_datetime"),
        col("dropoff_datetime").cast("timestamp").alias("dropoff_datetime"),
        col("PULocationID").cast("integer").alias("PULocationID"),
        col("DOLocationID").cast("integer").alias("DOLocationID"),
        coalesce(col("trip_miles"), lit(0.0)).cast("decimal(10,2)").alias("trip_miles"),
        col("trip_time").cast("integer").alias("trip_time_seconds"),
        coalesce(col("base_passenger_fare"), lit(0.0)).cast("decimal(10,2)").alias("base_passenger_fare"),
        coalesce(col("tolls"), lit(0.0)).cast("decimal(10,2)").alias("tolls"),
        coalesce(col("bcf"), lit(0.0)).cast("decimal(10,2)").alias("bcf"),
        coalesce(col("sales_tax"), lit(0.0)).cast("decimal(10,2)").alias("sales_tax"),
        coalesce(col("congestion_surcharge"), lit(0.0)).cast("decimal(10,2)").alias("congestion_surcharge"),
        coalesce(col("airport_fee"), lit(0.0)).cast("decimal(10,2)").alias("airport_fee"),
        coalesce(col("tips"), lit(0.0)).cast("decimal(10,2)").alias("tips"),
        coalesce(col("driver_pay"), lit(0.0)).cast("decimal(10,2)").alias("driver_pay"),
        when(col("shared_request_flag") == "Y", True).otherwise(False).alias("shared_request_flag"),
        when(col("shared_match_flag") == "Y", True).otherwise(False).alias("shared_match_flag"),
        when(col("access_a_ride_flag") == "Y", True).otherwise(False).alias("access_a_ride_flag"),
        when(col("wav_request_flag") == "Y", True).otherwise(False).alias("wav_request_flag"),
        when(col("wav_match_flag") == "Y", True).otherwise(False).alias("wav_match_flag"),
        lit(0.0).cast("decimal(10,2)").alias("cbd_congestion_fee"),
        col("pickup_datetime").cast("date").alias("trip_date"),
        hour("pickup_datetime").alias("pickup_hour"),
        when(dayofweek("pickup_datetime") == 1, "Sunday")
        .when(dayofweek("pickup_datetime") == 2, "Monday")
        .when(dayofweek("pickup_datetime") == 3, "Tuesday")
        .when(dayofweek("pickup_datetime") == 4, "Wednesday")
        .when(dayofweek("pickup_datetime") == 5, "Thursday")
        .when(dayofweek("pickup_datetime") == 6, "Friday")
        .when(dayofweek("pickup_datetime") == 7, "Saturday")
        .otherwise("Unknown").alias("day_of_week"),
        when(dayofweek("pickup_datetime").isin([1, 7]), True).otherwise(False).alias("is_weekend"),
        (col("trip_time") / 60.0).cast("decimal(10,2)").alias("trip_duration_minutes"),
        (coalesce(col("base_passenger_fare"), lit(0.0)) +
         coalesce(col("tolls"), lit(0.0)) +
         coalesce(col("bcf"), lit(0.0)) +
         coalesce(col("sales_tax"), lit(0.0)) +
         coalesce(col("congestion_surcharge"), lit(0.0)) +
         coalesce(col("airport_fee"), lit(0.0)) +
         lit(0.0)
         ).cast("decimal(10,2)").alias("total_fare"),
        (coalesce(col("base_passenger_fare"), lit(0.0)) +
         coalesce(col("tolls"), lit(0.0)) +
         coalesce(col("bcf"), lit(0.0)) +
         coalesce(col("sales_tax"), lit(0.0)) +
         coalesce(col("congestion_surcharge"), lit(0.0)) +
         coalesce(col("airport_fee"), lit(0.0)) +
         lit(0.0) +
         coalesce(col("tips"), lit(0.0))
         ).cast("decimal(10,2)").alias("total_amount"),
        when(col("trip_time") > 0,
             (col("trip_miles") / (col("trip_time") / 3600.0))
             ).otherwise(None).cast("decimal(10,2)").alias("avg_speed_mph"),
        when(coalesce(col("airport_fee"), lit(0.0)) > 0, True).otherwise(False).alias("is_airport_trip"),
        year("pickup_datetime").alias("year"),
        month("pickup_datetime").alias("month"),
        dayofmonth("pickup_datetime").alias("day"),
        current_timestamp().alias("processing_timestamp")
    )

    print("Core transformations completed!")
    return curated_df


curated_df = transform_raw_to_curated(raw_df)

print(f"Total records: {curated_df.count():,}")
print(f"Total columns: {len(curated_df.columns)}")
curated_df.printSchema()
curated_df.show(5, truncate=False)


def validate_curated_data(curated_df):
    print("Starting data quality validation...")
    initial_count = curated_df.count()

    validated_df = curated_df.filter(
        col("pickup_datetime").isNotNull() &
        col("dropoff_datetime").isNotNull() &
        col("PULocationID").isNotNull() &
        col("DOLocationID").isNotNull() &
        (col("trip_miles") >= 0) & (col("trip_miles") <= 500) &
        (col("base_passenger_fare") >= 0) & (col("base_passenger_fare") <= 10000) &
        (col("dropoff_datetime") > col("pickup_datetime")) &
        (col("pickup_datetime") >= col("request_datetime")) &
        col("hvfhs_license_num").isin(['HV0002', 'HV0003', 'HV0004', 'HV0005'])
    )

    validated_count = validated_df.count()
    rejected_count = initial_count - validated_count
    print(f"Valid records: {validated_count:,} | Rejected: {rejected_count:,} ({(rejected_count/initial_count)*100:.2f}%)")
    return validated_df, validated_count


validated_df, final_count = validate_curated_data(curated_df)
validated_df.show(5, truncate=False)

print(f"Writing validated data to: {CURATED_PATH}")
validated_df.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet(CURATED_PATH)

print(f"Data successfully written to S3 curated layer! Final count: {final_count:,} records")

job.commit()
