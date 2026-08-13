#!/bin/bash

# NYC TLC Data Pipeline - S3 Bucket and IAM Setup
# Creates S3 buckets, IAM roles for Glue, Step Functions, EventBridge Scheduler, and Lambda

set -e

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION="us-east-1"
DATA_BUCKET="mission-deh-hof-nyc-tlc-${ACCOUNT_ID}"
ATHENA_BUCKET="mission-deh-hof-nyc-tlc-athena-${ACCOUNT_ID}"

echo "Account ID: ${ACCOUNT_ID}"
echo "Region: ${REGION}"
echo "Data Bucket: ${DATA_BUCKET}"
echo "Athena Bucket: ${ATHENA_BUCKET}"

# Create S3 data bucket
echo "Creating S3 data bucket..."
if [ "${REGION}" == "us-east-1" ]; then
    aws s3api create-bucket \
        --bucket "${DATA_BUCKET}" \
        --region "${REGION}"
else
    aws s3api create-bucket \
        --bucket "${DATA_BUCKET}" \
        --region "${REGION}" \
        --create-bucket-configuration LocationConstraint="${REGION}"
fi

# Block all public access on data bucket
aws s3api put-public-access-block \
    --bucket "${DATA_BUCKET}" \
    --public-access-block-configuration \
    "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

# Enable versioning on data bucket
aws s3api put-bucket-versioning \
    --bucket "${DATA_BUCKET}" \
    --versioning-configuration Status=Enabled

# Create Athena results bucket
echo "Creating Athena results bucket..."
if [ "${REGION}" == "us-east-1" ]; then
    aws s3api create-bucket \
        --bucket "${ATHENA_BUCKET}" \
        --region "${REGION}"
else
    aws s3api create-bucket \
        --bucket "${ATHENA_BUCKET}" \
        --region "${REGION}" \
        --create-bucket-configuration LocationConstraint="${REGION}"
fi

# Block all public access on Athena bucket
aws s3api put-public-access-block \
    --bucket "${ATHENA_BUCKET}" \
    --public-access-block-configuration \
    "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

echo "S3 buckets created successfully!"
echo "Data bucket: s3://${DATA_BUCKET}/"
echo "Athena bucket: s3://${ATHENA_BUCKET}/"
