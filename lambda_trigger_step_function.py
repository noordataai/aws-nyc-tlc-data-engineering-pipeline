import json
import boto3
import os

sfn = boto3.client('stepfunctions')
sts = boto3.client('sts')

def lambda_handler(event, context):
    account_id = sts.get_caller_identity()['Account']
    region = os.environ['AWS_REGION']

    for record in event['Records']:
        key = record['s3']['object']['key']
        bucket = record['s3']['bucket']['name']

        if key.endswith('ingestion.done'):
            response = sfn.start_execution(
                stateMachineArn=f'arn:aws:states:{region}:{account_id}:stateMachine:mission-deh-hof-nyc-tlc-data-pipeline-event-driven-cft',
                input=json.dumps({'bucket': bucket, 'key': key})
            )
            return {'statusCode': 200, 'executionArn': response['executionArn']}

    return {'statusCode': 200, 'message': 'No ingestion.done file found, skipping.'}
