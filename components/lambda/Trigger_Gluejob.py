import boto3
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue = boto3.client('glue')
JOB_NAME = 'RockPaperScissorsGlueJob'

def lambda_handler(event, context):
    for record in event['Records']:
        body = json.loads(record['body'])
        # S3 event notification body
        s3_info = body['Records'][0]['s3']
        bucket = s3_info['bucket']['name']
        key = s3_info['object']['key']

        response = glue.start_job_run(
            JobName=JOB_NAME,
            Arguments={
                '--input_path': f's3://{bucket}/{key}',
                '--output_path': f's3://{bucket}/output/',
            }
        )
        logger.info(f"Started Glue job run: {response['JobRunId']} for s3://{bucket}/{key}")

    return {'statusCode': 200}
