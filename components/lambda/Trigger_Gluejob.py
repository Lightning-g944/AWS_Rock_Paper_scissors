# lambda to trigger glue job when a file is uploaded to the landing zone
import boto3
import logging
logger = logging.getLogger()

def lambda_handler(event, context):
    """Lambda function to trigger Glue job when a file is uploaded to the landing zone"""
    try:
        glue = boto3.client('glue')
        job_name = 'RPSGlueJob'

        response = glue.start_job_run(JobName=job_name)
        logger.info(f"Glue job triggered successfully: {response['JobRunId']}")

    except Exception as e:
        logger.error(f"Error triggering Glue job: {e}")