import boto3
import logging
import pandas as pd
import random
from botocore.exceptions import ClientError

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize S3 client
s3 = boto3.client('s3')

def generate_user_action():
    """Generate a random Rock Paper Scissors action"""
    actions = ['rock', 'paper', 'scissors']
    return random.choice(actions)

def create_parquet_file(file_path, data):
    """Create a parquet file from the data"""
    df = pd.DataFrame(data)
    df.to_parquet(file_path, index=False)

def lambda_handler(event, context):
    """Lambda function to generate user actions for Rock Paper Scissors"""
    try:
        num_records = event.get('num_records', 100)
        bucket_name = event.get('bucket_name')
        
        # Validate required parameters
        if not bucket_name:
            raise ValueError("bucket_name is required in the event")
        
        # Generate data
        data = {
            'user_id': [f'user_{i}' for i in range(num_records)],
            'action': [generate_user_action() for _ in range(num_records)]
        }

        # Create parquet file
        file_path = "/tmp/employees.parquet"
        create_parquet_file(file_path, data)

        # Upload to S3
        s3_key = 'Landing/employees.parquet'
        s3.upload_file(file_path, bucket_name, s3_key)

        logger.info(f"Parquet file uploaded to S3 at: s3://{bucket_name}/{s3_key}")
        
        return {
            'statusCode': 200,
            'body': f'Successfully uploaded {num_records} records to s3://{bucket_name}/{s3_key}'
        }
        
    except ClientError as e:
        logger.error(f"AWS error: {e}")
        return {
            'statusCode': 500,
            'body': f'AWS error: {str(e)}'
        }
    except Exception as e:
        logger.error(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }
