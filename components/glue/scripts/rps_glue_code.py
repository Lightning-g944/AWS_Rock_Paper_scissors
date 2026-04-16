import json
import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
import random
import logging
from awsglue.utils import getResolvedOptions
import sys

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)

# AWS clients
s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", 
     "input_path", 
     "output_path", 
     "sqs_arn",
     ]
)

def read_sqs_message(sqs_arn):
    """Read SQS message to extract bucket and key"""
    queue_url = sqs_arn.replace('arn:aws:sqs:', 'https://').replace(':', '/').rsplit('/', 1)[0]
    response = sqs_client.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)
    
    if 'Messages' in response:
        message = json.loads(response['Messages'][0]['Body'])
        return message.get('bucket'), message.get('key')
    return None, None

def system_turn():
    """Generate system's Rock, Paper, Scissors move"""
    moves = ['rock', 'paper', 'scissors']
    return random.choice(moves)

def score(player_move, system_move):
    """Compare moves and return result"""
    if player_move == system_move:
        return 'tie'
    elif (player_move == 'rock' and system_move == 'scissors') or \
         (player_move == 'paper' and system_move == 'rock') or \
         (player_move == 'scissors' and system_move == 'paper'):
        return 'win'
    else:
        return 'loss'

# Main execution
bucket, key = read_sqs_message(args['sqs_arn'])

if bucket and key:
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    player_data = json.loads(obj['Body'].read())
    
    system_move = system_turn()
    result = score(player_data['move'], system_move)
    
    output_data = {
        'player_move': player_data['move'],
        'system_move': system_move,
        'result': result
    }
    
    s3_client.put_object(Bucket=bucket, Key=args['output_path'], Body=json.dumps(output_data))

# Setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def main():
    """Main function to run the Rock Paper Scissors Glue job"""
    job.init(args['JOB_NAME'], args)
    
    bucket, key = read_sqs_message(args['sqs_arn'])
    
    if bucket and key:
        logger.info(f"Processing file from bucket: {bucket}, key: {key}")
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        player_data = json.loads(obj['Body'].read())
        
        system_move = system_turn()
        result = score(player_data['move'], system_move)
        logger.info(f"Player: {player_data['move']}, System: {system_move}, Result: {result}")
        
        output_data = {
            'player_move': player_data['move'],
            'system_move': system_move,
            'result': result
        }
        
        s3_client.put_object(Bucket=bucket, Key=args['output_path'], Body=json.dumps(output_data))
        logger.info(f"Output written to {args['output_path']}")
    else:
        logger.warning("No messages found in SQS queue")
    
    job.commit()

if __name__ == '__main__':
    main()