import json
import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import random
import logging
from awsglue.utils import getResolvedOptions
import sys

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# AWS clients
s3_client = boto3.client('s3')

# Setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "input_path", "output_path"]
)

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

def process_parquet_data(s3_path):
    """Read and process parquet file from S3"""
    try:
        # Read parquet file using Glue DynamicFrame
        dynamic_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [s3_path]},
            format="parquet"
        )
        
        # Convert to Spark DataFrame for easier processing
        df = dynamic_frame.toDF()
        
        # Show schema for debugging
        logger.info("Parquet file schema:")
        df.printSchema()
        
        # Collect all rows to process each user action
        rows = df.collect()
        results = []
        
        for row in rows:
            # Extract player move from the row
            # Assuming the parquet has columns: user_id, action
            user_id = row['user_id']
            player_move = row['action']
            
            # Generate system move and calculate result
            system_move = system_turn()
            result = score(player_move, system_move)
            
            results.append({
                'user_id': user_id,
                'player_move': player_move,
                'system_move': system_move,
                'result': result
            })
            
            logger.info(f"User {user_id}: Player: {player_move}, System: {system_move}, Result: {result}")
        
        return results
        
    except Exception as e:
        logger.error(f"Error processing parquet data: {e}")
        return []

def write_results_to_s3(results, bucket, output_key):
    """Write results to S3 as JSON"""
    try:
        # Convert results to DataFrame
        results_df = spark.createDataFrame(results)
        
        results_dynamic_frame = DynamicFrame.fromDF(
            results_df, 
            glueContext, 
            "results_frame"
        )
        
        # Write as parquet to S3
        glueContext.write_dynamic_frame.from_options(
            frame=results_dynamic_frame,
            connection_type="s3",
            connection_options={"path": f"s3://{bucket}/{output_key}"},
            format="parquet"
        ).repartition(1)
        
        logger.info(f"Results written to s3://{bucket}/{output_key}")
        
    except Exception as e:
        logger.error(f"Error writing results to S3: {e}")

def main():
    """Main function to run the Rock Paper Scissors Glue job"""
    try:
        job.init(args['JOB_NAME'], args)

        input_path = args['input_path']
        output_path = args['output_path']
        bucket = input_path.split('/')[2]

        logger.info(f"Processing parquet file: {input_path}")
        results = process_parquet_data(input_path)

        if results:
            output_key = '/'.join(output_path.replace('s3://', '').split('/')[1:])
            write_results_to_s3(results, bucket, output_key)
            logger.info(f"Successfully processed {len(results)} records")
        else:
            logger.warning("No data processed from parquet file")

        job.commit()

    except Exception as e:
        logger.error(f"Job failed with error: {e}")
        raise

if __name__ == '__main__':
    main()
