---> set Role Context
USE ROLE accountadmin;

---> set Warehouse Context
USE WAREHOUSE compute_wh;

---> create the Database
CREATE DATABASE IF NOT EXISTS Rock_Paper_Scissors
COMMENT = 'to view and analyse the scores of rock paper scissors' ;

---> create the Schema
create schema if not exists ROCK_PAPER_SCISSORS.Glue_Results;

---> create the Table
create table if not exists ROCK_PAPER_SCISSORS.Glue_Results.results
(
    player_move    string,
    result         string,
    system_move    string,
    user_id        string
);
---> query the empty Table
SELECT * FROM ROCK_PAPER_SCISSORS.Glue_Results.results;

---> Create the Amazon S3 Storage Integration
    -- Configuring a Snowflake Storage Integration to Access Amazon S3: https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration


CREATE STAGE my_s3_stage
 STORAGE_INTEGRATION = S3_INIT
 URL = 's3://rps-pipeline-landing-zone/output/'
 FILE_FORMAT = (TYPE = PARQUET);

 ---> Describe our Integration
    -- DESCRIBE INTEGRATIONS: https://docs.snowflake.com/en/sql-reference/sql/desc-integration
DESCRIBE INTEGRATION S3_INIT;

---> View our Storage Integrations
    -- SHOW INTEGRATIONS: https://docs.snowflake.com/en/sql-reference/sql/show-integrations

SHOW STORAGE INTEGRATIONS;

COPY INTO ROCK_PAPER_SCISSORS.GLUE_RESULTS.RESULTS
    FROM @ROCK_PAPER_SCISSORS.GLUE_RESULTS.AWS_GLUE_RESULTS
    FILE_FORMAT = (TYPE = PARQUET)
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

SELECT * FROM ROCK_PAPER_SCISSORS.GLUE_RESULTS.RESULTS LIMIT 5;