CREATE DATABASE DB1 --CREATE A DATABASE

USE DATABASE DB1

CREATE SCHEMA TS1

--Create Stage

CREATE OR REPLACE STAGE S3_STAGE url = 's3://itransitionbucket/'
credentials = (aws_key_id = 'AKIASRUPMU7WNUAYPC5Y' aws_secret_key = 'UezjKBSVUPO+2RCA0rQP8OvIMDEbRTUq2pqvc5MF')

LIST @DB1.TS1.S3_STAGE/;

--CREATE table in Snowflake with VARIANT column

CREATE TABLE DB1.TS1.PERSON_NESTED
(
    INPUT_DATA VARIANT
);

--create File Format

CREATE FILE FORMAT DB1.TS1.JSON
TYPE = 'JSON' COMPRESSION = 'AUTO'
ENABLE_OCTAL = FALSE
ALLOW_DUPLICATE = FALSE
STRIP_OUTER_ARRAY = FALSE
STRIP_NULL_VALUES = FALSE
IGNORE_UTF8_ERRORS = FALSE
COMMENT = 'MY FIRST JSON FILE FORMAT';

--Create a Snowpipe with Auto Ingest Enabled

CREATE OR REPLACE PIPE DB1.TS1.PERSON_PIPE auto_ingest = true as
COPY INTO DB1.TS1.PERSON_NESTED
FROM @DB1.TS1.S3_STAGE
file_format = (type = 'JSON');

-- see my pipe
show pipes DB1.TS1.PERSON_PIPE;
show stages

--CHECK PIPE RUNNING
select system$pipe_status('DB1.TS1.PERSON_PIPE');

select * from table(information_schema.
copy_history(table_name=>'PERSON_NESTED',
start_time=>dateadd(hours,-1, current_timestamp())));

select * from DB1.TS1.PERSON_NESTED

--Change Data Capture using Streams, Tasks and Merge.
CREATE STREAM DB1.TS1.PERSON_NESTED_STREAM ON TABLE DB1.TS1.PERSON_NESTED

select * from PERSON_NESTED_STREAM
--create target table
CREATE OR REPLACE TABLE DB1.TS1.GAME_EVENTS
(
appUserId string,
platformAccountId string,
timestampClient string,
platform string,
appName string,
countryCode string
);


--drop table DB1.TS1.GAME_EVENTS

--Create task
CREATE OR REPLACE TASK DB1.TS1.PERSON_TASK_NEW
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '2 minute'
WHEN        
    SYSTEM$STREAM_HAS_DATA('DB1.TS1.PERSON_NESTED_STREAM') 
AS
    INSERT INTO DB1.TS1.GAME_EVENTS (
       appUserId,
       platformAccountId,
       timestampClient,
       platform,
       appName,
       countryCode
   ) 
SELECT 
  PARSE_JSON(INPUT_DATA):event_data:appUserId::string AS appUserId,
  PARSE_JSON(INPUT_DATA):event_data:data:eventData:platformAccountId::string AS platformAccountId,
  PARSE_JSON(INPUT_DATA):event_data:timestampClient::string AS timestampClient,
  PARSE_JSON(INPUT_DATA):event_data:platform::string AS platform,
  PARSE_JSON(INPUT_DATA):event_data:appName::string AS appName,
  PARSE_JSON(INPUT_DATA):event_data:countryCode::string AS countryCode
FROM DB1.TS1.PERSON_NESTED_STREAM;

--Manually run task
execute task DB1.TS1.PERSON_TASK_NEW;

--Resume my task
ALTER TASK DB1.TS1.PERSON_TASK_NEW RESUME;

SELECT count(*) FROM DB1.TS1.PERSON_NESTED;
SELECT COUNT(*) FROM DB1.TS1.PERSON_NESTED_STREAM;
SELECT count(*) FROM DB1.TS1.GAME_EVENTS;

SELECT CURRENT_WAREHOUSE();
SHOW TASKS ;
