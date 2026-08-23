-- 02_external_stage.sql
-- Points Snowflake at the S3 processed zone. Replace the placeholders with your actual
-- bucket and IAM role ARN (storage integration is the recommended auth method over
-- raw AWS keys - see AWS/Snowflake docs for the trust-relationship setup, it's a few
-- steps of back-and-forth between the two consoles that doesn't reduce to a single
-- SQL script).

CREATE STORAGE INTEGRATION IF NOT EXISTS s3_processed_zone_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<YOUR_ACCOUNT_ID>:role/snowflake-s3-access-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://<YOUR_BUCKET>/processed/');

CREATE FILE FORMAT IF NOT EXISTS parquet_format
  TYPE = PARQUET;

CREATE STAGE IF NOT EXISTS processed_zone_stage
  STORAGE_INTEGRATION = s3_processed_zone_integration
  URL = 's3://<YOUR_BUCKET>/processed/'
  FILE_FORMAT = parquet_format;

-- sanity check after setup:
-- LIST @processed_zone_stage;
