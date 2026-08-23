# Setup Guide

## Running locally (no AWS/Snowflake account needed)

```bash
pip install -r requirements.txt
python src/utils/generate_raw_data.py   # creates data/raw/*.csv
python src/local/run_pipeline.py        # runs the full pipeline against mocked S3
pytest tests/ -v
```

The local run uses `moto` to mock S3 in-process - real boto3 calls, simulated AWS
backend. It proves the ingestion/transform/validation logic works without needing
credentials. It does NOT touch Snowflake (see below for that).

## Deploying the real Glue job to AWS

1. Create an S3 bucket, upload `data/raw/*.csv` to `s3://<bucket>/raw/`.
2. Upload `src/glue_jobs/glue_etl_job.py`, `src/utils/transform.py`, and
   `src/utils/data_quality.py` to an S3 path Glue can read from (e.g. `s3://<bucket>/scripts/`).
3. Create a Glue job (Console, CLI, or Terraform) pointing at `glue_etl_job.py`, with
   `transform.py` and `data_quality.py` set as "Python library path" additional files.
4. Set job parameters: `--RAW_S3_PATH s3://<bucket>/raw/` and
   `--OUTPUT_S3_PATH s3://<bucket>/processed/`.
5. Attach an IAM role with S3 read/write on the bucket and standard Glue service permissions.
6. Run the job. Check CloudWatch Logs for the data quality check output.

## Connecting Snowflake

1. Run `snowflake/ddl/01_warehouse_db_schema.sql` through `06_fact_orders.sql` in order,
   as a role with CREATE privileges.
2. Set up the storage integration in `02_external_stage.sql` - this needs an IAM role
   trust relationship between AWS and Snowflake (Snowflake's docs walk through the
   exact steps: `DESC STORAGE INTEGRATION` gives you values to paste into the AWS IAM
   trust policy, then you update the Snowflake integration with the real role ARN).
3. Set environment variables: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`.
4. Run `python src/local/load_to_snowflake.py`.
5. Query with anything in `snowflake/queries/`.
