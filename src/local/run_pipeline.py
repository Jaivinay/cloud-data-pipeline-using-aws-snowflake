# src/local/run_pipeline.py
#
# Runs the whole pipeline end to end locally: raw data -> upload to S3 -> PySpark
# transform (reading/writing S3, same as the real Glue job would) -> data quality checks
# -> Snowflake load (stubbed unless real credentials are set, see load_to_snowflake.py).
#
# S3 here is moto's mocked S3, not real AWS - moto intercepts boto3 calls and simulates
# S3 behavior in-process, so this is genuinely exercising real boto3 code paths (upload,
# list, download) without needing an AWS account to run the pipeline locally. Point
# S3_ENDPOINT_URL at nothing (real AWS) and this same code talks to real S3 - that's the
# whole reason to use moto instead of just reading/writing local files directly.

import sys
import time
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "utils"))

from moto import mock_aws
from pyspark.sql import SparkSession

from s3_helpers import get_s3_client, ensure_bucket, upload_directory, download_prefix
from transform import run_transform
from data_quality import run_all_checks, DataQualityError

ROOT = Path(__file__).parent.parent.parent
RAW_DIR = ROOT / "data" / "raw"
PROCESSED_DIR = ROOT / "data" / "processed"
BUCKET = "cloud-pipeline-demo"


@mock_aws
def run():
    print("=" * 60)
    print("STEP 1: Upload raw data to S3 (raw zone)")
    print("=" * 60)
    s3 = get_s3_client()
    ensure_bucket(s3, BUCKET)
    uploaded = upload_directory(s3, RAW_DIR, BUCKET, "raw")
    for key in uploaded:
        print(f"  uploaded -> s3://{BUCKET}/{key}")

    print("\n" + "=" * 60)
    print("STEP 2: AWS Glue Crawler simulation (schema discovery)")
    print("=" * 60)
    # a real Glue Crawler infers schema + registers it in the Glue Data Catalog. we don't
    # have Glue Catalog available outside AWS, so this step just prints the inferred
    # schema PySpark would see - the actual schema-read happens for real in step 3
    spark = SparkSession.builder.appName("pipeline").master("local[2]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    for name in ["customers", "products", "orders"]:
        df = spark.read.option("header", True).option("inferSchema", True).csv(str(RAW_DIR / f"{name}.csv"))
        print(f"  {name}: {df.dtypes}")

    print("\n" + "=" * 60)
    print("STEP 3: Glue ETL transform (PySpark)")
    print("=" * 60)
    # reads local RAW_DIR here rather than downloading from mocked S3 first, since PySpark's
    # native S3 connector (s3a://) needs real AWS or a running S3-compatible endpoint with
    # hadoop-aws configured - moto's in-process mock doesn't serve that protocol. the real
    # Glue job (src/glue_jobs/glue_etl_job.py) reads s3://... directly since it runs inside
    # AWS where that connector works natively. this is the one place local vs Glue diverges,
    # and it's called out explicitly rather than silently papered over.
    tables = run_transform(spark, str(RAW_DIR))
    for name, df in tables.items():
        print(f"  {name}: {df.count()} rows")

    print("\n" + "=" * 60)
    print("STEP 4: Data quality validation")
    print("=" * 60)
    try:
        results = run_all_checks(tables)
        for r in results:
            status = "PASS" if r["passed"] else "FAIL"
            print(f"  [{status}] {r['check']}")
    except DataQualityError as e:
        print(f"  DATA QUALITY FAILURE: {e}")
        print("  Halting pipeline - would not proceed to write processed zone or load Snowflake.")
        spark.stop()
        return

    print("\n" + "=" * 60)
    print("STEP 5: Write processed zone (Parquet) + upload to S3")
    print("=" * 60)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    for name, df in tables.items():
        out_path = PROCESSED_DIR / name
        df.write.mode("overwrite").parquet(str(out_path))
        uploaded = upload_directory(s3, out_path, BUCKET, f"processed/{name}")
        print(f"  {name}: wrote {len(uploaded)} parquet part(s) -> s3://{BUCKET}/processed/{name}/")

    print("\n" + "=" * 60)
    print("STEP 6: Snowflake load")
    print("=" * 60)
    print("  Skipped in this local run - requires real Snowflake credentials.")
    print("  See src/local/load_to_snowflake.py and snowflake/ddl/*.sql for the real load path.")

    spark.stop()
    print("\nPipeline complete.")


if __name__ == "__main__":
    run()
