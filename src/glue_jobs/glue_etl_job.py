# src/glue_jobs/glue_etl_job.py
#
# This is what actually deploys to AWS Glue as a job. It's a thin wrapper around
# src/utils/transform.py - the transform logic itself is tested locally with plain
# PySpark (see tests/test_transform.py) since the `awsglue` package only exists inside
# the AWS Glue runtime and can't be installed/tested outside it. Splitting it this way
# means the actual transformation logic has real test coverage, and this file is just
# glue-job boilerplate (reading job args, wrapping the Spark session, writing to S3)
# that's necessarily untested outside AWS itself.
#
# Deploy: upload this file + src/utils/transform.py + src/utils/data_quality.py to the
# S3 location your Glue job's script path points to, or package as a Glue job zip.
#
# Job parameters expected (--RAW_S3_PATH, --OUTPUT_S3_PATH are custom job parameters
# you'd set when creating the Glue job in the console or via Terraform/CloudFormation):
#   --RAW_S3_PATH     s3://your-bucket/raw/
#   --OUTPUT_S3_PATH  s3://your-bucket/processed/

import sys
sys.path.append(".")  # so `from transform import ...` resolves when this + transform.py are co-located

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

from transform import run_transform
from data_quality import run_all_checks, DataQualityError

args = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_S3_PATH", "OUTPUT_S3_PATH"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

tables = run_transform(spark, args["RAW_S3_PATH"])

try:
    check_results = run_all_checks(tables)
    print(f"Data quality: {len(check_results)}/{len(check_results)} checks passed")
except DataQualityError as e:
    # fail the Glue job loudly rather than write bad data to the processed zone -
    # CloudWatch alarms on Glue job failure are how you'd get paged for this
    print(f"DATA QUALITY CHECK FAILED: {e}")
    job.commit()
    sys.exit(1)

for name, df in tables.items():
    (
        df.write
        .mode("overwrite")
        .parquet(f"{args['OUTPUT_S3_PATH']}/{name}/")
    )
    print(f"wrote {name} -> {args['OUTPUT_S3_PATH']}/{name}/")

job.commit()
