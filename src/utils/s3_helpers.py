# src/utils/s3_helpers.py
#
# Thin boto3 wrapper. Nothing clever here on purpose - the point is these are real boto3
# calls, so pointing this module at real AWS instead of the moto mock (see
# src/local/run_pipeline.py for how that's toggled) is a config change, not a rewrite.

import boto3
from pathlib import Path


def get_s3_client(endpoint_url=None):
    """endpoint_url is only set when running against moto's mock S3 locally.
    Against real AWS, leave it None and boto3 picks up credentials from the environment
    the normal way (env vars, ~/.aws/credentials, or an IAM role if running on Glue/EC2)."""
    return boto3.client("s3", endpoint_url=endpoint_url, region_name="us-east-1")


def ensure_bucket(s3_client, bucket_name: str):
    existing = [b["Name"] for b in s3_client.list_buckets().get("Buckets", [])]
    if bucket_name not in existing:
        s3_client.create_bucket(Bucket=bucket_name)


def upload_directory(s3_client, local_dir: Path, bucket_name: str, s3_prefix: str):
    uploaded = []
    for path in Path(local_dir).glob("*"):
        if path.is_file():
            key = f"{s3_prefix}/{path.name}"
            s3_client.upload_file(str(path), bucket_name, key)
            uploaded.append(key)
    return uploaded


def download_prefix(s3_client, bucket_name: str, s3_prefix: str, local_dir: Path):
    local_dir = Path(local_dir)
    local_dir.mkdir(parents=True, exist_ok=True)
    paginator = s3_client.get_paginator("list_objects_v2")
    downloaded = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=s3_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            dest = local_dir / Path(key).name
            s3_client.download_file(bucket_name, key, str(dest))
            downloaded.append(str(dest))
    return downloaded


def list_prefix(s3_client, bucket_name: str, s3_prefix: str) -> list[str]:
    paginator = s3_client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=s3_prefix):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return keys
