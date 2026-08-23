# src/local/load_to_snowflake.py
#
# Real snowflake-connector-python code. Not run as part of the standard local pipeline
# (see run_pipeline.py) since it needs a real Snowflake account - but this is functional
# code, not a stub, and it's how you'd actually wire the processed S3 parquet into the
# tables defined in snowflake/ddl/.

import os
import sys
from pathlib import Path

import snowflake.connector

SNOWFLAKE_DML_DIR = Path(__file__).parent.parent.parent / "snowflake" / "dml"

REQUIRED_ENV_VARS = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]

# order matters - dimensions before the fact table, see the comment at the top of
# load_fact_orders.sql for why
LOAD_ORDER = ["load_dim_customers.sql", "load_dim_products.sql", "load_dim_date.sql", "load_fact_orders.sql"]


def get_connection():
    missing = [v for v in REQUIRED_ENV_VARS if not os.environ.get(v)]
    if missing:
        raise RuntimeError(
            f"Missing required environment variables: {missing}. "
            f"Set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD "
            f"(and optionally SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, "
            f"which default to the values set up in snowflake/ddl/01_warehouse_db_schema.sql)."
        )
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "ecommerce_wh"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "ecommerce_db"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "analytics"),
    )


def run_load():
    conn = get_connection()
    cursor = conn.cursor()
    try:
        for filename in LOAD_ORDER:
            sql = (SNOWFLAKE_DML_DIR / filename).read_text()
            print(f"Running {filename}...")
            cursor.execute(sql)
            print(f"  {cursor.rowcount} rows affected")
    finally:
        cursor.close()
        conn.close()
    print("Snowflake load complete.")


if __name__ == "__main__":
    try:
        run_load()
    except RuntimeError as e:
        print(f"Skipped: {e}", file=sys.stderr)
        sys.exit(1)
