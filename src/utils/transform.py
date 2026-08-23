# src/utils/transform.py
#
# The actual PySpark transformation logic - cleaning, deduplication, standardization,
# and the star-schema build. Deliberately kept separate from src/glue_jobs/glue_etl_job.py
# so this exact code path is what runs both locally (tested with plain PySpark, see
# tests/test_transform.py) and on real AWS Glue (the glue job is a thin wrapper around
# these functions - see that file for why it can't be tested outside AWS directly).

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


def standardize_dates(df: DataFrame, col: str) -> DataFrame:
    """source data has 3 different date formats mixed in the same column (see
    generate_raw_data.py) - coalesce across format attempts.

    First pass at this used to_date() and it blew up the whole job: Spark's ANSI mode
    makes to_date() THROW on a format mismatch instead of returning null like I assumed,
    so the coalesce never even got a chance to fall through to the next format. Needed
    try_to_date() instead, which actually returns null on mismatch."""
    return df.withColumn(
        col,
        F.coalesce(
            F.expr(f"try_to_date({col}, 'yyyy-MM-dd')"),
            F.expr(f"try_to_date({col}, 'MM/dd/yyyy')"),
            F.expr(f"try_to_date({col}, 'dd-MM-yyyy')"),
        ),
    )


def clean_customers(df: DataFrame) -> DataFrame:
    df = df.dropDuplicates(["customer_id"])
    df = standardize_dates(df, "signup_date")
    # a missing email isn't a reason to drop the customer record - just flag it
    df = df.withColumn("has_valid_email", F.col("email").isNotNull())
    return df


def clean_products(df: DataFrame) -> DataFrame:
    df = df.dropDuplicates(["product_id"])
    df = df.withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
    return df


def clean_orders(df: DataFrame) -> DataFrame:
    df = df.dropDuplicates(["order_id"])
    df = standardize_dates(df, "order_date")

    # null quantity: rather than silently drop or silently impute, impute with 1
    # (the modal value) and flag the row so downstream analytics can decide whether
    # to trust it - dropping loses revenue signal, blind imputation hides a data
    # quality issue that should be visible
    df = df.withColumn("quantity_was_imputed", F.col("quantity").isNull())
    df = df.withColumn("quantity", F.coalesce(F.col("quantity"), F.lit(1)))

    df = df.withColumn(
        "line_total",
        F.col("quantity").cast(DoubleType()) * F.col("unit_price_at_order").cast(DoubleType()),
    )
    # drop rows where order_date failed to parse under all 3 known formats - can't
    # build a date dimension key for these, and there were none in practice, but a real
    # source system could send a genuinely malformed date
    df = df.filter(F.col("order_date").isNotNull())
    return df


def build_dim_date(df_orders: DataFrame) -> DataFrame:
    dates = df_orders.select(F.col("order_date").alias("date")).distinct()
    return (
        dates
        .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("day", F.dayofmonth("date"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("day_of_week", F.date_format("date", "EEEE"))
    )


def build_fact_orders(df_orders: DataFrame) -> DataFrame:
    return (
        df_orders
        .withColumn("date_key", F.date_format("order_date", "yyyyMMdd").cast("int"))
        .select(
            "order_id", "customer_id", "product_id", "date_key",
            "quantity", "unit_price_at_order", "line_total", "quantity_was_imputed",
        )
    )


def run_transform(spark: SparkSession, raw_dir: str):
    """orchestrates the full clean -> star-schema build. returns a dict of named
    DataFrames ready to be written out (parquet locally, or by the Glue job to S3)."""
    customers_raw = spark.read.option("header", True).csv(f"{raw_dir}/customers.csv")
    products_raw = spark.read.option("header", True).csv(f"{raw_dir}/products.csv")
    orders_raw = spark.read.option("header", True).csv(f"{raw_dir}/orders.csv")

    dim_customers = clean_customers(customers_raw)
    dim_products = clean_products(products_raw)
    orders_clean = clean_orders(orders_raw)
    dim_date = build_dim_date(orders_clean)
    fact_orders = build_fact_orders(orders_clean)

    return {
        "dim_customers": dim_customers,
        "dim_products": dim_products,
        "dim_date": dim_date,
        "fact_orders": fact_orders,
    }
