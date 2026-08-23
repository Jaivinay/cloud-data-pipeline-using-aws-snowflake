import sys
from pathlib import Path
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "utils"))

from pyspark.sql import SparkSession
from transform import clean_customers, clean_products, clean_orders, standardize_dates
from data_quality import (
    check_no_duplicate_keys, check_no_nulls, check_referential_integrity,
    check_value_range, DataQualityError,
)


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.appName("tests").master("local[1]").getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def test_standardize_dates_handles_all_three_source_formats(spark):
    df = spark.createDataFrame(
        [("2024-01-15",), ("01/15/2024",), ("15-01-2024",), (None,)], ["d"]
    )
    result = standardize_dates(df, "d").collect()
    parsed = [r["d"] for r in result]
    assert parsed[0] == parsed[1] == parsed[2]  # all three formats parse to the same date
    assert parsed[3] is None  # null stays null, doesn't get misparsed into a fake date


def test_clean_customers_dedupes_by_id(spark):
    df = spark.createDataFrame(
        [("C1", "a@x.com", "2024-01-01"), ("C1", "a@x.com", "2024-01-01"), ("C2", "b@x.com", "2024-01-02")],
        ["customer_id", "email", "signup_date"],
    )
    result = clean_customers(df)
    assert result.count() == 2


def test_clean_customers_flags_missing_email_instead_of_dropping(spark):
    # single-row all-None column breaks PySpark's schema inference (learned this running
    # the test, not guessing) - give it an explicit schema instead of inferring
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([
        StructField("customer_id", StringType()), StructField("email", StringType()),
        StructField("signup_date", StringType()),
    ])
    df = spark.createDataFrame([("C1", None, "2024-01-01")], schema)
    result = clean_customers(df).collect()
    assert len(result) == 1  # row kept
    assert result[0]["has_valid_email"] is False


def test_clean_orders_imputes_null_quantity_and_flags_it(spark):
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    schema = StructType([
        StructField("order_id", StringType()), StructField("customer_id", StringType()),
        StructField("product_id", StringType()), StructField("quantity", IntegerType()),
        StructField("order_date", StringType()), StructField("unit_price_at_order", DoubleType()),
    ])
    df = spark.createDataFrame([("O1", "C1", "P1", None, "2024-01-01", 10.0)], schema)
    result = clean_orders(df).collect()[0]
    assert result["quantity"] == 1  # imputed
    assert result["quantity_was_imputed"] is True
    assert result["line_total"] == 10.0


def test_referential_integrity_check_catches_orphan_fact_rows(spark):
    dim = spark.createDataFrame([("C1",)], ["customer_id"])
    fact = spark.createDataFrame([("O1", "C1"), ("O2", "C999")], ["order_id", "customer_id"])
    with pytest.raises(DataQualityError):
        check_referential_integrity(fact, "customer_id", dim, "customer_id", "test")


def test_duplicate_key_check_catches_duplicates(spark):
    df = spark.createDataFrame([("C1",), ("C1",)], ["customer_id"])
    with pytest.raises(DataQualityError):
        check_no_duplicate_keys(df, "customer_id", "test")


def test_value_range_check_catches_out_of_range(spark):
    df = spark.createDataFrame([(5,), (500,)], ["quantity"])
    with pytest.raises(DataQualityError):
        check_value_range(df, "quantity", 1, 100, "test")
