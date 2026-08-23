# src/utils/data_quality.py
#
# A handful of concrete checks that run after the transform, before anything gets loaded
# into Snowflake. Not a full framework (that's explicitly a "future enhancement" in the
# README) - this is the minimum that would actually catch a broken pipeline run.

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class DataQualityError(Exception):
    pass


def check_no_duplicate_keys(df: DataFrame, key_col: str, name: str):
    total = df.count()
    distinct = df.select(key_col).distinct().count()
    if total != distinct:
        raise DataQualityError(f"{name}: {total - distinct} duplicate {key_col} values after dedup step")
    return {"check": f"{name}_no_duplicates", "passed": True, "row_count": total}


def check_no_nulls(df: DataFrame, col: str, name: str, allow_fraction: float = 0.0):
    total = df.count()
    nulls = df.filter(F.col(col).isNull()).count()
    fraction = nulls / total if total else 0
    passed = fraction <= allow_fraction
    if not passed:
        raise DataQualityError(f"{name}.{col}: {fraction:.1%} null, exceeds allowed {allow_fraction:.1%}")
    return {"check": f"{name}_{col}_nulls", "passed": True, "null_fraction": fraction}


def check_referential_integrity(fact_df: DataFrame, fact_key: str, dim_df: DataFrame, dim_key: str, name: str):
    """every fact_key value must exist in the dimension table - catches an ETL bug
    where fact rows reference a customer/product that got filtered out upstream."""
    orphans = fact_df.join(dim_df, fact_df[fact_key] == dim_df[dim_key], "left_anti").count()
    if orphans > 0:
        raise DataQualityError(f"{name}: {orphans} fact rows have no matching {dim_key} in dimension table")
    return {"check": f"{name}_referential_integrity", "passed": True, "orphan_count": orphans}


def check_value_range(df: DataFrame, col: str, min_val, max_val, name: str):
    out_of_range = df.filter((F.col(col) < min_val) | (F.col(col) > max_val)).count()
    if out_of_range > 0:
        raise DataQualityError(f"{name}.{col}: {out_of_range} rows outside [{min_val}, {max_val}]")
    return {"check": f"{name}_{col}_range", "passed": True}


def run_all_checks(tables: dict) -> list[dict]:
    """runs the full check suite against the transformed tables. raises on the first
    failure - stopping the load is the right call here, a partially-validated warehouse
    load is worse than a failed pipeline run someone gets paged for."""
    results = []
    results.append(check_no_duplicate_keys(tables["dim_customers"], "customer_id", "dim_customers"))
    results.append(check_no_duplicate_keys(tables["dim_products"], "product_id", "dim_products"))
    results.append(check_no_duplicate_keys(tables["fact_orders"], "order_id", "fact_orders"))

    results.append(check_no_nulls(tables["fact_orders"], "quantity", "fact_orders", allow_fraction=0.0))
    results.append(check_no_nulls(tables["fact_orders"], "date_key", "fact_orders", allow_fraction=0.0))

    results.append(check_referential_integrity(
        tables["fact_orders"], "customer_id", tables["dim_customers"], "customer_id", "fact_orders_to_dim_customers"
    ))
    results.append(check_referential_integrity(
        tables["fact_orders"], "product_id", tables["dim_products"], "product_id", "fact_orders_to_dim_products"
    ))

    results.append(check_value_range(tables["fact_orders"], "quantity", 1, 100, "fact_orders"))
    results.append(check_value_range(tables["fact_orders"], "line_total", 0, 100000, "fact_orders"))

    return results
