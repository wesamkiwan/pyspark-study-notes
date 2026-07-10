"""
Solution to exercises/exercise_02.py — read this only after attempting it yourself.
"""

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "_output")


def safe_count(df) -> int:
    # Forcing real materialization (not just changing the counting API) is what actually
    # fixes the unreliable count when a corrupt-record column is involved -- see Lesson 3.
    return df.localCheckpoint().count()


def main() -> None:
    spark = (
        SparkSession.builder.appName("io-exercise-02")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    orders_schema = StructType(
        [
            StructField("order_id", IntegerType()),
            StructField("emp_id", IntegerType()),
            StructField("product", StringType()),
            StructField("category", StringType()),
            StructField("amount", DoubleType()),
            StructField("order_date", DateType()),
            StructField("region", StringType()),
            StructField("_corrupt_record", StringType()),
        ]
    )

    orders = spark.read.csv(
        os.path.join(DATA_DIR, "messy_orders.csv"),
        header=True,
        schema=orders_schema,
        mode="PERMISSIVE",
        columnNameOfCorruptRecord="_corrupt_record",
    )

    clean = orders.filter(col("_corrupt_record").isNull())
    quarantine = orders.filter(col("_corrupt_record").isNotNull())

    clean_count = safe_count(clean)
    quarantine_count = safe_count(quarantine)

    # --- Part B: schema evolution ---
    employees = spark.read.csv(
        os.path.join(DATA_DIR, "employees.csv"), header=True, inferSchema=True
    )

    v1 = employees.select("emp_id", "name", "department")
    v2 = employees.select("emp_id", "name", "department", "salary")

    v1_path = os.path.join(OUTPUT_DIR, "evolving", "v1")
    v2_path = os.path.join(OUTPUT_DIR, "evolving", "v2")
    v1.write.mode("overwrite").parquet(v1_path)
    v2.write.mode("overwrite").parquet(v2_path)

    no_merge = spark.read.parquet(v1_path, v2_path)
    no_merge_columns = no_merge.columns

    merged = spark.read.option("mergeSchema", "true").parquet(v1_path, v2_path)
    merged_columns = merged.columns
    merged_row_count = safe_count(merged)

    # ---- self-check ----
    print(f"clean_count = {clean_count}, quarantine_count = {quarantine_count}")
    assert clean_count == 5, f"expected 5 clean rows, got {clean_count}"
    assert quarantine_count == 3, f"expected 3 quarantined rows, got {quarantine_count}"

    print(f"no_merge_columns = {no_merge_columns}")
    assert "salary" not in no_merge_columns, "without mergeSchema, salary should NOT appear"
    assert len(no_merge_columns) == 3, f"expected 3 columns without merge, got {no_merge_columns}"

    print(f"merged_columns = {merged_columns}, merged_row_count = {merged_row_count}")
    assert "salary" in merged_columns, "with mergeSchema=true, salary SHOULD appear"
    assert merged_row_count == 30, f"expected 30 total rows (15 + 15), got {merged_row_count}"

    print("\nAll checks passed!")
    spark.stop()


if __name__ == "__main__":
    main()
