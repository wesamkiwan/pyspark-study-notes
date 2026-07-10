"""
Exercise 2 — Quarantining bad records safely, and schema evolution

Fill in every `# TODO`. Run with:

    python 02-io-and-schemas/exercises/exercise_02.py

Don't peek at solutions/solution_02.py until you've tried this. This exercise deliberately
makes you apply the "don't trust bare .count()" warning from Lesson 3 — read that lesson first
if you haven't.
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
    """
    TODO 1: implement a row count that is reliable even when `df` (or an ancestor of it)
    was read with a corrupt-record column in its schema -- see Lesson 3's warning about
    .count() being unreliable in that situation. Note: swapping .count() for
    agg(count("*")) does NOT fix this on its own (verified) -- the fix has to force real
    materialization first, e.g. via .cache() or .localCheckpoint(), before counting.
    """
    raise NotImplementedError


def main() -> None:
    spark = (
        SparkSession.builder.appName("io-exercise-02")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # TODO 2: build a schema for messy_orders.csv (order_id int, emp_id int, product string,
    #   category string, amount double, order_date date, region string) PLUS a
    #   "_corrupt_record" string column for capturing malformed rows.
    orders_schema = None  # <-- replace this

    # TODO 3: read data/messy_orders.csv with this schema, mode="PERMISSIVE", and
    #   columnNameOfCorruptRecord="_corrupt_record".
    orders = None  # <-- replace this

    # TODO 4: split into `clean` (._corrupt_record IS NULL) and `quarantine` (IS NOT NULL).
    clean = None  # <-- replace this
    quarantine = None  # <-- replace this

    # TODO 5: get their row counts using your safe_count() function, not bare .count().
    clean_count = None  # <-- replace this
    quarantine_count = None  # <-- replace this

    # --- Part B: schema evolution ---
    employees = spark.read.csv(
        os.path.join(DATA_DIR, "employees.csv"), header=True, inferSchema=True
    )

    # TODO 6: write two versions of this data to OUTPUT_DIR/evolving/v1 and .../v2:
    #   v1 = only emp_id, name, department
    #   v2 = emp_id, name, department, AND salary
    #   (overwrite mode, parquet)

    # TODO 7: read v1 and v2 together WITHOUT mergeSchema. How many columns does the
    #   result have? Store the column list in `no_merge_columns`.
    no_merge_columns = None  # <-- replace this

    # TODO 8: read v1 and v2 together WITH mergeSchema=true. Store the column list in
    #   `merged_columns`, and the total row count (using safe_count, though a plain
    #   .count() is actually fine here -- there's no corrupt-record column involved)
    #   in `merged_row_count`.
    merged_columns = None  # <-- replace this
    merged_row_count = None  # <-- replace this

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
