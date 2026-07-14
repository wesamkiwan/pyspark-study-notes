"""
Exercise 2 -- ROWS vs the default RANGE frame on data with real ties, lag-based gap
analysis, and checking a window's physical plan for the no-partitionBy trap.

Fill in every `# TODO`. Run with:

    python 07-window-functions/exercises/exercise_02.py

Don't peek at solutions/solution_02.py until you've tried this.
"""

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, lag, row_number, sum as spark_sum
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")


def main() -> None:
    spark = (
        SparkSession.builder.appName("window-exercise-02")
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
        ]
    )
    orders = spark.read.csv(
        os.path.join(DATA_DIR, "orders.csv"), header=True, schema=orders_schema
    )

    # TODO 1: build a window partitioned by "category", ordered by ("amount", "order_id")
    #   (the second column as a tiebreaker), with an EXPLICIT ROWS frame from
    #   unboundedPreceding to currentRow. Compute a running total of "amount" over it,
    #   then filter down to just the 5 Hardware / Widget A rows, ordered by order_id.
    #   Columns needed: category, product, order_id, amount, running_total.
    running_rows = None  # <-- replace this

    # TODO 2: build the SAME query but with NO explicit frame (just partitionBy +
    #   orderBy("amount") -- the default RANGE frame from Lesson 4). Filter to the same
    #   5 Hardware / Widget A rows.
    running_default = None  # <-- replace this

    # TODO 3: using lag("order_date") over a window partitioned by "category" ordered
    #   by "order_date", compute "days_since_prev" via datediff. Then find the MAXIMUM
    #   days_since_prev per category.
    max_gap_by_category = None  # <-- replace this (dict: category -> max gap in days)

    # TODO 4: confirm that a window with NO partitionBy at all (just
    #   Window.orderBy(col("amount").desc())) produces a physical plan containing
    #   "Exchange SinglePartition". Get the plan as a string via
    #   df._jdf.queryExecution().executedPlan().toString()
    plan_has_single_partition = None  # <-- replace this (a bool)

    # ---- self-check ----
    rows_totals = [r["running_total"] for r in running_rows.orderBy("order_id").collect()]
    print(f"rows_totals = {rows_totals}")
    assert rows_totals == [551.5, 801.5, 1051.5, 1301.5, 1551.5], rows_totals
    assert len(set(rows_totals)) == 5, "ROWS frame should give 5 DIFFERENT incrementing totals"

    default_totals = [r["running_total"] for r in running_default.orderBy("order_id").collect()]
    print(f"default_totals = {default_totals}")
    assert all(t == 1551.5 for t in default_totals), (
        f"expected the default RANGE frame to give the SAME total (1551.5) on every tied row, got {default_totals}"
    )

    print(f"max_gap_by_category = {max_gap_by_category}")
    assert max_gap_by_category == {"Electronics": 52, "Hardware": 33}, max_gap_by_category

    print(f"plan_has_single_partition = {plan_has_single_partition}")
    assert plan_has_single_partition is True, "expected 'Exchange SinglePartition' in the plan"

    print("\nAll checks passed!")
    spark.stop()


if __name__ == "__main__":
    main()
