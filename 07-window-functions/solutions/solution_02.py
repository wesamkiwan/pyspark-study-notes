"""
Solution to exercises/exercise_02.py -- read this only after attempting it yourself.
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

    w_rows = (
        Window.partitionBy("category")
        .orderBy("amount", "order_id")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    running_rows = (
        orders.select(
            "category", "product", "order_id", "amount",
            spark_sum("amount").over(w_rows).alias("running_total"),
        )
        .filter((col("category") == "Hardware") & (col("product") == "Widget A"))
    )

    w_default = Window.partitionBy("category").orderBy("amount")
    running_default = (
        orders.select(
            "category", "product", "order_id", "amount",
            spark_sum("amount").over(w_default).alias("running_total"),
        )
        .filter((col("category") == "Hardware") & (col("product") == "Widget A"))
    )

    w_lag = Window.partitionBy("category").orderBy("order_date")
    gapped = orders.select(
        "category", "order_date", lag("order_date").over(w_lag).alias("prev_date")
    ).withColumn("days_since_prev", datediff(col("order_date"), col("prev_date")))
    max_gap_rows = gapped.groupBy("category").agg({"days_since_prev": "max"}).collect()
    max_gap_by_category = {r["category"]: r["max(days_since_prev)"] for r in max_gap_rows}

    no_partition_plan = (
        orders.withColumn("gr", row_number().over(Window.orderBy(col("amount").desc())))
        ._jdf.queryExecution()
        .executedPlan()
        .toString()
    )
    plan_has_single_partition = "SinglePartition" in no_partition_plan

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
