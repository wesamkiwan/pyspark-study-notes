"""
Solution to exercises/exercise_02.py — read this only after attempting it yourself.
"""

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

DATA_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "data", "orders.csv")


def main() -> None:
    spark = (
        SparkSession.builder.appName("exercise-02")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    orders = spark.read.csv(DATA_PATH, header=True, inferSchema=True)

    # --- Part A ---
    # ANSWER: No computation has happened yet. filter/select/withColumnRenamed are all
    # transformations, which are lazy — Spark only records them as steps in a logical
    # plan. Nothing runs against the actual data until an action (like .show()) is called.

    filtered = orders.filter(col("category") == "Hardware")
    selected = filtered.select("order_id", "amount", "region")
    renamed = selected.withColumnRenamed("amount", "order_amount")

    print("Part A - first 5 Hardware orders:")
    renamed.show(5)  # <-- the action; this is where computation actually happens

    # --- Part B ---
    # ANSWER: "groupBy" is the signal. Filtering/selecting can be done independently,
    # partition by partition (narrow), because each row's fate doesn't depend on any
    # other row. Summing per region requires every row for a given region to be
    # co-located on the same partition before the sum can be computed, and rows for
    # the same region can start out scattered across many different partitions — so
    # Spark must physically move ("shuffle") data across the network/disk to group
    # matching keys together first.

    revenue_by_region = orders.groupBy("region").agg(
        spark_sum("amount").alias("total_revenue")
    )

    print("\nPart B - physical plan (look for 'Exchange' = the shuffle):")
    revenue_by_region.explain()

    print("\nPart B - revenue by region:")
    revenue_by_region.orderBy(col("total_revenue").desc()).show()

    # --- Part C ---
    # ANSWER: .collect() is the dangerous one. It pulls EVERY row back to the driver's
    # single-machine Python process as an in-memory list. On 500 million rows this will
    # almost certainly exceed the driver's memory and crash the application. .show() and
    # .take(5) only ever pull a small, bounded number of rows. .count() never pulls row
    # data to the driver at all — it computes the count in a distributed way and returns
    # just a single integer.

    spark.stop()


if __name__ == "__main__":
    main()
