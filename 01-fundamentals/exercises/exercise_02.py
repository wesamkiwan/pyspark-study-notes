"""
Exercise 2 — Lazy evaluation, actions vs transformations, reading query plans

This exercise is half code, half reasoning. Fill in every `# TODO`, including the
`# ANSWER:` comments where you must write a one-line explanation in your own words.
There are no automated checks for the reasoning questions — the point is to think them
through and then compare against solutions/solution_02.py.

Run with:
    python 01-fundamentals/exercises/exercise_02.py
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

    # --- Part A: predict before you run ---
    #
    # Below, three transformations are chained but NO action is called yet.
    #
    #   filtered = orders.filter(col("category") == "Hardware")
    #   selected = filtered.select("order_id", "amount", "region")
    #   renamed = selected.withColumnRenamed("amount", "order_amount")
    #
    # ANSWER (write 1-2 sentences): has any of this actually run against the data yet?
    # Why or why not?
    # TODO: ______________________________________________________

    filtered = orders.filter(col("category") == "Hardware")
    selected = filtered.select("order_id", "amount", "region")
    renamed = selected.withColumnRenamed("amount", "order_amount")

    # TODO: now trigger an action that prints the first 5 rows of `renamed`.
    #   (this is the point where computation actually happens)

    # --- Part B: narrow vs wide, shuffle or no shuffle? ---
    #
    # Below is a groupBy + sum -- a WIDE transformation (needs a shuffle).
    #
    # ANSWER: which single word in the code below signals that a shuffle will happen,
    # and why does summing require data to move between partitions in a way that
    # filtering/selecting does not?
    # TODO: ______________________________________________________

    revenue_by_region = orders.groupBy("region").agg(
        spark_sum("amount").alias("total_revenue")
    )

    # TODO: call .explain() on revenue_by_region and read the physical plan.
    #   Look for an operator whose name contains "Exchange" — that IS the shuffle,
    #   made visible in the plan.

    # TODO: now actually show revenue_by_region, ordered by total_revenue descending.

    # --- Part C: safe vs dangerous actions ---
    #
    # ANSWER: of .collect(), .show(), .count(), and .take(5), which one would be
    # DANGEROUS to call on a 500-million-row DataFrame, and why specifically?
    # TODO: ______________________________________________________

    spark.stop()


if __name__ == "__main__":
    main()
