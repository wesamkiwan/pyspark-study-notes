"""
Exercise 2 -- safely combining reordered DataFrames, distinct combinations, and Spark SQL.

Fill in every `# TODO`. Run with:

    python 03-core-transformations/exercises/exercise_02.py

Don't peek at solutions/solution_02.py until you've tried this. This exercise deliberately
recreates Lesson 4's union-column-order trap -- read that lesson first if you haven't.
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


def main() -> None:
    spark = (
        SparkSession.builder.appName("core-transformations-exercise-02")
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

    # `east` simulates a partner feed whose columns arrived in a different order than `west`'s --
    # a realistic scenario, not a contrived one. Both are given to you as-is.
    west = orders.filter(col("region") == "West").select("order_id", "product", "amount")
    east = orders.filter(col("region") == "East").select("amount", "order_id", "product")

    # TODO 1: combine `west` and `east` into one DataFrame WITHOUT scrambling the data --
    #   i.e. don't use plain `.union()` here. See Lesson 4 for why.
    combined = None  # <-- replace this

    # TODO 2: get the distinct (product, category) combinations across ALL of `orders`
    #   (not just west/east) as a DataFrame.
    distinct_combos = None  # <-- replace this

    # TODO 3: register `orders` as a temp view named "orders_sql", then use spark.sql(...)
    #   to compute, per region: order_count = COUNT(*), total_revenue = ROUND(SUM(amount), 2).
    #   Alias both, and order by total_revenue descending.
    region_revenue = None  # <-- replace this

    # ---- self-check ----
    combined_count = combined.count()
    print(f"combined_count = {combined_count}")
    assert combined_count == 11, f"expected 11 rows (7 west + 4 east), got {combined_count}"

    east_order_ids = [1003, 1006, 1010, 1015]
    matched = combined.filter(col("order_id").isin(east_order_ids)).count()
    print(f"matched east order_ids in combined = {matched}")
    assert matched == 4, (
        f"expected all 4 east order_ids to be findable under the order_id column, got {matched} "
        "-- if you used plain .union(), the east rows' real order_ids ended up mislabeled as "
        "`product` instead (see Lesson 4)"
    )

    combo_count = distinct_combos.count()
    print(f"distinct_combos count = {combo_count}")
    assert combo_count == 5, f"expected 5 distinct (product, category) pairs, got {combo_count}"

    revenue_rows = [r.asDict() for r in region_revenue.collect()]
    print(f"region_revenue = {revenue_rows}")
    assert revenue_rows[0]["region"] == "West" and revenue_rows[0]["total_revenue"] == 3670.99
    assert revenue_rows[-1]["region"] == "South" and revenue_rows[-1]["order_count"] == 1

    print("\nAll checks passed!")
    spark.stop()


if __name__ == "__main__":
    main()
