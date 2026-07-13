"""
Solution to exercises/exercise_02.py -- read this only after attempting it yourself.
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

    west = orders.filter(col("region") == "West").select("order_id", "product", "amount")
    east = orders.filter(col("region") == "East").select("amount", "order_id", "product")

    combined = west.unionByName(east)

    distinct_combos = orders.select("product", "category").distinct()

    orders.createOrReplaceTempView("orders_sql")
    region_revenue = spark.sql(
        """
        SELECT region, COUNT(*) AS order_count, ROUND(SUM(amount), 2) AS total_revenue
        FROM orders_sql
        GROUP BY region
        ORDER BY total_revenue DESC
        """
    )

    # ---- self-check ----
    combined_count = combined.count()
    print(f"combined_count = {combined_count}")
    assert combined_count == 11, f"expected 11 rows (7 west + 4 east), got {combined_count}"

    east_order_ids = [1003, 1006, 1010, 1015]
    matched = combined.filter(col("order_id").isin(east_order_ids)).count()
    print(f"matched east order_ids in combined = {matched}")
    assert matched == 4, (
        f"expected all 4 east order_ids to be findable under the order_id column, got {matched}"
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
