"""
Solution to exercises/exercise_01.py -- read this only after attempting it yourself.
"""

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round, sum as spark_sum
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
        SparkSession.builder.appName("joins-exercise-01")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    emp_schema = StructType(
        [
            StructField("emp_id", IntegerType()),
            StructField("name", StringType()),
            StructField("department", StringType()),
            StructField("salary", DoubleType()),
            StructField("hire_date", DateType()),
            StructField("manager_id", IntegerType()),
        ]
    )
    employees = spark.read.csv(
        os.path.join(DATA_DIR, "employees.csv"), header=True, schema=emp_schema
    )

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

    no_orders_via_left = (
        employees.join(orders, on="emp_id", how="left")
        .filter(col("order_id").isNull())
        .select("name")
        .distinct()
    )

    no_orders_via_anti = employees.join(orders, on="emp_id", how="left_anti").select("name")

    totals_by_employee = (
        employees.join(orders, on="emp_id", how="inner")
        .groupBy("name")
        .agg(spark_round(spark_sum("amount"), 2).alias("total_amount"))
        .orderBy(col("total_amount").desc())
    )

    # ---- self-check ----
    left_names = sorted(r["name"] for r in no_orders_via_left.collect())
    anti_names = sorted(r["name"] for r in no_orders_via_anti.collect())
    print(f"no_orders_via_left ({len(left_names)}) = {left_names}")
    print(f"no_orders_via_anti ({len(anti_names)}) = {anti_names}")
    assert len(left_names) == 11, f"expected 11 names, got {len(left_names)}"
    assert left_names == anti_names, "LEFT+filter and LEFT ANTI should give the identical set"

    totals_rows = [r.asDict() for r in totals_by_employee.collect()]
    print(f"totals_by_employee = {totals_rows}")
    assert len(totals_rows) == 4, f"expected 4 employees with orders, got {len(totals_rows)}"
    assert totals_rows[0]["name"] == "Elena Petrova" and totals_rows[0]["total_amount"] == 2500.74
    assert totals_rows[-1]["name"] == "Katya Ivanova" and totals_rows[-1]["total_amount"] == 1461.24

    print("\nAll checks passed!")
    spark.stop()


if __name__ == "__main__":
    main()
