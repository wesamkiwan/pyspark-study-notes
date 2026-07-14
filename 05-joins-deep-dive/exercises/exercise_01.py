"""
Exercise 1 -- LEFT join vs LEFT ANTI for the same "no match" question, and an
inner join into a groupBy/agg (using on="col_name" to avoid the duplicate-column trap).

Fill in every `# TODO`. Run with:

    python 05-joins-deep-dive/exercises/exercise_01.py

Don't peek at solutions/solution_01.py until you've tried this.
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

    # TODO 1: LEFT join employees to orders on "emp_id". From that result, find the
    #   distinct names of employees with NO matching order (order_id IS NULL).
    no_orders_via_left = None  # <-- replace this (a DataFrame with one column: name)

    # TODO 2: get the SAME set of employees using a LEFT ANTI join instead -- no
    #   filtering on nullability required.
    no_orders_via_anti = None  # <-- replace this (a DataFrame with one column: name)

    # TODO 3: INNER join employees to orders on "emp_id" (use on="emp_id", not a
    #   condition, so there's only one emp_id column -- see Lesson 2), then group by
    #   name and compute each employee's total order amount, rounded to 2 decimals,
    #   aliased "total_amount". Order by total_amount descending.
    totals_by_employee = None  # <-- replace this

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
