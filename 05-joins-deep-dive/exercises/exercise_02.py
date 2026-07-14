"""
Exercise 2 -- forcing a broadcast join and checking the physical plan, the NULL
join-key non-match trap, and the crossJoin.enabled safety valve.

Fill in every `# TODO`. Run with:

    python 05-joins-deep-dive/exercises/exercise_02.py

Don't peek at solutions/solution_02.py until you've tried this.
"""

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import broadcast
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.utils import AnalysisException

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")


def main() -> None:
    spark = (
        SparkSession.builder.appName("joins-exercise-02")
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

    # TODO 1: join employees to orders on "emp_id", forcing orders to be broadcast
    #   with the broadcast() hint. Get the physical plan as a string via:
    #   joined._jdf.queryExecution().executedPlan().toString()
    joined_plan_text = None  # <-- replace this (the plan string, not the DataFrame)

    # TODO 2: build two 2-row DataFrames using Row(k=..., v=...) / Row(k=..., v2=...)
    #   where BOTH have one row with k=1 and one row with k=None (matching Lesson 5's
    #   example). Inner-join them on "k" and confirm what actually matches.
    null_key_result = None  # <-- replace this

    # TODO 3: with spark.sql.crossJoin.enabled set to "false", confirm that
    #   employees.select("emp_id").join(orders.select("order_id")) (no condition)
    #   raises AnalysisException. Set `blocked = True` if it does.
    blocked = False  # <-- set this correctly inside a try/except

    # TODO 4: regardless of the crossJoin.enabled setting, get the SAME cartesian
    #   product using the explicit .crossJoin(...) method instead, and reset
    #   spark.sql.crossJoin.enabled back to "true" afterward.
    explicit_cross_count = None  # <-- replace this

    # ---- self-check ----
    print(f"BroadcastHashJoin present: {'BroadcastHashJoin' in joined_plan_text}")
    assert "BroadcastHashJoin" in joined_plan_text, "expected a broadcast hash join in the plan"

    null_key_rows = [r.asDict() for r in null_key_result.collect()]
    print(f"null_key_result = {null_key_rows}")
    assert null_key_rows == [{"k": 1, "v": "a", "v2": "x"}], null_key_rows

    print(f"blocked = {blocked}")
    assert blocked is True, "expected AnalysisException with crossJoin.enabled=false"

    print(f"explicit_cross_count = {explicit_cross_count}")
    assert explicit_cross_count == 225, f"expected 15*15=225, got {explicit_cross_count}"

    print("\nAll checks passed!")
    spark.stop()


if __name__ == "__main__":
    main()
