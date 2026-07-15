"""
Exercise 1 -- verifying cache()'s recompute-avoidance with an accumulator-based
call counter, on a fresh UDF/dataset from Lesson 2's pattern.

Fill in every `# TODO`. Run with:

    python 09-performance-tuning/exercises/exercise_01.py

Don't peek at solutions/solution_01.py until you've tried this.
"""

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
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
        SparkSession.builder.appName("perf-exercise-01")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    sc = spark.sparkContext

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

    def load_employees():
        return spark.read.csv(
            os.path.join(DATA_DIR, "employees.csv"), header=True, schema=emp_schema
        )

    # ---- run WITHOUT caching ----
    calls_uncached = sc.accumulator(0)

    # TODO 1: write a UDF `withholding(salary)` -> DoubleType that increments
    #   `calls_uncached` by 1 every call (guard: if salary is None, return None
    #   without incrementing further logic beyond the guard -- Lesson 3 of
    #   Module 08's null-handling rule still applies here), otherwise returns
    #   salary * 0.22.
    withholding_uncached = None  # <-- replace this with your @udf-decorated function

    uncached_df = load_employees().withColumn("withholding", withholding_uncached(col("salary")))

    # TODO 2: run TWO actions against `uncached_df` that both require the
    #   "withholding" column (e.g. .agg(spark_sum("withholding")).collect()),
    #   with no caching in between.

    print(f"calls with NO cache, after 2 actions = {calls_uncached.value}")

    # ---- run WITH caching ----
    calls_cached = sc.accumulator(0)

    # TODO 3: same idea as TODO 1, but increment `calls_cached` instead.
    withholding_cached = None  # <-- replace this with your @udf-decorated function

    # TODO 4: build the same withholding column on a FRESH read of employees,
    #   call `.cache()` on it, then run the SAME two actions as TODO 2 against
    #   the cached DataFrame.
    cached_df = None  # <-- replace this

    print(f"calls WITH cache, after 2 actions = {calls_cached.value}")

    # ---- self-check ----
    assert calls_uncached.value == 30, (
        f"expected 30 (15 employees x 2 uncached passes), got {calls_uncached.value}"
    )
    assert calls_cached.value == 15, (
        f"expected 15 (computed once, reused on the 2nd action), got {calls_cached.value}"
    )

    print("\nAll checks passed!")
    spark.stop()


if __name__ == "__main__":
    main()
