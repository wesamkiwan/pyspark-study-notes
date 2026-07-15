"""
Solution to exercises/exercise_01.py -- read this only after attempting it yourself.
"""

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, udf
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

    @udf(returnType=DoubleType())
    def withholding_uncached(salary):
        calls_uncached.add(1)
        if salary is None:
            return None
        return salary * 0.22

    uncached_df = load_employees().withColumn("withholding", withholding_uncached(col("salary")))

    uncached_df.agg(spark_sum("withholding")).collect()
    uncached_df.agg(spark_sum("withholding")).collect()

    print(f"calls with NO cache, after 2 actions = {calls_uncached.value}")

    # ---- run WITH caching ----
    calls_cached = sc.accumulator(0)

    @udf(returnType=DoubleType())
    def withholding_cached(salary):
        calls_cached.add(1)
        if salary is None:
            return None
        return salary * 0.22

    cached_df = load_employees().withColumn("withholding", withholding_cached(col("salary"))).cache()

    cached_df.agg(spark_sum("withholding")).collect()
    cached_df.agg(spark_sum("withholding")).collect()

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
