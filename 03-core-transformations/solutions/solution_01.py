"""
Solution to exercises/exercise_01.py -- read this only after attempting it yourself.
"""

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, round as spark_round, when
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
        SparkSession.builder.appName("core-transformations-exercise-01")
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

    high_earning_sales = employees.filter(
        (col("department") == "Sales") & (col("salary") > 70000)
    )

    tiered = employees.withColumn(
        "salary_band",
        when(col("salary") >= 100000, "Senior")
        .when(col("salary") >= 70000, "Mid")
        .when(col("salary").isNull(), "Unknown")
        .otherwise("Junior"),
    )

    dept_stats = (
        employees.groupBy("department")
        .agg(
            count("*").alias("headcount"),
            spark_round(avg("salary"), 2).alias("avg_salary"),
        )
        .orderBy(col("avg_salary").desc())
    )

    # ---- self-check ----
    high_count = high_earning_sales.count()
    print(f"high_earning_sales count = {high_count}")
    assert high_count == 2, f"expected 2 rows, got {high_count}"
    high_names = {r["name"] for r in high_earning_sales.collect()}
    assert high_names == {"David Kim", "Farid Haidari"}, f"unexpected names: {high_names}"

    band_counts = {r["salary_band"]: r["count"] for r in tiered.groupBy("salary_band").count().collect()}
    print(f"band_counts = {band_counts}")
    assert band_counts == {"Senior": 3, "Mid": 7, "Junior": 4, "Unknown": 1}, band_counts

    stats_rows = [r.asDict() for r in dept_stats.collect()]
    print(f"dept_stats = {stats_rows}")
    assert stats_rows[0]["department"] == "Engineering", "expected Engineering to have the highest avg_salary"
    assert stats_rows[0]["headcount"] == 6 and stats_rows[0]["avg_salary"] == 109200.0
    assert stats_rows[-1]["department"] == "Marketing", "expected Marketing to have the lowest avg_salary"

    print("\nAll checks passed!")
    spark.stop()


if __name__ == "__main__":
    main()
