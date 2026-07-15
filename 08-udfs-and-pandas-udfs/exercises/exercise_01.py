"""
Exercise 1 -- basic UDF registration, an explicit None guard, and SQL registration.

Fill in every `# TODO`. Run with:

    python 08-udfs-and-pandas-udfs/exercises/exercise_01.py

Don't peek at solutions/solution_01.py until you've tried this.
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
        SparkSession.builder.appName("udf-exercise-01")
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

    # TODO 1: write a Python UDF `salary_band(salary)` -> StringType, with an explicit
    #   None guard (Lesson 3), that returns:
    #     "Unknown"  if salary is None
    #     "Junior"   if salary <  70000
    #     "Mid"      if 70000 <= salary < 100000
    #     "Senior"   if 100000 <= salary < 130000
    #     "Lead"     if salary >= 130000
    #   Declare returnType=StringType() explicitly (Lesson 2).
    salary_band = None  # <-- replace this with your @udf-decorated function

    banded = employees.withColumn("band", salary_band(col("salary")))

    # TODO 2: register `salary_band` for use from SQL as "salary_band_sql", create a
    #   temp view called "employees_view" from `banded`, then use spark.sql(...) to
    #   count employees per band, ordered by band name.
    band_counts = None  # <-- replace this (columns: band, cnt), ordered by band

    # ---- self-check ----
    mona_band = banded.filter(col("name") == "Mona Farouk").select("band").collect()[0]["band"]
    print(f"Mona Farouk's band = {mona_band}")
    assert mona_band == "Unknown", f"expected Unknown for a NULL salary, got {mona_band}"

    counts_rows = [(r["band"], r["cnt"]) for r in band_counts.collect()]
    print(f"band_counts = {counts_rows}")
    assert counts_rows == [
        ("Junior", 4),
        ("Lead", 1),
        ("Mid", 7),
        ("Senior", 2),
        ("Unknown", 1),
    ], counts_rows

    print("\nAll checks passed!")
    spark.stop()


if __name__ == "__main__":
    main()
