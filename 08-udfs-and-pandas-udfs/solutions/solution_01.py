"""
Solution to exercises/exercise_01.py -- read this only after attempting it yourself.
"""

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
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

    @udf(returnType=StringType())
    def salary_band(salary):
        if salary is None:
            return "Unknown"
        if salary < 70000:
            return "Junior"
        if salary < 100000:
            return "Mid"
        if salary < 130000:
            return "Senior"
        return "Lead"

    banded = employees.withColumn("band", salary_band(col("salary")))

    spark.udf.register("salary_band_sql", salary_band)
    banded.createOrReplaceTempView("employees_view")
    band_counts = spark.sql(
        """
        SELECT band, COUNT(*) AS cnt
        FROM employees_view
        GROUP BY band
        ORDER BY band
        """
    )

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
