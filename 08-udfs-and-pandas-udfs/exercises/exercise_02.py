"""
Exercise 2 -- a vectorized pandas_udf, and a groupBy().applyInPandas() whole-group
transformation.

Fill in every `# TODO`. Run with:

    python 08-udfs-and-pandas-udfs/exercises/exercise_02.py

Don't peek at solutions/solution_02.py until you've tried this.
"""

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pandas as pd
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
        SparkSession.builder.appName("udf-exercise-02")
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

    # TODO 1: write a pandas_udf `to_monthly(salary: pd.Series) -> pd.Series` (Lesson 4)
    #   that converts an annual salary to a monthly one (divide by 12). Do NOT add a
    #   None/NaN guard -- part of the point of this exercise is observing that pandas
    #   Series division propagates NaN on its own, unlike Lesson 3's row UDF.
    to_monthly = None  # <-- replace this with your @pandas_udf-decorated function

    with_monthly = employees.withColumn("monthly_salary", to_monthly(col("salary")))

    # TODO 2: write a function `rank_by_salary(pdf: pd.DataFrame) -> pd.DataFrame` for
    #   use with applyInPandas (Lesson 5) that adds a "dept_rank" column: each
    #   department's employees ranked by salary, descending, highest salary = rank 1.
    #   Use pandas' `.rank(ascending=False, method="min")` on the salary column.
    #   Return columns: emp_id, name, department, salary, dept_rank.
    out_schema = StructType(
        [
            StructField("emp_id", IntegerType()),
            StructField("name", StringType()),
            StructField("department", StringType()),
            StructField("salary", DoubleType()),
            StructField("dept_rank", DoubleType()),
        ]
    )
    ranked = None  # <-- replace this: employees.groupBy("department").applyInPandas(...)

    # ---- self-check ----
    mona_monthly = (
        with_monthly.filter(col("name") == "Mona Farouk").select("monthly_salary").collect()[0][
            "monthly_salary"
        ]
    )
    print(f"Mona Farouk's monthly_salary = {mona_monthly}")
    assert mona_monthly is None, f"expected NULL to propagate through division, got {mona_monthly}"

    alice_monthly = (
        with_monthly.filter(col("name") == "Alice Chen").select("monthly_salary").collect()[0][
            "monthly_salary"
        ]
    )
    print(f"Alice Chen's monthly_salary = {alice_monthly}")
    assert abs(alice_monthly - 125000 / 12) < 0.01, alice_monthly

    eng_ranks = {
        r["name"]: r["dept_rank"]
        for r in ranked.filter(col("department") == "Engineering").collect()
    }
    print(f"Engineering dept_rank = {eng_ranks}")
    assert eng_ranks["Ines Moreau"] == 1.0, eng_ranks
    assert eng_ranks["Alice Chen"] == 2.0, eng_ranks
    assert eng_ranks["Carol Nunez"] == 3.0, eng_ranks
    assert eng_ranks["Bob Okafor"] == 4.0, eng_ranks
    assert eng_ranks["Jamal Smith"] == 5.0, eng_ranks
    assert eng_ranks["Mona Farouk"] is None, (
        "expected NULL -- pandas rank() gives NaN for a NULL salary, and Spark converts "
        "that NaN back to NULL when it lands in a DoubleType column"
    )

    print("\nAll checks passed!")
    spark.stop()


if __name__ == "__main__":
    main()
