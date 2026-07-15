"""
Solution to exercises/exercise_02.py -- read this only after attempting it yourself.
"""

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf
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

    @pandas_udf("double")
    def to_monthly(salary: pd.Series) -> pd.Series:
        return salary / 12

    with_monthly = employees.withColumn("monthly_salary", to_monthly(col("salary")))

    out_schema = StructType(
        [
            StructField("emp_id", IntegerType()),
            StructField("name", StringType()),
            StructField("department", StringType()),
            StructField("salary", DoubleType()),
            StructField("dept_rank", DoubleType()),
        ]
    )

    def rank_by_salary(pdf: pd.DataFrame) -> pd.DataFrame:
        pdf["dept_rank"] = pdf["salary"].rank(ascending=False, method="min")
        return pdf[["emp_id", "name", "department", "salary", "dept_rank"]]

    ranked = employees.groupBy("department").applyInPandas(rank_by_salary, schema=out_schema)

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
