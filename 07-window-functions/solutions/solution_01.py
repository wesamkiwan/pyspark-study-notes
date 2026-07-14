"""
Solution to exercises/exercise_01.py -- read this only after attempting it yourself.
"""

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dense_rank, rank, row_number
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")


def main() -> None:
    spark = (
        SparkSession.builder.appName("window-exercise-01")
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

    w = Window.partitionBy("department").orderBy(col("salary").desc(), col("emp_id"))

    ranked = employees.select(
        "department",
        "name",
        "salary",
        row_number().over(w).alias("rn"),
        rank().over(w).alias("rnk"),
        dense_rank().over(w).alias("drnk"),
    )

    top2_per_dept = ranked.filter(col("rn") <= 2)

    # ---- self-check ----
    ranked_rows = [r.asDict() for r in ranked.orderBy("department", "rn").collect()]
    print(f"ranked (first 3) = {ranked_rows[:3]}")
    assert len(ranked_rows) == 15, f"expected 15 employees, got {len(ranked_rows)}"
    assert all(r["rn"] == r["rnk"] == r["drnk"] for r in ranked_rows), (
        "expected rn/rnk/drnk to match -- no ties exist within a department in this data"
    )

    top2_names = [(r["department"], r["name"]) for r in top2_per_dept.orderBy("department", "rn").collect()]
    print(f"top2_per_dept = {top2_names}")
    assert top2_names == [
        ("Engineering", "Ines Moreau"),
        ("Engineering", "Alice Chen"),
        ("Finance", "Noah Bergstrom"),
        ("Finance", "Olivia Tran"),
        ("Marketing", "Grace Lin"),
        ("Marketing", "Hassan Ali"),
        ("Sales", "Farid Haidari"),
        ("Sales", "David Kim"),
    ], top2_names

    print("\nAll checks passed!")
    spark.stop()


if __name__ == "__main__":
    main()
