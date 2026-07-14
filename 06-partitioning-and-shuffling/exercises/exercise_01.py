"""
Exercise 1 -- reading a DataFrame's partition count, and the difference between
round-robin repartition(n) and hash-partitioning repartition(n, col).

Fill in every `# TODO`. Run with:

    python 06-partitioning-and-shuffling/exercises/exercise_01.py

Don't peek at solutions/solution_01.py until you've tried this.
"""

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id
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
        SparkSession.builder.appName("partitioning-exercise-01")
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

    # TODO 1: how many partitions does `employees` have right after reading?
    input_partition_count = None  # <-- replace this (an int)

    # TODO 2: repartition `employees` into 4 partitions HASHED BY "department"
    #   (repartition(4, "department")), tag each row with its physical partition via
    #   spark_partition_id() aliased "p", then build a DataFrame of (department, count
    #   of DISTINCT partitions that department appears in). Every department should
    #   appear in exactly 1 partition.
    hashed_dept_partition_counts = None  # <-- replace this

    # TODO 3: do the same thing but with plain repartition(4) (round-robin, no column).
    #   At least one department should now span MORE than 1 partition.
    roundrobin_dept_partition_counts = None  # <-- replace this

    # ---- self-check ----
    print(f"input_partition_count = {input_partition_count}")
    assert input_partition_count == 1, f"expected 1, got {input_partition_count}"

    hashed_counts = {r["department"]: r["count"] for r in hashed_dept_partition_counts.collect()}
    print(f"hashed_dept_partition_counts = {hashed_counts}")
    assert all(c == 1 for c in hashed_counts.values()), (
        f"every department should span exactly 1 partition under hash repartitioning, got {hashed_counts}"
    )

    rr_counts = {r["department"]: r["count"] for r in roundrobin_dept_partition_counts.collect()}
    print(f"roundrobin_dept_partition_counts = {rr_counts}")
    assert any(c > 1 for c in rr_counts.values()), (
        "expected at least one department to span multiple partitions under round-robin repartition(4)"
    )

    print("\nAll checks passed!")
    spark.stop()


if __name__ == "__main__":
    main()
