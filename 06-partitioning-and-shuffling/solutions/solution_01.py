"""
Solution to exercises/exercise_01.py -- read this only after attempting it yourself.
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

    input_partition_count = employees.rdd.getNumPartitions()

    hashed = employees.repartition(4, "department").withColumn("p", spark_partition_id())
    hashed_dept_partition_counts = (
        hashed.select("department", "p").distinct().groupBy("department").count()
    )

    roundrobin = employees.repartition(4).withColumn("p", spark_partition_id())
    roundrobin_dept_partition_counts = (
        roundrobin.select("department", "p").distinct().groupBy("department").count()
    )

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
