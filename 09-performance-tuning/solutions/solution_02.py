"""
Solution to exercises/exercise_02.py -- read this only after attempting it yourself.
"""

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark import StorageLevel
from pyspark.sql import SparkSession

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")


def main() -> None:
    spark = (
        SparkSession.builder.appName("perf-exercise-02")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    employees_path = os.path.join(DATA_DIR, "employees.csv")

    disk_only_df = spark.read.csv(employees_path, header=True, inferSchema=True).persist(
        StorageLevel.DISK_ONLY
    )
    disk_only_df.count()

    # ---- self-check 1 ----
    assert disk_only_df.storageLevel.useMemory is False, "DISK_ONLY should not use memory"
    assert disk_only_df.storageLevel.useDisk is True, "DISK_ONLY should use disk"
    print(f"DISK_ONLY storageLevel = {disk_only_df.storageLevel}")

    disk_only_df.unpersist()

    spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")
    default_partitions = spark.read.csv(
        employees_path, header=True, inferSchema=True
    ).rdd.getNumPartitions()

    spark.conf.set("spark.sql.files.maxPartitionBytes", "400")
    small_partitions = spark.read.csv(
        employees_path, header=True, inferSchema=True
    ).rdd.getNumPartitions()

    # ---- self-check 2 ----
    print(f"partitions at default maxPartitionBytes = {default_partitions}")
    print(f"partitions at maxPartitionBytes=400 = {small_partitions}")
    assert default_partitions == 1, f"expected 1 partition at the default, got {default_partitions}"
    assert small_partitions == 2, f"expected 2 partitions at maxPartitionBytes=400, got {small_partitions}"

    print("\nAll checks passed!")
    spark.stop()


if __name__ == "__main__":
    main()
