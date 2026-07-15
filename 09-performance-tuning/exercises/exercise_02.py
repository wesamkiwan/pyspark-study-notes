"""
Exercise 2 -- choosing an explicit storage level, and verifying
spark.sql.files.maxPartitionBytes controls input-read partition count.

Fill in every `# TODO`. Run with:

    python 09-performance-tuning/exercises/exercise_02.py

Don't peek at solutions/solution_02.py until you've tried this.
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

    # TODO 1: read employees.csv (header=True, inferSchema=True is fine here),
    #   then persist it explicitly with StorageLevel.DISK_ONLY (Lesson 3) --
    #   NOT the default .cache(). Trigger one action so the storage level is
    #   actually applied.
    disk_only_df = None  # <-- replace this

    # ---- self-check 1 ----
    assert disk_only_df.storageLevel.useMemory is False, "DISK_ONLY should not use memory"
    assert disk_only_df.storageLevel.useDisk is True, "DISK_ONLY should use disk"
    print(f"DISK_ONLY storageLevel = {disk_only_df.storageLevel}")

    disk_only_df.unpersist()

    # TODO 2: read employees.csv (692 bytes on disk) TWICE -- once with
    #   spark.sql.files.maxPartitionBytes left at the library default
    #   ("134217728"), once after setting it to "400" (smaller than the file)
    #   via spark.conf.set(...). Record df.rdd.getNumPartitions() each time.
    default_partitions = None  # <-- replace this
    small_partitions = None  # <-- replace this

    # ---- self-check 2 ----
    print(f"partitions at default maxPartitionBytes = {default_partitions}")
    print(f"partitions at maxPartitionBytes=400 = {small_partitions}")
    assert default_partitions == 1, f"expected 1 partition at the default, got {default_partitions}"
    assert small_partitions == 2, f"expected 2 partitions at maxPartitionBytes=400, got {small_partitions}"

    print("\nAll checks passed!")
    spark.stop()


if __name__ == "__main__":
    main()
