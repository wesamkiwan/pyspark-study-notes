"""
Exercise 2 -- the spark.sql.shuffle.partitions default vs AQE coalescing, and a full
skew-detection-and-salting walkthrough on a synthetic skewed dataset.

Fill in every `# TODO`. Run with:

    python 06-partitioning-and-shuffling/exercises/exercise_02.py

Don't peek at solutions/solution_02.py until you've tried this.
"""

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    floor,
    lit,
    rand,
    sum as spark_sum,
    when,
)
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
    spark = SparkSession.builder.appName("partitioning-exercise-02").master("local[*]").getOrCreate()
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

    # TODO 1: with spark.sql.adaptive.enabled set to "false", group `employees` by
    #   department and get the resulting partition count. Should be the raw 200 default.
    partitions_aqe_off = None  # <-- replace this (an int)

    # TODO 2: set spark.sql.adaptive.enabled back to "true" and repeat the exact same
    #   groupBy. AQE should coalesce this down dramatically.
    partitions_aqe_on = None  # <-- replace this (an int)

    # TODO 3: set spark.sql.shuffle.partitions to "8" and spark.sql.adaptive.enabled
    #   to "false" (isolate the skew, don't let AQE's skew handling mask it for this
    #   exercise). Build the same synthetic skewed dataset as Lesson 5:
    #     N = 100_000 rows via spark.range(N)
    #     "key" = "HOT" for the first 90% of rows (id < N * 0.9),
    #             else "key_<id % 999>"
    #     "value" = 1
    #   Then repartition(8, "key") and get the max partition size via
    #   .rdd.glom().map(len).collect().
    max_partition_before_salting = None  # <-- replace this (an int)

    # TODO 4: salt the hot key with NUM_SALTS = 8 (a "salt" column via
    #   floor(rand(seed=42) * NUM_SALTS), then a "salted_key" column concatenating
    #   key and salt). Repartition(16, "salted_key") and get the new max partition size.
    max_partition_after_salting = None  # <-- replace this (an int)

    # TODO 5: verify correctness -- the plain groupBy("key") total for "HOT" must equal
    #   the two-stage salted total (groupBy("key","salt") then groupBy("key") again).
    plain_hot_total = None  # <-- replace this (an int)
    salted_hot_total = None  # <-- replace this (an int)

    # ---- self-check ----
    print(f"partitions_aqe_off = {partitions_aqe_off}")
    assert partitions_aqe_off == 200, f"expected 200, got {partitions_aqe_off}"

    print(f"partitions_aqe_on = {partitions_aqe_on}")
    assert partitions_aqe_on < 50, f"expected AQE to coalesce well under 50, got {partitions_aqe_on}"

    print(f"max_partition_before_salting = {max_partition_before_salting}")
    print(f"max_partition_after_salting = {max_partition_after_salting}")
    assert max_partition_before_salting > 80_000, (
        f"expected the unsalted HOT partition to hold the vast majority of 90,000 rows, "
        f"got {max_partition_before_salting}"
    )
    assert max_partition_after_salting < max_partition_before_salting / 4, (
        "expected salting to cut the largest partition down by at least 4x, "
        f"got before={max_partition_before_salting}, after={max_partition_after_salting}"
    )

    print(f"plain_hot_total = {plain_hot_total}, salted_hot_total = {salted_hot_total}")
    assert plain_hot_total == 90_000, f"expected 90000, got {plain_hot_total}"
    assert plain_hot_total == salted_hot_total, "salting must not change the aggregated result"

    print("\nAll checks passed!")
    spark.stop()


if __name__ == "__main__":
    main()
