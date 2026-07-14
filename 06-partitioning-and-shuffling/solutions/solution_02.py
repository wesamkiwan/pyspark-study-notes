"""
Solution to exercises/exercise_02.py -- read this only after attempting it yourself.
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

    spark.conf.set("spark.sql.adaptive.enabled", "false")
    partitions_aqe_off = employees.groupBy("department").count().rdd.getNumPartitions()

    spark.conf.set("spark.sql.adaptive.enabled", "true")
    partitions_aqe_on = employees.groupBy("department").count().rdd.getNumPartitions()

    spark.conf.set("spark.sql.shuffle.partitions", "8")
    spark.conf.set("spark.sql.adaptive.enabled", "false")

    N = 100_000
    df = spark.range(N).withColumn(
        "key",
        when(col("id") < N * 0.9, "HOT").otherwise(
            concat_ws("_", lit("key"), (col("id") % 999).cast("string"))
        ),
    ).withColumn("value", lit(1))

    sizes_before = df.repartition(8, "key").rdd.glom().map(len).collect()
    max_partition_before_salting = max(sizes_before)

    NUM_SALTS = 8
    salted = df.withColumn("salt", floor(rand(seed=42) * NUM_SALTS)).withColumn(
        "salted_key", concat_ws("_", col("key"), col("salt"))
    )
    sizes_after = salted.repartition(16, "salted_key").rdd.glom().map(len).collect()
    max_partition_after_salting = max(sizes_after)

    plain_hot_total = (
        df.groupBy("key").agg(spark_sum("value").alias("total")).filter(col("key") == "HOT").collect()[0]["total"]
    )
    stage1 = salted.groupBy("key", "salt").agg(spark_sum("value").alias("partial_total"))
    stage2 = stage1.groupBy("key").agg(spark_sum("partial_total").alias("total"))
    salted_hot_total = stage2.filter(col("key") == "HOT").collect()[0]["total"]

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
