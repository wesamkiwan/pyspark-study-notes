"""
Solution to exercises/exercise_01.py -- read this only after attempting it yourself.
"""

import os
import sys
import shutil
import tempfile

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def main() -> None:
    builder = (
        SparkSession.builder.appName("dep-exercise-01")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    root = tempfile.mkdtemp(prefix="dep_ex01_")
    table_path = os.path.join(root, "overwrite_table")

    def daily_batch():
        return spark.createDataFrame(
            [(1, "alice", 10.0, "2024-01-01"), (2, "bob", 20.0, "2024-01-01")],
            ["order_id", "customer", "amount", "load_date"],
        )

    def load():
        (
            daily_batch()
            .write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", "load_date = '2024-01-01'")
            .partitionBy("load_date")
            .save(table_path)
        )

    load()
    load()  # a retry -- same input, same code

    row_count = spark.read.format("delta").load(table_path).count()
    print(f"row count after running the load TWICE: {row_count}")

    assert row_count == 2, f"expected the idempotent job to stay at 2 rows after 2 runs, got {row_count}"

    print("\nAll checks passed!")
    spark.stop()
    shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
