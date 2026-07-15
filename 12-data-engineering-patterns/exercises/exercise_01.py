"""
Exercise 1 -- idempotent loads, verified: a naive append-based job run twice
doubles its output; a replaceWhere overwrite-based job run twice does not.

Fill in every `# TODO`. Run with:

    python 12-data-engineering-patterns/exercises/exercise_01.py

Don't peek at solutions/solution_01.py until you've tried this.
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

    # TODO 1: write daily_batch() to `table_path` in Delta format using:
    #   - mode("overwrite")
    #   - option("replaceWhere", "load_date = '2024-01-01'")
    #   - partitionBy("load_date")

    # TODO 2: run the EXACT SAME write again (simulating a job retry) -- same code as TODO 1

    row_count = spark.read.format("delta").load(table_path).count()
    print(f"row count after running the load TWICE: {row_count}")

    # ---- self-check ----
    assert row_count == 2, f"expected the idempotent job to stay at 2 rows after 2 runs, got {row_count}"

    print("\nAll checks passed!")
    spark.stop()
    shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
