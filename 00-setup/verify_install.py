"""
Sanity check: confirms Java + PySpark + Py4J are all wired up correctly.

Run:
    python 00-setup/verify_install.py
"""

import os
import sys

import pyspark
from pyspark.sql import SparkSession

# Spark's executors spawn Python worker processes using whatever "python" resolves to on
# PATH — not necessarily the interpreter you launched this script with. On Windows this
# often hits the Microsoft Store's "python.exe" alias stub instead of your venv, which
# fails silently deep inside the JVM ("Python worker failed to connect back"). Pinning
# these two env vars to sys.executable makes it unambiguous everywhere.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def main() -> None:
    print(f"PySpark version: {pyspark.__version__}")

    spark = (
        SparkSession.builder.appName("verify-install")
        .master("local[*]")  # use all local CPU cores as "executors"
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")  # Spark's default logging is very noisy

    print(f"Spark UI available at: {spark.sparkContext.uiWebUrl}")

    df = spark.createDataFrame(
        [("alice", 29), ("bob", 31), ("carol", 27)],
        schema=["name", "age"],
    )

    print("\nSample DataFrame:")
    df.show()

    print("Schema:")
    df.printSchema()

    row_count = df.count()  # this triggers an actual Spark job
    assert row_count == 3, f"expected 3 rows, got {row_count}"
    print(f"\nSetup OK - counted {row_count} rows via a real Spark job.")

    spark.stop()


if __name__ == "__main__":
    main()
