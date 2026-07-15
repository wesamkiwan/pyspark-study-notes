"""
Exercise 1 -- MERGE as a real upsert, verified: an update to an existing row and an
insert of a brand new row, both applied atomically in one MERGE.

Fill in every `# TODO`. Run with:

    python 11-delta-lake/exercises/exercise_01.py

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
from delta.tables import DeltaTable


def main() -> None:
    builder = (
        SparkSession.builder.appName("delta-exercise-01")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    root = tempfile.mkdtemp(prefix="delta_ex01_")
    table_path = os.path.join(root, "customers_delta")

    target = spark.createDataFrame(
        [(1, "alice", "gold", 100), (2, "bob", "silver", 50), (3, "carol", "silver", 75)],
        ["cust_id", "name", "tier", "points"],
    )
    target.write.format("delta").mode("overwrite").save(table_path)

    # bob's points change (50 -> 90), and a brand new customer (dave) arrives
    updates = spark.createDataFrame(
        [(2, "bob", "silver", 90), (4, "dave", "bronze", 10)],
        ["cust_id", "name", "tier", "points"],
    )

    # TODO 1: get a DeltaTable handle on `table_path` via DeltaTable.forPath(...)
    dt = None  # <-- replace this

    # TODO 2: build and .execute() a MERGE:
    #   - join condition: target.cust_id = source.cust_id  (alias target "t", source "s")
    #   - whenMatchedUpdateAll()
    #   - whenNotMatchedInsertAll()

    result = {r["cust_id"]: r["points"] for r in dt.toDF().collect()}
    print(result)

    # ---- self-check ----
    assert result == {1: 100, 2: 90, 3: 75, 4: 10}, f"unexpected MERGE result: {result}"

    print("\nAll checks passed!")
    spark.stop()
    shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
