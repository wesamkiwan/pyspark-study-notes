"""
Solution to exercises/exercise_02.py -- read this only after attempting it yourself.
"""

import os
import sys
import glob
import shutil
import tempfile

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from delta.tables import DeltaTable


def count_data_files(table_path) -> int:
    return len(glob.glob(os.path.join(table_path, "*.parquet")))


def main() -> None:
    builder = (
        SparkSession.builder.appName("delta-exercise-02")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    root = tempfile.mkdtemp(prefix="delta_ex02_")
    table_path = os.path.join(root, "events_delta")

    for i in range(6):
        spark.createDataFrame([(i, f"user{i}", i * 1.5)], ["event_id", "user", "value"]).write.format(
            "delta"
        ).mode("append").save(table_path)

    files_before = count_data_files(table_path)
    print("data files before OPTIMIZE:", files_before)

    dt = DeltaTable.forPath(spark, table_path)

    dt.optimize().executeCompaction()

    files_after_optimize = count_data_files(table_path)
    print("data files after OPTIMIZE:", files_after_optimize)

    try:
        dt.vacuum(0)
    except Exception as e:
        print("vacuum(0) without disabling safety check ->", type(e).__name__, "-", str(e).splitlines()[0])

    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    dt.vacuum(0)

    files_after_vacuum = count_data_files(table_path)
    print("data files after VACUUM:", files_after_vacuum)
    row_count = dt.toDF().count()
    print("row count after VACUUM (should be unaffected):", row_count)

    assert files_after_optimize > files_before, (
        "expected OPTIMIZE to ADD a compacted file, not remove the old ones yet"
    )
    assert files_after_vacuum < files_after_optimize, (
        "expected VACUUM to actually reduce the file count"
    )
    assert row_count == 6, f"expected all 6 rows still readable after VACUUM, got {row_count}"

    print("\nAll checks passed!")
    spark.stop()
    shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
