"""
Driver for Capstone 2 -- runs solution.py stage1 then stage2 as two genuinely
separate Python processes/JVMs sharing the same checkpoint, then verifies the
final Delta tables. Run:

    python 15-capstone-projects/capstone-2-streaming-order-monitor/run_capstone2.py
"""

import os
import sys
import shutil
import subprocess
import tempfile

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, count as spark_count

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SOLUTION = os.path.join(SCRIPT_DIR, "solution.py")


def run_stage(stage, input_dir, checkpoint_dir, silver_dir, dead_letter_dir):
    result = subprocess.run(
        [sys.executable, SOLUTION, stage, input_dir, checkpoint_dir, silver_dir, dead_letter_dir],
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )
    print(f"--- {stage} stdout (filtered) ---")
    for line in result.stdout.splitlines():
        if any(k in line for k in ("STAGE", "silver row count", "dead_letter row count")):
            print(line)
    if result.returncode != 0:
        print(f"--- {stage} STDERR (last 30 lines) ---")
        print("\n".join(result.stderr.splitlines()[-30:]))
        raise RuntimeError(f"{stage} failed with exit code {result.returncode}")


def main():
    root = tempfile.mkdtemp(prefix="capstone2_")
    input_dir = os.path.join(root, "input")
    checkpoint_dir = os.path.join(root, "checkpoint")
    silver_dir = os.path.join(root, "silver")
    dead_letter_dir = os.path.join(root, "dead_letter")
    os.makedirs(input_dir)

    run_stage("stage1", input_dir, checkpoint_dir, silver_dir, dead_letter_dir)
    run_stage("stage2", input_dir, checkpoint_dir, silver_dir, dead_letter_dir)

    builder = (
        SparkSession.builder.appName("capstone-2-verify")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    silver = spark.read.format("delta").load(silver_dir)
    dead_letter = spark.read.format("delta").load(dead_letter_dir)

    silver_count = silver.count()
    dead_letter_count = dead_letter.count()
    total_revenue = silver.agg(spark_sum("amount")).first()[0]

    print("\n=== FINAL STATE (after both stages, across two separate processes) ===")
    print(f"silver row count: {silver_count}")
    print(f"dead_letter row count: {dead_letter_count}")
    print(f"total revenue (silver): {round(total_revenue, 2)}")

    print("\n=== gold: region totals (from silver) ===")
    silver.groupBy("region").agg(
        spark_sum("amount").alias("total_amount"), spark_count("*").alias("order_count")
    ).orderBy("region").show()

    # ---- verify ----
    assert silver_count == 14, f"expected 14 clean orders (15 total - 1 bad), got {silver_count}"
    assert dead_letter_count == 1, f"expected 1 dead-lettered order, got {dead_letter_count}"
    assert round(total_revenue, 2) == 7632.97, f"expected total revenue 7632.97, got {total_revenue}"

    print("\nAll capstone 2 assertions passed -- streaming + dead-letter + checkpoint restart all verified together!")

    spark.stop()
    shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
