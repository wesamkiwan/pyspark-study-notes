"""
Capstone 2 solution -- Streaming Order Monitor.

Simulates orders arriving as a stream of small CSV files (Module 10's file source),
splitting each micro-batch into clean vs dead-lettered rows (Module 12) via
foreachBatch (Module 10 Lesson 5), writing both idempotently to Delta (Module 11),
with checkpoint-based fault tolerance verified across two genuinely separate process
invocations (Module 10 Lesson 4).

Run TWICE, as two separate processes, sharing the same directories:

    python solution.py stage1 <input_dir> <checkpoint_dir> <silver_dir> <dead_letter_dir>
    python solution.py stage2 <input_dir> <checkpoint_dir> <silver_dir> <dead_letter_dir>

See run_capstone2.py for a driver script that does both stages and prints final results.
"""

import os
import sys
import time

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType, DateType,
)

REPO_ROOT = r"C:\Users\wesam\.claude\wesam\Pyspark for data engineers\pyspark-study-notes"
DATA_DIR = os.path.join(REPO_ROOT, "data")

order_schema = StructType(
    [
        StructField("order_id", IntegerType()),
        StructField("emp_id", IntegerType()),
        StructField("product", StringType()),
        StructField("category", StringType()),
        StructField("amount", DoubleType()),
        StructField("order_date", DateType()),
        StructField("region", StringType()),
    ]
)


def write_order_file(input_dir, name, row_dict):
    path = os.path.join(input_dir, name)
    tmp = path + ".tmp"
    cols = ["order_id", "emp_id", "product", "category", "amount", "order_date", "region"]
    with open(tmp, "w") as f:
        f.write(",".join(cols) + "\n")
        f.write(",".join(str(row_dict[c]) for c in cols) + "\n")
    os.rename(tmp, path)


def wait_for_batch_count(query, n, timeout=30):
    start = time.time()
    while len(query.recentProgress) < n and time.time() - start < timeout:
        time.sleep(0.2)
    time.sleep(0.5)
    return len(query.recentProgress)


def main():
    stage = sys.argv[1]
    input_dir, checkpoint_dir, silver_dir, dead_letter_dir = sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]

    builder = (
        SparkSession.builder.appName(f"capstone-2-{stage}")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    known_emp_ids = [
        r["emp_id"]
        for r in spark.read.csv(os.path.join(DATA_DIR, "employees.csv"), header=True, inferSchema=True)
        .select("emp_id")
        .collect()
    ]

    def process_batch(batch_df, batch_id):
        batch_df = batch_df.cache()
        bad = batch_df.filter((col("amount") <= 0) | (~col("emp_id").isin(known_emp_ids)))
        good = batch_df.join(bad.select("order_id"), on="order_id", how="left_anti")

        if bad.count() > 0:
            bad.write.format("delta").mode("append").save(dead_letter_dir)
        if good.count() > 0:
            good.write.format("delta").mode("append").save(silver_dir)
        batch_df.unpersist()

    stream_df = spark.readStream.schema(order_schema).option("header", "true").csv(input_dir)
    query = (
        stream_df.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", checkpoint_dir)
        .trigger(availableNow=True)
        .start()
    )

    if stage == "stage1":
        import csv as csv_module

        with open(os.path.join(DATA_DIR, "orders.csv")) as f:
            rows = list(csv_module.DictReader(f))

        # feed the first 10 orders as the "stream so far"
        for i, row in enumerate(rows[:10]):
            write_order_file(input_dir, f"order_{row['order_id']}.csv", row)

        query.awaitTermination()
        print(f"STAGE1: batches = {len(query.recentProgress)}")

    elif stage == "stage2":
        import csv as csv_module

        with open(os.path.join(DATA_DIR, "orders.csv")) as f:
            rows = list(csv_module.DictReader(f))

        # a BRAND NEW process/JVM, same checkpoint -- feed the REMAINING 5 orders
        for row in rows[10:]:
            write_order_file(input_dir, f"order_{row['order_id']}.csv", row)

        query.awaitTermination()
        print(f"STAGE2: batches processed THIS RUN = {len(query.recentProgress)}")

    silver_count = spark.read.format("delta").load(silver_dir).count() if os.path.exists(silver_dir) else 0
    dead_letter_count = (
        spark.read.format("delta").load(dead_letter_dir).count() if os.path.exists(dead_letter_dir) else 0
    )
    print(f"[{stage}] silver row count: {silver_count}")
    print(f"[{stage}] dead_letter row count: {dead_letter_count}")

    spark.stop()


if __name__ == "__main__":
    main()
