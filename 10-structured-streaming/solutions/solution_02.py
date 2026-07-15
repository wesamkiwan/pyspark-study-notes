"""
Solution to exercises/exercise_02.py -- read this only after attempting it yourself.
"""

import os
import sys
import time
import shutil
import tempfile

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, count as spark_count, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

schema = StructType(
    [
        StructField("order_id", IntegerType()),
        StructField("customer", StringType()),
        StructField("event_time", TimestampType()),
    ]
)


def ts(seconds: int) -> str:
    return f"2024-01-01 00:{seconds // 60:02d}:{seconds % 60:02d}"


def write_csv(input_dir, name, order_id, customer, event_seconds):
    path = os.path.join(input_dir, name)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        f.write("order_id,customer,event_time\n")
        f.write(f"{order_id},{customer},{ts(event_seconds)}\n")
    os.rename(tmp, path)


def wait_for_batch_count(query, n, timeout=20):
    start = time.time()
    while len(query.recentProgress) < n and time.time() - start < timeout:
        time.sleep(0.2)
    time.sleep(0.5)
    return len(query.recentProgress)


def main() -> None:
    spark = (
        SparkSession.builder.appName("stream-exercise-02")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    input_dir = tempfile.mkdtemp(prefix="stream_ex02_")
    stream_df = spark.readStream.schema(schema).option("header", "true").csv(input_dir)

    windowed = (
        stream_df.withWatermark("event_time", "5 seconds")
        .groupBy(window(col("event_time"), "10 seconds"))
        .agg(spark_count("*").alias("cnt"), spark_sum("order_id").alias("total"))
    )

    query = (
        windowed.writeStream.format("memory")
        .queryName("ex02_windows")
        .outputMode("complete")
        .start()
    )

    write_csv(input_dir, "b1.csv", 1, "a", 0)
    wait_for_batch_count(query, 1)

    write_csv(input_dir, "b2.csv", 2, "b", 12)
    wait_for_batch_count(query, 2)

    write_csv(input_dir, "b3.csv", 100, "late-but-ok", 3)
    wait_for_batch_count(query, 3)

    write_csv(input_dir, "b4.csv", 4, "d", 25)
    wait_for_batch_count(query, 4)

    write_csv(input_dir, "b5.csv", 999, "late-and-dropped", 4)
    wait_for_batch_count(query, 5)

    rows = {
        r["w_start"]: (r["cnt"], r["total"])
        for r in spark.sql(
            "select window.start as w_start, cnt, total from ex02_windows"
        ).collect()
    }
    print(rows)

    from datetime import datetime

    w0 = datetime(2024, 1, 1, 0, 0, 0)
    w10 = datetime(2024, 1, 1, 0, 0, 10)
    w20 = datetime(2024, 1, 1, 0, 0, 20)

    assert rows[w0] == (2, 101), (
        f"expected window[0,10) to include the late-but-ok row (cnt=2, total=1+100=101), got {rows[w0]}"
    )
    assert rows[w10] == (1, 2), f"expected window[10,20) untouched by any late row, got {rows[w10]}"
    assert rows[w20] == (1, 4), f"expected window[20,30) with just the t=25s row, got {rows[w20]}"
    assert rows[w0][1] != 1100, "the late-and-dropped row's order_id (999) must NOT show up anywhere"

    print("\nAll checks passed!")
    query.stop()
    spark.stop()
    shutil.rmtree(input_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
