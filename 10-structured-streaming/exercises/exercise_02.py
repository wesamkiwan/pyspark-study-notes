"""
Exercise 2 -- watermarks and windowed aggregations: verify exactly which late-arriving
row gets merged into an already-open window, and which one gets silently dropped once
its window has closed for good.

Fill in every `# TODO`. Run with:

    python 10-structured-streaming/exercises/exercise_02.py

Don't peek at solutions/solution_02.py until you've tried this.
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
    time.sleep(0.5)  # let the memory sink settle
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

    # TODO 1: build `windowed` --
    #   - withWatermark("event_time", "5 seconds")
    #   - groupBy a 10-second tumbling window() over "event_time"
    #   - agg a count(*) aliased "cnt" and a sum("order_id") aliased "total"
    windowed = None  # <-- replace this

    # TODO 2: start a writeStream query on `windowed`:
    #   - format("memory"), queryName("ex02_windows")
    #   - outputMode("complete")   <-- lets us see the whole result table each batch
    query = None  # <-- replace this

    # batch 1: on-time row, opens window [0,10)
    write_csv(input_dir, "b1.csv", 1, "a", 0)
    wait_for_batch_count(query, 1)

    # batch 2: on-time row at t=12s, opens window [10,20), advances watermark to 12-5=7s
    write_csv(input_dir, "b2.csv", 2, "b", 12)
    wait_for_batch_count(query, 2)

    # batch 3: LATE row at t=3s -- watermark is only 7s, window [0,10) (end=10) is still open
    #   -> this one should be ACCEPTED into window [0,10)
    write_csv(input_dir, "b3.csv", 100, "late-but-ok", 3)
    wait_for_batch_count(query, 3)

    # batch 4: on-time row at t=25s -- advances watermark to 25-5=20s, closing [0,10) and [10,20)
    write_csv(input_dir, "b4.csv", 4, "d", 25)
    wait_for_batch_count(query, 4)

    # batch 5: LATE row at t=4s -- watermark is now 20s, window [0,10) (end=10) closed long ago
    #   -> this one should be SILENTLY DROPPED
    write_csv(input_dir, "b5.csv", 999, "late-and-dropped", 4)
    wait_for_batch_count(query, 5)

    rows = {
        r["w_start"]: (r["cnt"], r["total"])
        for r in spark.sql(
            "select window.start as w_start, cnt, total from ex02_windows"
        ).collect()
    }
    print(rows)

    # ---- self-check ----
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
