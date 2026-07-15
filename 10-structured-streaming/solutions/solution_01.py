"""
Solution to exercises/exercise_01.py -- read this only after attempting it yourself.
"""

import os
import sys
import shutil
import tempfile

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType(
    [
        StructField("order_id", IntegerType()),
        StructField("customer", StringType()),
        StructField("amount", DoubleType()),
    ]
)


def write_csv(input_dir, name, rows):
    path = os.path.join(input_dir, name)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        f.write("order_id,customer,amount\n")
        for r in rows:
            f.write(f"{r[0]},{r[1]},{r[2]}\n")
    os.rename(tmp, path)


def main() -> None:
    spark = (
        SparkSession.builder.appName("stream-exercise-01")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    input_dir = tempfile.mkdtemp(prefix="stream_ex01_")

    write_csv(input_dir, "f1.csv", [(1, "alice", 10.0)])
    write_csv(input_dir, "f2.csv", [(2, "bob", 20.0)])
    write_csv(input_dir, "f3.csv", [(3, "carol", 30.0)])

    stream_df = spark.readStream.schema(schema).option("header", "true").csv(input_dir)

    query = (
        stream_df.writeStream.format("memory")
        .queryName("ex01_orders")
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()

    is_active_after = query.isActive
    row_count = spark.sql("select count(*) as n from ex01_orders").collect()[0]["n"]

    print(f"isActive after awaitTermination() returned: {is_active_after}")
    print(f"rows processed: {row_count}")

    assert is_active_after is False, (
        "expected the AvailableNow query to have stopped ITSELF -- isActive should be False"
    )
    assert row_count == 3, f"expected all 3 backlog rows processed, got {row_count}"

    print("\nAll checks passed!")
    spark.stop()
    shutil.rmtree(input_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
