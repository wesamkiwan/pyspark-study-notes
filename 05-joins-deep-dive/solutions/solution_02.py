"""
Solution to exercises/exercise_02.py -- read this only after attempting it yourself.
"""

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import broadcast
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.utils import AnalysisException

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")


def main() -> None:
    spark = (
        SparkSession.builder.appName("joins-exercise-02")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    emp_schema = StructType(
        [
            StructField("emp_id", IntegerType()),
            StructField("name", StringType()),
            StructField("department", StringType()),
            StructField("salary", DoubleType()),
            StructField("hire_date", DateType()),
            StructField("manager_id", IntegerType()),
        ]
    )
    employees = spark.read.csv(
        os.path.join(DATA_DIR, "employees.csv"), header=True, schema=emp_schema
    )

    orders_schema = StructType(
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
    orders = spark.read.csv(
        os.path.join(DATA_DIR, "orders.csv"), header=True, schema=orders_schema
    )

    joined = employees.join(broadcast(orders), on="emp_id", how="inner")
    joined_plan_text = joined._jdf.queryExecution().executedPlan().toString()

    a = spark.createDataFrame([Row(k=1, v="a"), Row(k=None, v="b")])
    b = spark.createDataFrame([Row(k=1, v2="x"), Row(k=None, v2="y")])
    null_key_result = a.join(b, on="k", how="inner")

    spark.conf.set("spark.sql.crossJoin.enabled", "false")
    blocked = False
    try:
        employees.select("emp_id").join(orders.select("order_id")).count()
    except AnalysisException:
        blocked = True

    explicit_cross_count = employees.select("emp_id").crossJoin(orders.select("order_id")).count()
    spark.conf.set("spark.sql.crossJoin.enabled", "true")

    # ---- self-check ----
    print(f"BroadcastHashJoin present: {'BroadcastHashJoin' in joined_plan_text}")
    assert "BroadcastHashJoin" in joined_plan_text, "expected a broadcast hash join in the plan"

    null_key_rows = [r.asDict() for r in null_key_result.collect()]
    print(f"null_key_result = {null_key_rows}")
    assert null_key_rows == [{"k": 1, "v": "a", "v2": "x"}], null_key_rows

    print(f"blocked = {blocked}")
    assert blocked is True, "expected AnalysisException with crossJoin.enabled=false"

    print(f"explicit_cross_count = {explicit_cross_count}")
    assert explicit_cross_count == 225, f"expected 15*15=225, got {explicit_cross_count}"

    print("\nAll checks passed!")
    spark.stop()


if __name__ == "__main__":
    main()
