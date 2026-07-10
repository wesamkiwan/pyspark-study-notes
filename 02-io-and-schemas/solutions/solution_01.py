"""
Solution to exercises/exercise_01.py — read this only after attempting it yourself.
"""

import contextlib
import io
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "_output")


def main() -> None:
    spark = (
        SparkSession.builder.appName("io-exercise-01")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    employee_schema = StructType(
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
        os.path.join(DATA_DIR, "employees.csv"), header=True, schema=employee_schema
    )

    employees_json = spark.read.json(os.path.join(DATA_DIR, "employees.json"))

    employees_pretty = spark.read.option("multiLine", "true").json(
        os.path.join(DATA_DIR, "employees_pretty.json")
    )

    by_dept_path = os.path.join(OUTPUT_DIR, "by_dept")
    employees.write.mode("overwrite").partitionBy("department").parquet(by_dept_path)

    eng_from_disk = spark.read.parquet(by_dept_path).filter(col("department") == "Engineering")

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        eng_from_disk.explain()
    plan_text = buf.getvalue()

    # ---- self-check ----
    dtype_map = dict(employees.dtypes)
    print("employees dtypes:", dtype_map)
    assert dtype_map["salary"] == "double", f"expected salary as double, got {dtype_map['salary']}"
    assert dtype_map["hire_date"] == "date", f"expected hire_date as date, got {dtype_map['hire_date']}"

    json_count = employees_json.count()
    pretty_count = employees_pretty.count()
    print(f"employees_json count = {json_count}, employees_pretty count = {pretty_count}")
    assert json_count == 8, f"expected 8 rows from employees.json, got {json_count}"
    assert pretty_count == 3, f"expected 3 rows from employees_pretty.json, got {pretty_count}"

    print("captured plan_text:\n", plan_text)
    assert "PartitionFilters" in plan_text, "expected PartitionFilters in the captured explain output"
    assert "Engineering" in plan_text, "expected the Engineering filter value in the captured explain output"

    eng_count = len(eng_from_disk.collect())
    print(f"eng_count = {eng_count}")
    assert eng_count == 6, f"expected 6 Engineering employees read back, got {eng_count}"

    print("\nAll checks passed!")
    spark.stop()


if __name__ == "__main__":
    main()
