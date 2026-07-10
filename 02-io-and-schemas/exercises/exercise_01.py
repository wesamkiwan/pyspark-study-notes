"""
Exercise 1 — Explicit schemas, reading multiple formats, partitioned writes

Fill in every `# TODO`. Run with:

    python 02-io-and-schemas/exercises/exercise_01.py

Self-checks with assertions. Don't peek at solutions/solution_01.py until you've tried this.
"""

import contextlib
import io
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

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

    # TODO 1: build an explicit StructType schema matching employees.csv's real columns:
    #   emp_id (int), name (string), department (string), salary (double),
    #   hire_date (date), manager_id (int).
    #   Import StructType/StructField/IntegerType/StringType/DoubleType/DateType from
    #   pyspark.sql.types.
    employee_schema = None  # <-- replace this

    # TODO 2: read employees.csv using this explicit schema (no inferSchema=True anywhere).
    employees = None  # <-- replace this

    # TODO 3: read data/employees.json (JSON Lines format, the default) with no options needed.
    employees_json = None  # <-- replace this

    # TODO 4: read data/employees_pretty.json (a pretty-printed JSON ARRAY) -- this one needs
    #   an option set to parse correctly. Which one, and what should it be set to?
    employees_pretty = None  # <-- replace this

    # TODO 5: write `employees` to OUTPUT_DIR/by_dept, partitioned by department,
    #   overwrite mode.

    # TODO 6: read OUTPUT_DIR/by_dept back, filter to department == "Engineering".
    #   Capture what .explain() prints (not just view it) into a Python string called
    #   `plan_text`, using contextlib.redirect_stdout + io.StringIO around a call to
    #   .explain(). You'll use plan_text in the self-check below.
    eng_from_disk = None  # <-- replace this
    plan_text = ""  # <-- replace this

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
