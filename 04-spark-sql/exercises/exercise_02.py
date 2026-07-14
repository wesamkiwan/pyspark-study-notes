"""
Exercise 2 -- NOT EXISTS (instead of the NOT IN + NULL trap), selectExpr/expr for
CASE WHEN + date functions, and the Catalog API for a managed table.

Fill in every `# TODO`. Run with:

    python 04-spark-sql/exercises/exercise_02.py

Don't peek at solutions/solution_02.py until you've tried this.
"""

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")


def main() -> None:
    spark = (
        SparkSession.builder.appName("spark-sql-exercise-02")
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
    employees.createOrReplaceTempView("employees")

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
    orders.createOrReplaceTempView("orders")

    # TODO 1: find employees who have NEVER placed an order. Use NOT EXISTS against
    #   `orders`, NOT `NOT IN` (Lesson 3 showed why NOT IN is unsafe here). Order by name.
    #   Column: name.
    never_ordered = None  # <-- replace this

    # TODO 2: use selectExpr to build a DataFrame with columns:
    #   name, department, hire_year (YEAR(hire_date)),
    #   salary_band (CASE WHEN salary >= 100000 THEN 'Senior'
    #                WHEN salary >= 70000 THEN 'Mid'
    #                WHEN salary IS NULL THEN 'Unknown'
    #                ELSE 'Junior' END)
    #   -- same tiering rule as Module 03 Lesson 2, expressed in SQL this time.
    tiered = None  # <-- replace this

    # TODO 3: save employees filtered to department = 'Engineering' (use expr() in the
    #   filter) as a MANAGED table named "eng_employees" via saveAsTable, overwrite mode.
    #   Then confirm it shows up in spark.catalog.listTables() as MANAGED, and drop it
    #   again at the end of this function (managed tables persist across runs otherwise).
    #   TODO 3a: write the table
    # (write it here)

    # ---- self-check ----
    never_ordered_names = [r["name"] for r in never_ordered.collect()]
    print(f"never_ordered = {never_ordered_names}")
    assert len(never_ordered_names) == 11, f"expected 11 names, got {len(never_ordered_names)}"
    assert "David Kim" not in never_ordered_names, "David Kim has placed orders and should not appear"

    band_counts = {r["salary_band"]: r["count"] for r in tiered.groupBy("salary_band").count().collect()}
    print(f"band_counts = {band_counts}")
    assert band_counts == {"Senior": 3, "Mid": 7, "Junior": 4, "Unknown": 1}, band_counts

    tables_before = {t.name: t.tableType for t in spark.catalog.listTables()}
    print(f"tables_before_drop = {tables_before}")
    assert tables_before.get("eng_employees") == "MANAGED", "expected eng_employees registered as MANAGED"

    eng_count = spark.sql("SELECT COUNT(*) AS n FROM eng_employees").collect()[0]["n"]
    print(f"eng_employees row count = {eng_count}")
    assert eng_count == 6, f"expected 6 Engineering employees, got {eng_count}"

    # TODO 3b: drop the eng_employees table now that the check above has run
    # (drop it here)

    tables_after = {t.name: t.tableType for t in spark.catalog.listTables()}
    print(f"tables_after_drop = {tables_after}")
    assert "eng_employees" not in tables_after, "eng_employees should be gone after DROP TABLE"

    print("\nAll checks passed!")
    spark.stop()


if __name__ == "__main__":
    main()
