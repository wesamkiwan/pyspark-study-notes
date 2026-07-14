"""
Exercise 1 -- parameterized SQL (args=), a CTE with a join back to the base table,
and the UNION vs UNION ALL dedup trap.

Fill in every `# TODO`. Run with:

    python 04-spark-sql/exercises/exercise_01.py

Don't peek at solutions/solution_01.py until you've tried this.
"""

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
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
        SparkSession.builder.appName("spark-sql-exercise-01")
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

    # TODO 1: write a parameterized spark.sql(...) query (use `args=`, NOT an f-string)
    #   that returns employees where department = :dept AND salary > :min_sal,
    #   ordered by salary descending. Call it with dept="Engineering", min_sal=100000.
    high_earners = None  # <-- replace this

    # TODO 2: write a query using a CTE named `dept_avg` that computes each department's
    #   average salary (rounded to 2 decimals), then join it back to `employees` to count
    #   how many employees in each department earn above their department's average.
    #   Columns: department, above_avg_count. Order by department.
    above_avg_counts = None  # <-- replace this

    # TODO 3: write TWO queries against `orders` selecting `product` --
    #   one for region = 'West', one for region = 'East' -- combined with plain SQL UNION
    #   (dedups) into `union_products`, and combined with UNION ALL (keeps duplicates)
    #   into `union_all_products`.
    union_products = None  # <-- replace this
    union_all_products = None  # <-- replace this

    # ---- self-check ----
    high_names = [r["name"] for r in high_earners.collect()]
    print(f"high_earners = {high_names}")
    assert high_names == ["Ines Moreau", "Alice Chen", "Carol Nunez"], high_names

    counts = {r["department"]: r["above_avg_count"] for r in above_avg_counts.collect()}
    print(f"above_avg_counts = {counts}")
    assert counts == {"Engineering": 2, "Finance": 1, "Marketing": 1, "Sales": 2}, counts

    union_count = union_products.count()
    union_all_count = union_all_products.count()
    print(f"union_count={union_count}, union_all_count={union_all_count}")
    assert union_count == 5, f"expected 5 distinct products, got {union_count}"
    assert union_all_count == 11, f"expected 11 rows (West + East, no dedup), got {union_all_count}"
    assert union_count < union_all_count, "UNION should return fewer rows than UNION ALL here"

    print("\nAll checks passed!")
    spark.stop()


if __name__ == "__main__":
    main()
