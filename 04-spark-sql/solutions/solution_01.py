"""
Solution to exercises/exercise_01.py -- read this only after attempting it yourself.
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

    high_earners = spark.sql(
        "SELECT name, salary FROM employees "
        "WHERE department = :dept AND salary > :min_sal "
        "ORDER BY salary DESC",
        args={"dept": "Engineering", "min_sal": 100000},
    )

    above_avg_counts = spark.sql(
        """
        WITH dept_avg AS (
            SELECT department, ROUND(AVG(salary), 2) AS avg_salary
            FROM employees
            GROUP BY department
        )
        SELECT e.department, COUNT(*) AS above_avg_count
        FROM employees e
        JOIN dept_avg d ON e.department = d.department
        WHERE e.salary > d.avg_salary
        GROUP BY e.department
        ORDER BY e.department
        """
    )

    union_products = spark.sql(
        """
        SELECT product FROM orders WHERE region = 'West'
        UNION
        SELECT product FROM orders WHERE region = 'East'
        """
    )
    union_all_products = spark.sql(
        """
        SELECT product FROM orders WHERE region = 'West'
        UNION ALL
        SELECT product FROM orders WHERE region = 'East'
        """
    )

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
