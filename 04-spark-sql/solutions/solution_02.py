"""
Solution to exercises/exercise_02.py -- read this only after attempting it yourself.
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

    never_ordered = spark.sql(
        """
        SELECT name FROM employees e
        WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.emp_id)
        ORDER BY name
        """
    )

    tiered = employees.selectExpr(
        "name",
        "department",
        "YEAR(hire_date) AS hire_year",
        "CASE WHEN salary >= 100000 THEN 'Senior' "
        "WHEN salary >= 70000 THEN 'Mid' "
        "WHEN salary IS NULL THEN 'Unknown' "
        "ELSE 'Junior' END AS salary_band",
    )

    employees.filter(expr("department = 'Engineering'")).write.mode("overwrite").saveAsTable(
        "eng_employees"
    )

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

    spark.sql("DROP TABLE eng_employees")

    tables_after = {t.name: t.tableType for t in spark.catalog.listTables()}
    print(f"tables_after_drop = {tables_after}")
    assert "eng_employees" not in tables_after, "eng_employees should be gone after DROP TABLE"

    print("\nAll checks passed!")
    spark.stop()


if __name__ == "__main__":
    main()
