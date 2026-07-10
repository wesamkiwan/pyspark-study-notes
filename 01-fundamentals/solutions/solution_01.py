"""
Solution to exercises/exercise_01.py — read this only after attempting it yourself.
"""

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

DATA_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "data", "employees.csv")


def main() -> None:
    spark = (
        SparkSession.builder.appName("exercise-01")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    employees = spark.read.csv(DATA_PATH, header=True, inferSchema=True)

    total_count = employees.count()

    eng_df = employees.filter(col("department") == "Engineering")

    eng_count = eng_df.count()

    null_salary_df = employees.filter(col("salary").isNull())
    null_salary_count = null_salary_df.count()

    avg_by_dept = (
        employees.filter(col("salary").isNotNull())
        .groupBy("department")
        .agg(avg("salary").alias("avg_salary"))
        .orderBy(col("avg_salary").desc())
    )

    department_names = [row["department"] for row in employees.select("department").distinct().collect()]

    # ---- self-check ----
    print("Your results:")
    print(f"  total_count = {total_count}")
    print(f"  eng_count = {eng_count}")
    print(f"  null_salary_count = {null_salary_count}")
    print("  avg_by_dept:")
    avg_by_dept.show()
    print(f"  department_names = {sorted(department_names)}")

    assert total_count == 15, f"expected 15 total employees, got {total_count}"
    assert eng_count == 6, f"expected 6 Engineering employees, got {eng_count}"
    assert null_salary_count == 1, f"expected 1 null-salary row, got {null_salary_count}"
    assert isinstance(department_names, list), "department_names should be a plain list"
    assert sorted(department_names) == [
        "Engineering",
        "Finance",
        "Marketing",
        "Sales",
    ], f"unexpected department_names: {department_names}"

    top_dept_row = avg_by_dept.collect()[0]
    assert top_dept_row["department"] == "Engineering", (
        f"expected Engineering to have the highest avg salary, "
        f"got {top_dept_row['department']}"
    )

    print("\nAll checks passed!")
    spark.stop()


if __name__ == "__main__":
    main()
