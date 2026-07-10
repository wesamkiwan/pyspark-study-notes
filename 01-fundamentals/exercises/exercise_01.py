"""
Exercise 1 — SparkSession, DataFrame basics, actions vs transformations

Fill in every `# TODO`. Then run this file directly:

    python 01-fundamentals/exercises/exercise_01.py

It self-checks your answers with assertions. "All checks passed!" means you're done.
If something's wrong you'll get an AssertionError pointing at the failing check.

Don't peek at solutions/solution_01.py until you've genuinely tried this yourself.
"""

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

DATA_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "data", "employees.csv")


def main() -> None:
    # TODO 1: build a SparkSession.
    #   - appName: "exercise-01"
    #   - master: local, using all available cores
    #   - config "spark.sql.shuffle.partitions" set to "8" (small local data doesn't need 200)
    spark = None  # <-- replace this

    spark.sparkContext.setLogLevel("ERROR")

    # TODO 2: read DATA_PATH as a CSV with a header row and inferred schema.
    employees = None  # <-- replace this

    # TODO 3: total number of rows in the whole file (this is an ACTION).
    total_count = None  # <-- replace this

    # TODO 4: a DataFrame containing only employees in the "Engineering" department.
    #   Remember: this line alone should trigger NO computation (it's a transformation).
    eng_df = None  # <-- replace this

    # TODO 5: how many rows are in eng_df? (an ACTION)
    eng_count = None  # <-- replace this

    # TODO 6: a DataFrame of employees whose salary is null.
    #   Hint: col("salary").isNull()
    null_salary_df = None  # <-- replace this
    null_salary_count = None  # <-- replace this

    # TODO 7: average salary per department, excluding rows with a null salary,
    #   column aliased to "avg_salary", ordered by avg_salary descending.
    avg_by_dept = None  # <-- replace this

    # TODO 8: the distinct department names as a plain Python list of strings, e.g.
    #   ["Engineering", "Sales", ...] — not a DataFrame.
    #   This calls for an action that returns rows to the driver. Since we know there are
    #   only a handful of distinct departments (never true for arbitrary columns!), this is
    #   one of the rare safe uses of collecting to the driver.
    department_names = None  # <-- replace this

    # ---- self-check: do not modify below this line ----
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
