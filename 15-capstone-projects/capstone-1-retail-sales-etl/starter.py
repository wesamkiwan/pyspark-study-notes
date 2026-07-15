"""
Capstone 1 -- Retail Sales ETL (medallion + quality gate). Read README.md first.

Fill in every `# TODO`. Run with:

    python 15-capstone-projects/capstone-1-retail-sales-etl/starter.py

Don't peek at solution.py until you've made a genuine attempt.
"""

import os
import sys
import shutil
import tempfile

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count as spark_count

REPO_ROOT = r"C:\Users\wesam\.claude\wesam\Pyspark for data engineers\pyspark-study-notes"
DATA_DIR = os.path.join(REPO_ROOT, "data")


def main() -> None:
    builder = (
        SparkSession.builder.appName("capstone-1-retail-sales-etl")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    root = tempfile.mkdtemp(prefix="capstone1_")
    bronze_orders_path = os.path.join(root, "bronze_orders")
    bronze_employees_path = os.path.join(root, "bronze_employees")
    dead_letter_path = os.path.join(root, "dead_letter_orders")
    silver_path = os.path.join(root, "silver_orders")
    gold_region_path = os.path.join(root, "gold_region_totals")
    gold_department_path = os.path.join(root, "gold_department_totals")
    gold_employee_path = os.path.join(root, "gold_employee_totals")

    # TODO 1: read data/orders.csv and data/employees.csv (header=True, inferSchema=True) into
    #   `raw_orders` / `raw_employees`, then write each to Delta at bronze_orders_path /
    #   bronze_employees_path (mode "overwrite").
    raw_orders = None  # <-- replace
    raw_employees = None  # <-- replace

    bronze_orders = spark.read.format("delta").load(bronze_orders_path)
    bronze_employees = spark.read.format("delta").load(bronze_employees_path)
    print(f"bronze orders: {bronze_orders.count()} rows")
    print(f"bronze employees: {bronze_employees.count()} rows")

    # TODO 2: build `known_emp_ids` (a Python list of every emp_id in bronze_employees).
    known_emp_ids = None  # <-- replace

    # TODO 3: build `quarantined` -- every bronze_orders row where amount <= 0 OR emp_id is
    #   NOT in known_emp_ids (use .isin(...) and ~ for "not in"). Union the two filtered sets
    #   and .distinct() them. Write `quarantined` to dead_letter_path (Delta, "overwrite").
    quarantined = None  # <-- replace

    print(f"\nquarantined (dead-letter) orders: {quarantined.count()}")
    quarantined.show(truncate=False)

    # TODO 4: build `valid_orders` -- bronze_orders MINUS whatever is in `quarantined`
    #   (a left_anti join on order_id is the clean way to do this).
    valid_orders = None  # <-- replace

    # TODO 5: build `silver` -- valid_orders INNER JOINed to bronze_employees on emp_id,
    #   selecting/keeping bronze_employees' "name" and "department" columns too.
    #   Write it to silver_path (Delta, "overwrite").
    silver = None  # <-- replace

    silver_count = spark.read.format("delta").load(silver_path).count()
    print(f"\nsilver (clean, enriched) orders: {silver_count}")

    silver_df = spark.read.format("delta").load(silver_path)

    # TODO 6: build the three gold aggregates and write each to Delta ("overwrite"):
    #   - gold_region: groupBy("region"), sum("amount") as total_amount, count("*") as order_count
    #   - gold_department: same shape, groupBy("department")
    #   - gold_employee: same shape, groupBy("emp_id", "name")

    print("\n=== gold: region totals ===")
    spark.read.format("delta").load(gold_region_path).orderBy("region").show()

    print("=== gold: department totals ===")
    spark.read.format("delta").load(gold_department_path).orderBy("department").show()

    print("=== gold: employee totals ===")
    spark.read.format("delta").load(gold_employee_path).orderBy(col("total_amount").desc()).show()

    # ---- self-check ----
    assert bronze_orders.count() == 15, f"expected 15 raw orders, got {bronze_orders.count()}"
    assert quarantined.count() == 1, f"expected exactly 1 quarantined order (emp_id=999), got {quarantined.count()}"
    assert silver_count == 14, f"expected 14 clean orders, got {silver_count}"

    region_totals = {
        r["region"]: (round(r["total_amount"], 2), r["order_count"])
        for r in spark.read.format("delta").load(gold_region_path).collect()
    }
    assert region_totals == {
        "West": (3670.99, 7),
        "East": (2500.74, 4),
        "North": (1461.24, 3),
    }, f"unexpected region totals: {region_totals}"
    assert "South" not in region_totals, "South's only order was the quarantined one -- should be gone from gold"

    dept_totals = {r["department"]: round(r["total_amount"], 2) for r in spark.read.format("delta").load(gold_department_path).collect()}
    assert dept_totals == {"Sales": 7632.97}, f"unexpected department totals: {dept_totals}"

    top_employee = spark.read.format("delta").load(gold_employee_path).orderBy(col("total_amount").desc()).first()
    assert top_employee["name"] == "Elena Petrova" and round(top_employee["total_amount"], 2) == 2500.74

    print("\nAll capstone 1 assertions passed!")

    spark.stop()
    shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
