"""
Capstone 1 solution -- Retail Sales ETL (medallion + quality gate).

Run:
    python 15-capstone-projects/capstone-1-retail-sales-etl/solution.py
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

    # ---- BRONZE: raw ingest, schema-enforced, no cleaning ----
    raw_orders = spark.read.csv(
        os.path.join(DATA_DIR, "orders.csv"), header=True, inferSchema=True
    )
    raw_employees = spark.read.csv(
        os.path.join(DATA_DIR, "employees.csv"), header=True, inferSchema=True
    )
    raw_orders.write.format("delta").mode("overwrite").save(bronze_orders_path)
    raw_employees.write.format("delta").mode("overwrite").save(bronze_employees_path)

    bronze_orders = spark.read.format("delta").load(bronze_orders_path)
    bronze_employees = spark.read.format("delta").load(bronze_employees_path)
    print(f"bronze orders: {bronze_orders.count()} rows")
    print(f"bronze employees: {bronze_employees.count()} rows")

    # ---- SILVER: quality gate (Module 12) -- referential integrity + value validity ----
    known_emp_ids = [r["emp_id"] for r in bronze_employees.select("emp_id").collect()]

    bad_amount = bronze_orders.filter(col("amount") <= 0)
    bad_emp_id = bronze_orders.filter(~col("emp_id").isin(known_emp_ids))
    quarantined = bad_amount.unionByName(bad_emp_id).distinct()

    print(f"\nquarantined (dead-letter) orders: {quarantined.count()}")
    quarantined.show(truncate=False)
    quarantined.write.format("delta").mode("overwrite").save(dead_letter_path)

    valid_orders = bronze_orders.join(
        quarantined.select("order_id"), on="order_id", how="left_anti"
    )

    # enrich with employee info (name, department) -- Module 05 join
    silver = valid_orders.join(
        bronze_employees.select("emp_id", "name", "department"), on="emp_id", how="inner"
    )
    silver.write.format("delta").mode("overwrite").save(silver_path)

    silver_count = spark.read.format("delta").load(silver_path).count()
    print(f"\nsilver (clean, enriched) orders: {silver_count}")

    # ---- GOLD: business-level aggregates ----
    silver_df = spark.read.format("delta").load(silver_path)

    gold_region = silver_df.groupBy("region").agg(
        spark_sum("amount").alias("total_amount"), spark_count("*").alias("order_count")
    )
    gold_region.write.format("delta").mode("overwrite").save(gold_region_path)

    gold_department = silver_df.groupBy("department").agg(
        spark_sum("amount").alias("total_amount"), spark_count("*").alias("order_count")
    )
    gold_department.write.format("delta").mode("overwrite").save(gold_department_path)

    gold_employee = silver_df.groupBy("emp_id", "name").agg(
        spark_sum("amount").alias("total_amount"), spark_count("*").alias("order_count")
    )
    gold_employee.write.format("delta").mode("overwrite").save(gold_employee_path)

    print("\n=== gold: region totals ===")
    spark.read.format("delta").load(gold_region_path).orderBy("region").show()

    print("=== gold: department totals ===")
    spark.read.format("delta").load(gold_department_path).orderBy("department").show()

    print("=== gold: employee totals ===")
    spark.read.format("delta").load(gold_employee_path).orderBy(col("total_amount").desc()).show()

    # ---- verify ----
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
