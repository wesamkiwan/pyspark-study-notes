"""
Capstone 3 solution -- Customer 360 Lakehouse.

An SCD2 customer dimension (Module 12) + an idempotent MERGE-based fact table
(Module 11/12) + a point-in-time-correct join between them, proving an order
placed BEFORE a tier upgrade correctly shows the OLD tier, not today's tier.

Run:
    python 15-capstone-projects/capstone-3-customer-360-lakehouse/solution.py
"""

import os
import sys
import shutil
import tempfile

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, BooleanType, DoubleType, DateType,
)
from delta.tables import DeltaTable

from pipeline_logic import find_changed_keys, point_in_time_join


def main() -> None:
    builder = (
        SparkSession.builder.appName("capstone-3-customer-360")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    root = tempfile.mkdtemp(prefix="capstone3_")
    dim_path = os.path.join(root, "dim_customers")
    fact_path = os.path.join(root, "fact_orders")

    # ---- SCD2 customer dimension, day 1 ----
    dim_schema = StructType(
        [
            StructField("cust_id", IntegerType()),
            StructField("name", StringType()),
            StructField("tier", StringType()),
            StructField("effective_start", DateType()),
            StructField("effective_end", DateType()),
            StructField("is_current", BooleanType()),
        ]
    )
    from datetime import date

    initial = spark.createDataFrame(
        [
            (1, "alice", "gold", date(2024, 1, 1), None, True),
            (2, "bob", "silver", date(2024, 1, 1), None, True),
        ],
        schema=dim_schema,
    )
    initial.write.format("delta").mode("overwrite").save(dim_path)
    dt = DeltaTable.forPath(spark, dim_path)

    # day 2: bob upgrades silver -> gold
    incoming = spark.createDataFrame(
        [(2, "bob", "gold")],
        schema=StructType(
            [StructField("cust_id", IntegerType()), StructField("name", StringType()), StructField("tier", StringType())]
        ),
    )
    current = dt.toDF().filter("is_current = true")
    changed = find_changed_keys(incoming, current)
    changed_rows = [tuple(r) for r in changed.collect()]  # materialize BEFORE the MERGE (Module 12)

    (
        dt.alias("t")
        .merge(changed.alias("s"), "t.cust_id = s.cust_id AND t.is_current = true")
        .whenMatchedUpdate(set={"is_current": "false", "effective_end": "CAST('2024-01-02' AS DATE)"})
        .execute()
    )

    changed_schema = StructType(
        [StructField("cust_id", IntegerType()), StructField("name", StringType()), StructField("tier", StringType())]
    )
    to_insert = spark.createDataFrame(changed_rows, schema=changed_schema).selectExpr(
        "cust_id", "name", "tier",
        "CAST('2024-01-02' AS DATE) as effective_start",
        "CAST(NULL AS DATE) as effective_end",
        "true as is_current",
    )
    to_insert.write.format("delta").mode("append").save(dim_path)

    print("=== dim_customers (SCD2) ===")
    spark.read.format("delta").load(dim_path).orderBy("cust_id", "effective_start").show()

    # ---- fact_orders: order 1 BEFORE bob's upgrade, order 2 AFTER -- idempotent MERGE loads ----
    fact_schema = StructType(
        [
            StructField("order_id", IntegerType()),
            StructField("cust_id", IntegerType()),
            StructField("amount", DoubleType()),
            StructField("order_date", DateType()),
        ]
    )
    fact_initial = spark.createDataFrame([], schema=fact_schema)
    fact_initial.write.format("delta").mode("overwrite").save(fact_path)
    fact_dt = DeltaTable.forPath(spark, fact_path)

    def load_fact_batch(rows):
        batch = spark.createDataFrame(rows, schema=fact_schema)
        (
            fact_dt.alias("t")
            .merge(batch.alias("s"), "t.order_id = s.order_id")
            .whenNotMatchedInsertAll()
            .execute()
        )

    # day 1 order: bob, while still silver
    load_fact_batch([(1, 2, 100.0, date(2024, 1, 1))])
    # day 2 order: bob, AFTER the gold upgrade
    load_fact_batch([(2, 2, 200.0, date(2024, 1, 2))])
    # re-run the SAME day-2 load again (a retry) -- MERGE should keep this idempotent
    load_fact_batch([(2, 2, 200.0, date(2024, 1, 2))])

    fact_count = spark.read.format("delta").load(fact_path).count()
    print(f"\nfact_orders row count after a repeated (retried) load: {fact_count}")

    # ---- point-in-time correct join ----
    fact_df = spark.read.format("delta").load(fact_path)
    dim_df = spark.read.format("delta").load(dim_path)
    result = point_in_time_join(fact_df, dim_df).orderBy("order_id").collect()

    print("\n=== point-in-time join: tier AT THE TIME of each order ===")
    for r in result:
        print(dict(r.asDict()))

    # ---- verify ----
    assert fact_count == 2, f"expected exactly 2 fact rows (retry must not duplicate), got {fact_count}"

    by_order = {r["order_id"]: r["tier_at_order_time"] for r in result}
    assert by_order[1] == "silver", f"order 1 (before upgrade) should show tier='silver', got {by_order[1]}"
    assert by_order[2] == "gold", f"order 2 (after upgrade) should show tier='gold', got {by_order[2]}"

    print("\nAll capstone 3 assertions passed -- point-in-time SCD2 join is correct, and the fact load is idempotent!")

    spark.stop()
    shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
