"""
Capstone 3 -- Customer 360 Lakehouse. Read README.md first.

`pipeline_logic.py` (find_changed_keys, point_in_time_join) is given -- your job is
wiring it into a working SCD2 + idempotent fact table pipeline.

Fill in every `# TODO`. Run with:

    python 15-capstone-projects/capstone-3-customer-360-lakehouse/starter.py

Don't peek at solution.py until you've made a genuine attempt.
"""

import os
import sys
import shutil
import tempfile
from datetime import date

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

    # TODO 1: use find_changed_keys(incoming, current) to find which keys changed, where
    #   `current` is dt.toDF() filtered to is_current = true. Remember Module 12 Lesson 2's
    #   verified trap: collect `changed` to a plain Python list of tuples BEFORE the MERGE
    #   in TODO 2 runs, or the insert in TODO 3 will silently find nothing.
    current = dt.toDF().filter("is_current = true")
    changed = None  # <-- replace
    changed_rows = None  # <-- replace: [tuple(r) for r in changed.collect()]

    # TODO 2: MERGE to expire bob's old row:
    #   condition "t.cust_id = s.cust_id AND t.is_current = true"
    #   whenMatchedUpdate(set={"is_current": "false", "effective_end": "CAST('2024-01-02' AS DATE)"})

    # TODO 3: build `to_insert` from `changed_rows` (explicit schema: cust_id IntegerType,
    #   name StringType, tier StringType) with effective_start=2024-01-02, effective_end=NULL,
    #   is_current=true, and append it to dim_path.

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
        # TODO 4: MERGE `batch` into fact_dt, keyed on order_id, whenNotMatchedInsertAll()
        #   (this is what makes a repeated load of the same order_id idempotent)
        pass

    load_fact_batch([(1, 2, 100.0, date(2024, 1, 1))])
    load_fact_batch([(2, 2, 200.0, date(2024, 1, 2))])
    load_fact_batch([(2, 2, 200.0, date(2024, 1, 2))])  # a retry -- same order_id again

    fact_count = spark.read.format("delta").load(fact_path).count()
    print(f"\nfact_orders row count after a repeated (retried) load: {fact_count}")

    # TODO 5: use point_in_time_join(fact_df, dim_df) to join fact_orders to dim_customers,
    #   getting the tier that was ACTUALLY ACTIVE when each order was placed.
    fact_df = spark.read.format("delta").load(fact_path)
    dim_df = spark.read.format("delta").load(dim_path)
    result = None  # <-- replace: point_in_time_join(fact_df, dim_df).orderBy("order_id").collect()

    print("\n=== point-in-time join: tier AT THE TIME of each order ===")
    for r in result:
        print(dict(r.asDict()))

    # ---- self-check ----
    assert fact_count == 2, f"expected exactly 2 fact rows (retry must not duplicate), got {fact_count}"

    by_order = {r["order_id"]: r["tier_at_order_time"] for r in result}
    assert by_order[1] == "silver", f"order 1 (before upgrade) should show tier='silver', got {by_order[1]}"
    assert by_order[2] == "gold", f"order 2 (after upgrade) should show tier='gold', got {by_order[2]}"

    print("\nAll capstone 3 assertions passed -- point-in-time SCD2 join is correct, and the fact load is idempotent!")

    spark.stop()
    shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
