"""
Solution to exercises/exercise_02.py -- read this only after attempting it yourself.
"""

import os
import sys
import shutil
import tempfile

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType


def main() -> None:
    builder = (
        SparkSession.builder.appName("dep-exercise-02")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    root = tempfile.mkdtemp(prefix="dep_ex02_")
    table_path = os.path.join(root, "customers_scd2")

    scd_schema = StructType(
        [
            StructField("cust_id", IntegerType()),
            StructField("name", StringType()),
            StructField("tier", StringType()),
            StructField("effective_start", StringType()),
            StructField("effective_end", StringType()),
            StructField("is_current", BooleanType()),
        ]
    )
    initial = spark.createDataFrame(
        [
            (1, "alice", "gold", "2024-01-01", None, True),
            (2, "bob", "silver", "2024-01-01", None, True),
        ],
        schema=scd_schema,
    )
    initial.write.format("delta").mode("overwrite").save(table_path)

    dt = DeltaTable.forPath(spark, table_path)

    incoming = spark.createDataFrame(
        [(2, "bob", "gold")],
        schema=StructType(
            [
                StructField("cust_id", IntegerType()),
                StructField("name", StringType()),
                StructField("tier", StringType()),
            ]
        ),
    )

    current = dt.toDF().filter("is_current = true")
    changed = (
        incoming.alias("i")
        .join(current.alias("c"), "cust_id")
        .filter("i.tier != c.tier")
        .select("i.cust_id", "i.name", "i.tier")
    )
    # Collect to a plain Python list NOW, before the MERGE below mutates the table. Verified:
    # even .cache() is NOT enough here -- Delta invalidates a cached DataFrame whose lineage
    # reads a table once that table is mutated, so `changed` would still silently re-evaluate
    # to zero rows if referenced again after the MERGE. Only fully materializing into Python
    # data decouples the insert step from the table's post-merge state.
    changed_rows = [tuple(r) for r in changed.collect()]

    (
        dt.alias("t")
        .merge(changed.alias("s"), "t.cust_id = s.cust_id AND t.is_current = true")
        .whenMatchedUpdate(set={"is_current": "false", "effective_end": "'2024-01-02'"})
        .execute()
    )

    changed_schema = StructType(
        [
            StructField("cust_id", IntegerType()),
            StructField("name", StringType()),
            StructField("tier", StringType()),
        ]
    )
    to_insert = spark.createDataFrame(changed_rows, schema=changed_schema).selectExpr(
        "cust_id", "name", "tier", "'2024-01-02' as effective_start",
        "CAST(NULL AS STRING) as effective_end", "true as is_current",
    )
    to_insert.write.format("delta").mode("append").save(table_path)

    result = dt.toDF().orderBy("cust_id", "effective_start").collect()
    for r in result:
        print(dict(r.asDict()))

    bob_rows = [r for r in result if r["cust_id"] == 2]
    alice_rows = [r for r in result if r["cust_id"] == 1]

    assert len(bob_rows) == 2, f"expected 2 historical rows for bob, got {len(bob_rows)}"
    assert bob_rows[0]["is_current"] is False and bob_rows[0]["tier"] == "silver"
    assert bob_rows[1]["is_current"] is True and bob_rows[1]["tier"] == "gold"
    assert len(alice_rows) == 1 and alice_rows[0]["is_current"] is True, "alice should be untouched"

    print("\nAll checks passed!")
    spark.stop()
    shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
