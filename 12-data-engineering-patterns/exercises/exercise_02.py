"""
Exercise 2 -- SCD Type 2, verified: an attribute change produces a NEW current
row while EXPIRING the old one (full history kept), an unchanged record stays
untouched, and a brand new key gets inserted directly.

Fill in every `# TODO`. Run with:

    python 12-data-engineering-patterns/exercises/exercise_02.py

Don't peek at solutions/solution_02.py until you've tried this.
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

    # day 2: bob changes tier (silver -> gold); alice is untouched
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

    # TODO 1: find keys whose tier genuinely changed vs the CURRENT row (is_current = true),
    #   then collect the result to a plain Python list of tuples via changed.collect() RIGHT AWAY.
    #   IMPORTANT: the MERGE in TODO 2 mutates the table. If you keep `changed` as a DataFrame and
    #   reference it again in TODO 3 (even a CACHED one -- Delta invalidates a cached DataFrame
    #   once the table it reads from is mutated, verified), it will silently re-evaluate against
    #   the POST-merge table, where the changed key no longer has an is_current=true row --
    #   producing zero rows to insert with no error. Only a genuine Python-side list (via
    #   .collect()) decouples TODO 3 from the table's post-merge state.
    current = dt.toDF().filter("is_current = true")
    changed = None  # <-- replace: join incoming to current on cust_id, filter tier differs
    changed_rows = None  # <-- replace: [tuple(r) for r in changed.collect()]

    # TODO 2: MERGE to expire the current row for changed keys:
    #   - condition: "t.cust_id = s.cust_id AND t.is_current = true"
    #   - whenMatchedUpdate(set={"is_current": "false", "effective_end": "'2024-01-02'"})
    #   - use `changed` (the DataFrame) as the merge source -- fine to use it ONCE, before TODO 3

    # TODO 3: build the new current row(s) from `changed_rows` (NOT `changed` the DataFrame) via
    #   spark.createDataFrame(changed_rows, schema=<explicit StructType matching cust_id/name/tier>),
    #   add effective_start='2024-01-02', effective_end=NULL, is_current=true, then append.
    #   Use an EXPLICIT schema here (cust_id as IntegerType) -- inferring it from the Python list
    #   makes cust_id a LongType, which won't match the table's IntegerType and fails the append
    #   with DELTA_FAILED_TO_MERGE_FIELDS.

    result = dt.toDF().orderBy("cust_id", "effective_start").collect()
    for r in result:
        print(dict(r.asDict()))

    # ---- self-check ----
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
