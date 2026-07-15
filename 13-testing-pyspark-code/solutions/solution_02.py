"""
Solution to exercises/exercise_02.py -- read this only after attempting it yourself.
"""

import pytest
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
from pipeline_logic import DataQualityError, run_quality_gate


def test_passes_a_clean_batch(spark):
    good = spark.createDataFrame([(1, 10.0), (2, 20.0)], ["order_id", "amount"])
    run_quality_gate(good)  # should not raise


def test_rejects_a_non_positive_amount(spark):
    bad = spark.createDataFrame([(1, -5.0)], ["order_id", "amount"])
    with pytest.raises(DataQualityError, match="amount <= 0"):
        run_quality_gate(bad)


def test_rejects_an_empty_batch(spark):
    schema = StructType(
        [StructField("order_id", IntegerType()), StructField("amount", DoubleType())]
    )
    empty = spark.createDataFrame([], schema=schema)

    with pytest.raises(DataQualityError, match="Expected at least"):
        run_quality_gate(empty)
