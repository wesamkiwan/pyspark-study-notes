"""
Exercise 2 -- writing GOOD tests for a quality gate function, including the edge
case (an empty batch) that a naive per-row-only quality gate silently lets through.

Fill in every `# TODO`. Run with:

    pytest 13-testing-pyspark-code/exercises/exercise_02.py -v

Don't peek at solutions/solution_02.py until you've tried this.
"""

import pytest
from pipeline_logic import DataQualityError, run_quality_gate


def test_passes_a_clean_batch(spark):
    good = spark.createDataFrame([(1, 10.0), (2, 20.0)], ["order_id", "amount"])
    # TODO 1: call run_quality_gate(good) -- it should NOT raise anything.


def test_rejects_a_non_positive_amount(spark):
    bad = spark.createDataFrame([(1, -5.0)], ["order_id", "amount"])
    # TODO 2: use `with pytest.raises(DataQualityError, match="amount <= 0"):`
    #   to assert that run_quality_gate(bad) raises the expected error.


def test_rejects_an_empty_batch(spark):
    # TODO 3: build an empty DataFrame with schema ["order_id", "amount"] (you'll need
    #   an explicit StructType -- an empty list alone can't infer a schema, Module 12
    #   Lesson 4's schema.add() pattern or a plain StructType both work).
    empty = None  # <-- replace this

    # TODO 4: assert that run_quality_gate(empty) raises DataQualityError with a
    #   message matching "Expected at least" -- this is the edge case a quality gate
    #   built only from per-row checks would otherwise silently let through.
