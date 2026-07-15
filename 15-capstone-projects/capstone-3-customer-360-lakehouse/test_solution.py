"""
Tests for pipeline_logic.py (Module 13 style) -- one test per distinct behavior.

Run:
    pytest 15-capstone-projects/capstone-3-customer-360-lakehouse/test_solution.py -v
"""

from datetime import date

from chispa.dataframe_comparer import assert_df_equality
from pipeline_logic import find_changed_keys, point_in_time_join


def test_find_changed_keys_detects_a_real_tier_change(spark):
    current = spark.createDataFrame([(1, "alice", "gold"), (2, "bob", "silver")], ["cust_id", "name", "tier"])
    incoming = spark.createDataFrame([(2, "bob", "gold")], ["cust_id", "name", "tier"])
    result = find_changed_keys(incoming, current)
    expected = spark.createDataFrame([(2, "bob", "gold")], ["cust_id", "name", "tier"])
    assert_df_equality(result, expected, ignore_row_order=True, ignore_nullable=True)


def test_find_changed_keys_ignores_unchanged_rows(spark):
    current = spark.createDataFrame([(1, "alice", "gold")], ["cust_id", "name", "tier"])
    incoming = spark.createDataFrame([(1, "alice", "gold")], ["cust_id", "name", "tier"])
    assert find_changed_keys(incoming, current).count() == 0


def test_point_in_time_join_uses_the_tier_active_at_order_time(spark):
    dimension = spark.createDataFrame(
        [
            (2, "bob", "silver", date(2024, 1, 1), date(2024, 1, 2)),
            (2, "bob", "gold", date(2024, 1, 2), None),
        ],
        ["cust_id", "name", "tier", "effective_start", "effective_end"],
    )
    fact = spark.createDataFrame(
        [
            (1, 2, 100.0, date(2024, 1, 1)),   # BEFORE the upgrade
            (2, 2, 200.0, date(2024, 1, 2)),   # AFTER the upgrade
        ],
        ["order_id", "cust_id", "amount", "order_date"],
    )
    result = {r["order_id"]: r["tier_at_order_time"] for r in point_in_time_join(fact, dimension).collect()}
    assert result[1] == "silver"
    assert result[2] == "gold"


def test_point_in_time_join_does_not_just_use_the_current_tier(spark):
    """The bug this test guards against: joining against only is_current=true would
    incorrectly show BOTH orders as 'gold', including the one placed before the upgrade."""
    dimension = spark.createDataFrame(
        [
            (2, "bob", "silver", date(2024, 1, 1), date(2024, 1, 2)),
            (2, "bob", "gold", date(2024, 1, 2), None),
        ],
        ["cust_id", "name", "tier", "effective_start", "effective_end"],
    )
    fact = spark.createDataFrame([(1, 2, 100.0, date(2024, 1, 1))], ["order_id", "cust_id", "amount", "order_date"])
    result = point_in_time_join(fact, dimension).collect()
    assert result[0]["tier_at_order_time"] != "gold", (
        "the order predates the upgrade -- it must NOT show today's tier"
    )
