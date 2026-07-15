"""
Core transformation logic for Capstone 3, kept as pure functions (Module 13 Lesson 1)
so test_solution.py can test them in isolation.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def find_changed_keys(incoming: DataFrame, current: DataFrame) -> DataFrame:
    """SCD2 step 1 (Module 12): keys whose tier genuinely differs from the CURRENT row."""
    return (
        incoming.alias("i")
        .join(current.alias("c"), "cust_id")
        .filter("i.tier != c.tier")
        .select("i.cust_id", "i.name", "i.tier")
    )


def point_in_time_join(fact: DataFrame, dimension: DataFrame) -> DataFrame:
    """
    Join fact orders to the SCD2 customer dimension using the TIER THAT WAS ACTIVE
    when each order was placed -- not whatever the customer's tier is today.
    `dimension` must have cust_id, tier, effective_start, effective_end (nullable).
    `fact` must have cust_id, order_date (>= effective_start, and < effective_end OR
    effective_end IS NULL).
    """
    return (
        fact.alias("f")
        .join(
            dimension.alias("d"),
            (col("f.cust_id") == col("d.cust_id"))
            & (col("f.order_date") >= col("d.effective_start"))
            & (col("d.effective_end").isNull() | (col("f.order_date") < col("d.effective_end"))),
        )
        .select("f.order_id", "f.cust_id", "f.amount", "f.order_date", col("d.tier").alias("tier_at_order_time"))
    )
