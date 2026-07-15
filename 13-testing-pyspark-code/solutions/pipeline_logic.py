"""
A small quality-gate function (Module 12 style) for solution_02.py to write tests
against. Not part of the exercise itself -- the function is already implemented;
solution_02.py writes tests for it, including its edge cases.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


class DataQualityError(Exception):
    pass


def run_quality_gate(df: DataFrame, min_expected_rows: int = 1) -> None:
    row_count = df.count()
    if row_count < min_expected_rows:
        raise DataQualityError(f"Expected at least {min_expected_rows} row(s), got {row_count}")

    non_positive_amount = df.filter(col("amount") <= 0).count()
    if non_positive_amount > 0:
        raise DataQualityError(f"{non_positive_amount} row(s) with amount <= 0")
