"""
Solution to exercises/exercise_01.py -- read this only after attempting it yourself.
"""

from chispa.dataframe_comparer import assert_df_equality, assert_approx_df_equality
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


def test_row_order(spark):
    df1 = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    df2 = spark.createDataFrame([(2, "b"), (1, "a")], ["id", "name"])

    assert_df_equality(df1, df2, ignore_row_order=True)


def test_nullable_flags(spark):
    schema_nullable = StructType(
        [StructField("id", IntegerType(), True), StructField("name", StringType(), True)]
    )
    schema_not_nullable = StructType(
        [StructField("id", IntegerType(), False), StructField("name", StringType(), False)]
    )
    df3 = spark.createDataFrame([(1, "a")], schema=schema_nullable)
    df4 = spark.createDataFrame([(1, "a")], schema=schema_not_nullable)

    assert_df_equality(df3, df4, ignore_nullable=True)


def test_float_precision(spark):
    df5 = spark.createDataFrame([(1, 0.1 + 0.2)], ["id", "value"])
    df6 = spark.createDataFrame([(1, 0.3)], ["id", "value"])

    assert_approx_df_equality(df5, df6, precision=0.0001)
