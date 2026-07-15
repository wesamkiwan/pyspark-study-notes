"""
Exercise 1 -- chispa's real default behaviors: row order, column order, nullable
flags, and float precision all fail assert_df_equality by default, even when the
underlying data is "the same." Fix each assertion with the right chispa option.

Fill in every `# TODO`. Run with (note: `pytest`, not `python`, for this module):

    pytest 13-testing-pyspark-code/exercises/exercise_01.py -v

Don't peek at solutions/solution_01.py until you've tried this.
"""

from chispa.dataframe_comparer import assert_df_equality, assert_approx_df_equality
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


def test_row_order(spark):
    df1 = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    df2 = spark.createDataFrame([(2, "b"), (1, "a")], ["id", "name"])  # same rows, different order

    # TODO 1: this comparison should PASS despite the different row order.
    #   Add the one keyword argument that makes assert_df_equality ignore row order.
    assert_df_equality(df1, df2)


def test_nullable_flags(spark):
    schema_nullable = StructType(
        [StructField("id", IntegerType(), True), StructField("name", StringType(), True)]
    )
    schema_not_nullable = StructType(
        [StructField("id", IntegerType(), False), StructField("name", StringType(), False)]
    )
    df3 = spark.createDataFrame([(1, "a")], schema=schema_nullable)
    df4 = spark.createDataFrame([(1, "a")], schema=schema_not_nullable)

    # TODO 2: the DATA is identical here -- only the schema's nullable flag differs.
    #   Add the keyword argument that makes assert_df_equality ignore that difference.
    assert_df_equality(df3, df4)


def test_float_precision(spark):
    df5 = spark.createDataFrame([(1, 0.1 + 0.2)], ["id", "value"])
    df6 = spark.createDataFrame([(1, 0.3)], ["id", "value"])

    # TODO 3: 0.1 + 0.2 != 0.3 exactly in floating point. Replace assert_df_equality
    #   with the chispa function meant for float columns, passing a small precision.
    assert_df_equality(df5, df6)
