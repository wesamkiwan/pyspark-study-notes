"""
Solution to exercises/exercise_01_job.py -- read this only after attempting it yourself.

Run with:
    spark-submit \
      --master "local[*]" \
      --conf "spark.sql.shuffle.partitions=8" \
      --conf "spark.app.custom.label=exercise-run" \
      --py-files helper_module.py \
      solution_01_job.py
"""

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from helper_module import shout

if __name__ == "__main__":
    spark = SparkSession.builder.appName("exercise-01-job").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    shuffle_partitions = spark.conf.get("spark.sql.shuffle.partitions")
    custom_label = spark.conf.get("spark.app.custom.label")
    shout_result = shout("py-files works")

    print(f"shuffle_partitions={shuffle_partitions}")
    print(f"custom_label={custom_label}")
    print(f"shout_result={shout_result}")

    assert shuffle_partitions == "8", f"expected '8' (from --conf), got {shuffle_partitions!r}"
    assert custom_label == "exercise-run", f"expected 'exercise-run', got {custom_label!r}"
    assert shout_result == "PY-FILES WORKS!!!", f"unexpected shout_result: {shout_result!r}"

    print("\nAll checks passed!")
    spark.stop()
