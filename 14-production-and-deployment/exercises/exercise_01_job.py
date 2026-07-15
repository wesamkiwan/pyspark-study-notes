"""
Exercise 1 -- spark-submit, verified: --conf overrides and --py-files both take
real effect, readable back from inside the submitted job itself.

This file is the JOB to submit -- fill in every `# TODO`, then run it with:

    spark-submit \
      --master "local[*]" \
      --conf "spark.sql.shuffle.partitions=8" \
      --conf "spark.app.custom.label=exercise-run" \
      --py-files helper_module.py \
      exercise_01_job.py

(helper_module.py is provided alongside this file.) Don't peek at
solutions/solution_01_job.py until you've tried this.
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

    # TODO 1: read back "spark.sql.shuffle.partitions" via spark.conf.get(...) into `shuffle_partitions`
    shuffle_partitions = None  # <-- replace this

    # TODO 2: read back the custom config "spark.app.custom.label" via spark.conf.get(...)
    #   into `custom_label`
    custom_label = None  # <-- replace this

    # TODO 3: call shout(...) from helper_module on the string "py-files works" into `shout_result`
    shout_result = None  # <-- replace this

    print(f"shuffle_partitions={shuffle_partitions}")
    print(f"custom_label={custom_label}")
    print(f"shout_result={shout_result}")

    # ---- self-check ----
    assert shuffle_partitions == "8", f"expected '8' (from --conf), got {shuffle_partitions!r}"
    assert custom_label == "exercise-run", f"expected 'exercise-run', got {custom_label!r}"
    assert shout_result == "PY-FILES WORKS!!!", f"unexpected shout_result: {shout_result!r}"

    print("\nAll checks passed!")
    spark.stop()
