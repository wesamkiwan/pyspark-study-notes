# Lesson 4 — Pandas UDFs and Arrow: Vectorized Execution

A regular `@udf` ships rows to the Python worker one at a time, pickled individually — that's what
`BatchEvalPython` was doing in Lesson 1. A `pandas_udf` ships whole **batches of rows as a single
Arrow columnar buffer**, your function receives and returns a `pandas.Series` (or several), and the
result is converted back to Spark columns in bulk. This is "vectorized" execution: your function
runs once per batch, operating on a whole array at a time with pandas/NumPy operations, not once
per row.

## Writing one

```python
from pyspark.sql.functions import pandas_udf, col
import pandas as pd

@pandas_udf("double")
def times_two(s: pd.Series) -> pd.Series:
    return s * 2

df.select("n", times_two(col("n")).alias("doubled")).show()
```

The type hint (`pd.Series -> pd.Series`) is how Spark 3.x's `pandas_udf` knows this is a
Series-to-Series (scalar) Pandas UDF, the most common variant. `pandas_udf` needs `pyarrow`
installed in addition to `pandas` (both added to `requirements.txt` for this module).

## Verifying it's actually a different execution path: `ArrowEvalPython`

```python
df.withColumn("doubled", times_two(col("n"))).explain()
```

Verified output:

```
== Physical Plan ==
*(2) Project [id#0L AS n#2L, pythonUDF0#8 AS doubled#5]
+- ArrowEvalPython [times_two(id#0L)#4], [pythonUDF0#8], 200
   +- *(1) Range (0, 10, step=1, splits=16)
```

Compare this to Lesson 1's `BatchEvalPython` node for the plain UDF — same shape of plan, genuinely
different physical operator. `ArrowEvalPython` is the node that batches rows into Arrow record
batches instead of pickling them individually. The batch size is controlled by
`spark.sql.execution.arrow.maxRecordsPerBatch` (verified default: `10000` rows per batch) — tune
this down if a single batch's pandas DataFrame would use too much executor memory for wide rows.

## Verified timing: measure, don't assume

A lot of folklore claims Pandas UDFs are "10-100x faster" than row UDFs. That number depends
heavily on the workload — verified locally (PySpark 3.5.3, `local[*]`, 2,000,000 rows, 8
partitions, timings averaged over 2 runs each):

| Workload | Row-at-a-time `@udf` | `pandas_udf` |
|---|---|---|
| Pure numeric (`sqrt(n) * 1.5`, summed) | ~7.5-7.8s | ~6.9-7.3s |
| Branching/string logic (salary-band bucketing) | ~6.7s | ~6.9s |

**The real, measured speedup here was modest (roughly 5-15%) for numeric work, and negligible — or
even a wash — for branching/string logic**, not the dramatic gap folklore suggests. Two reasons,
both real:
- On a single local machine with cached, already-in-memory data, the fixed cost of spinning up a
  Python worker and the JVM↔Python IPC round-trip is shared by both approaches — vectorization
  saves per-row *pickling* overhead specifically, which is a smaller fraction of total time here
  than it would be on a real multi-node cluster with network-bound shuffles.
- `pandas_udf`'s advantage is largest when the actual computation vectorizes well as a NumPy
  array op (the numeric case above). Branching/string logic doesn't vectorize as cleanly even
  inside pandas — `pd.cut`/string ops still iterate more than raw arithmetic does under the hood.

**The takeaway: `pandas_udf` is still the right default over a plain UDF once you've decided a UDF
is necessary at all** — the Arrow path is never *worse* in principle, and the numeric case shows a
real, if modest, win in this environment. But don't cite a specific multiplier as if it's guaranteed
for your workload; profile your actual job. Also don't let a modest local speedup talk you out of
Lesson 1's real rule — a built-in Spark function beats both of these for anything it can express,
regardless of the UDF-vs-Pandas-UDF gap.

---
**Next:** [Lesson 5 — applyInPandas and the Decision Tree](05-applyinpandas-and-the-decision-tree.md)
