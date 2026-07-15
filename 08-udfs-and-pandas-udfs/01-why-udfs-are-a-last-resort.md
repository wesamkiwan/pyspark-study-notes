# Lesson 1 — Why UDFs Are a Last Resort

Every Spark built-in function (`col("n") * 2`, `upper()`, `when()/otherwise()`, ...) is an
expression Catalyst understands. It can push predicates through them, fuse them into a single
generated JVM method (whole-stage codegen), and reorder them relative to joins and filters. A
Python UDF is none of that — it's an opaque function pointer Catalyst can't see inside. It has to
serialize each row out to a separate Python process, run your function, and deserialize the result
back, once per row.

## Verifying it: `.explain()` on identical logic, two ways

```python
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType

df = spark.range(10).withColumnRenamed("id", "n")

# native expression
native = df.withColumn("doubled", col("n") * 2)
native.explain()

# equivalent Python UDF
@udf(returnType=IntegerType())
def double_it(n):
    return n * 2

udf_df = df.withColumn("doubled", double_it(col("n")))
udf_df.explain()
```

Verified output — native expression, a single whole-stage-codegen unit:

```
== Physical Plan ==
*(1) Project [id#0L AS n#2L, (id#0L * 2) AS doubled#4L]
+- *(1) Range (0, 10, step=1, splits=16)
```

Verified output — the UDF version:

```
== Physical Plan ==
*(2) Project [id#0L AS n#2L, pythonUDF0#11 AS doubled#8]
+- BatchEvalPython [double_it(id#0L)#7], [pythonUDF0#11]
   +- *(1) Range (0, 10, step=1, splits=16)
```

Two things to notice, both verified from the plan itself:

- **The native plan has one `*(1)` codegen stage covering everything** — reading the range and
  doing the multiply happen in the same generated JVM bytecode, no boundary crossed.
- **The UDF plan splits into `*(1)` then `BatchEvalPython` then `*(2)`** — `BatchEvalPython` has no
  `*` star, meaning it sits *outside* codegen entirely. It's the node that ships batches of rows to
  a separate Python worker process, runs `double_it`, and reads the results back. Every row pays
  this serialization round-trip; the JVM can't fuse this step into the surrounding stages the way it
  fuses native expressions.

## What this costs you in practice

- **No predicate pushdown through a UDF.** `df.filter(some_udf(col("x")) > 5)` can't be pushed down
  into a file scan the way `df.filter(col("x") > 5)` can, because Catalyst has no idea what the UDF
  computes.
- **A process boundary per batch, not per pipeline.** Each executor's JVM talks to a spawned Python
  process over a socket for every batch of rows that hits a UDF — the same category of cost as any
  inter-process communication, paid repeatedly.
- **Nothing here is unique to "old" PySpark.** This is still true on PySpark 3.5.3, the version this
  course verifies everything against — it's not a legacy problem that vectorized execution fixed
  for regular UDFs specifically (that's what `pandas_udf`, Lesson 4, is for).

## The rule

**Reach for a built-in Spark SQL function first, always.** Almost everything you'd write a UDF for
has a built-in equivalent once you know where to look: string manipulation (`regexp_extract`,
`split`, `concat_ws`), conditional logic (`when`/`otherwise`, `coalesce`), date math
(`date_add`, `datediff`), even fairly involved JSON handling (`from_json`, `get_json_object`). A
plain Python UDF is the *last* resort — reserved for genuinely custom logic with no built-in
equivalent, and even then, Lesson 4's `pandas_udf` should usually be preferred over the row-at-a-time
UDF this lesson just measured.

---
**Next:** [Lesson 2 — Registering UDFs and the Return-Type Trap](02-registering-udfs-and-the-return-type-trap.md)
