# Lesson 2 — Registering UDFs and the Return-Type Trap

There are two ways to register a Python function as a Spark UDF: the `@udf` decorator for
DataFrame API use, or `spark.udf.register()` to also make it callable from SQL strings.

```python
from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType

# DataFrame API
@udf(returnType=IntegerType())
def double_it(n):
    return n * 2

df.withColumn("doubled", double_it(col("n")))

# also usable from spark.sql(...) after registering
spark.udf.register("double_it_sql", double_it)
spark.sql("SELECT double_it_sql(n) AS doubled FROM my_view")
```

Both paths require declaring a `returnType`. That declaration is a **promise you make to Spark**,
not something Spark checks against your function's actual behavior at definition time — and what
happens when you break that promise is the real trap.

## The trap: a return-type mismatch fails silently, not loudly

```python
@udf(returnType=IntegerType())
def bad_return(n):
    return f"value-{n}"  # declared IntegerType, but this returns a str

df.withColumn("bad", bad_return(col("n"))).show()
```

Verified output:

```
+---+----+
|  n| bad|
+---+----+
|  0|NULL|
|  1|NULL|
|  2|NULL|
|  3|NULL|
|  4|NULL|
+---+----+
```

**No exception. No warning. Every row silently becomes `NULL`.** The Python function ran
successfully and returned a real string every time — Spark's internal conversion from the Python
return value to the declared Spark type failed for every row, and that failure surfaces as `NULL`
rather than an error. This is arguably worse than a crash: a crash tells you immediately that
something is wrong. A column full of unexpected `NULL`s can silently propagate through filters,
joins, and aggregations before anyone notices the data is wrong.

## Why this matters more than it looks like it should

This isn't a rare typo scenario — it's exactly what happens when:
- A UDF's logic has a code path that returns the wrong type under some condition (e.g. returns a
  `str` error message from one branch, an `int` from another, but you declared `IntegerType()`)
- A upstream library function you're wrapping changes its return type across a version bump
- You copy-paste a UDF and change its logic but forget to update the `returnType` declaration to
  match

## The defense

- **Test the UDF standalone in plain Python first**, on representative inputs including edge cases,
  before wrapping it as a UDF — catch type bugs in a debugger, not as silent `NULL`s in a Spark job.
- **After adding a UDF to a pipeline, spot-check for unexpected `NULL`s** in the output column
  specifically — don't just check that the job ran without error, since this failure mode produces
  no error at all.
- Prefer `pandas_udf` (Lesson 4) where possible — pandas/Arrow type coercion tends to raise loudly on
  genuine type mismatches rather than silently nulling, since you're handing back a typed
  `pandas.Series`, not an arbitrary Python object per row.

---
**Next:** [Lesson 3 — NULL Handling Inside UDFs](03-null-handling-in-udfs.md)
