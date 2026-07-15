# Module 08 Quiz

Answer each yourself before expanding the answer.

---

**1. `.explain()` on a native `col("n") * 2` expression shows one `*(1)` codegen stage covering
both the range scan and the multiply. `.explain()` on an equivalent `@udf` shows `*(1)`, then
`BatchEvalPython`, then `*(2)`. What does the missing `*` on `BatchEvalPython` tell you?**

<details>
<summary>Answer</summary>

`BatchEvalPython` sits *outside* Catalyst's whole-stage codegen — verified from the plan itself.
The JVM can generate fused bytecode for the native expression and for the two Project/Range
sections around the UDF, but it cannot fuse the UDF call itself into that generated code, because a
Python UDF is an opaque function Catalyst can't see inside. Every row has to actually leave the JVM,
run in a separate Python process, and come back.
</details>

---

**2. A UDF is declared `@udf(returnType=IntegerType())` but its Python body actually returns an
f-string (`f"value-{n}"`) for every row. What happens when you run it — an exception, or something
else?**

<details>
<summary>Answer</summary>

Verified: every row silently becomes `NULL`. No exception is raised anywhere — the Python function
runs successfully and returns a real string each time, but Spark's conversion from that Python
value to the declared `IntegerType()` fails silently rather than erroring. This is a genuinely
dangerous failure mode because it produces no signal that anything went wrong.
</details>

---

**3. Why can't you rely on a Python UDF to handle a `NULL` input as gracefully as a native Spark
expression like `col("salary") + 1` does?**

<details>
<summary>Answer</summary>

Spark's `NULL` becomes Python's `None` inside a UDF, and ordinary Python code has no automatic
null-propagation the way Spark's native expressions do. Verified: `salary * 1.1` inside an
unguarded UDF, run against `employees.csv`'s genuine `NULL` salary row (Mona Farouk), throws
`TypeError: unsupported operand type(s) for *: 'NoneType' and 'float'` and fails the whole task.
Every UDF needs an explicit `if x is None: return None` (or equivalent) guard for any argument that
might be null — you don't get this behavior for free the way you do with built-in functions.
</details>

---

**4. What's the key structural difference between `@udf` and `@pandas_udf` in terms of what your
Python function actually receives?**

<details>
<summary>Answer</summary>

A plain `@udf` receives one row's worth of plain Python values at a time (called once per row, in
principle). A `@pandas_udf` (Series-to-Series) receives a whole *batch* of rows as a
`pandas.Series` at once — your function runs once per batch and operates on the whole array with
vectorized pandas/NumPy operations, not once per row. Verified via `.explain()`: the pandas UDF's
plan shows `ArrowEvalPython` instead of `BatchEvalPython`, the node that batches rows into Arrow
columnar buffers rather than pickling them individually.
</details>

---

**5. `spark.sql.execution.arrow.maxRecordsPerBatch` controls what, specifically, and what's its
verified default?**

<details>
<summary>Answer</summary>

It controls how many rows get grouped into a single Arrow record batch handed to a `pandas_udf`
call — verified default: `10000` rows per batch. Lowering it trades some vectorization efficiency
for a smaller per-batch pandas DataFrame in executor memory, useful if your rows are wide enough
that 10,000 of them at once would risk memory pressure.
</details>

---

**6. A verified local timing test (PySpark 3.5.3, `local[*]`, 2,000,000 rows) found only a
~5-15% speedup for `pandas_udf` over a row-at-a-time `@udf` on a pure numeric workload, and
roughly no difference at all for a branching/string-logic workload. Does this mean the commonly
cited "Pandas UDFs are 10-100x faster" claim is simply wrong?**

<details>
<summary>Answer</summary>

Not wrong in general, but workload- and environment-dependent, which the folklore version usually
omits. On a single local machine with already-cached, in-memory data, the fixed JVM↔Python IPC
setup cost is shared by both paths, so vectorization's per-row-pickling savings are a smaller slice
of total time than they'd be on a real multi-node cluster with network-bound shuffles. The
computation also has to actually vectorize well (numeric/NumPy-style) to benefit — branching/string
logic doesn't gain much from being handed a `pandas.Series` instead of one value at a time. The
takeaway verified here: measure your actual workload rather than assuming a specific multiplier.
</details>

---

**7. `groupBy("department").applyInPandas(zscore_fn, schema=out_schema)` computes each
department's mean/std and a z-score per employee. Engineering has a genuine `NULL` salary row
(Mona Farouk). What happens to (a) the other Engineering employees' z-scores, and (b) her own row?**

<details>
<summary>Answer</summary>

Verified: (a) the other five Engineering employees' z-scores are computed correctly and are
unaffected — `pandas.Series.mean()`/`.std()` skip `NULL`s (`NaN` in pandas) by default. (b) Her own
`salary_zscore` comes back `NULL` — `NULL - mean` is still `NULL`, so the null doesn't silently
disappear, it just doesn't corrupt the rest of the group's statistics either.
</details>

---

**8. What's the specific production risk of `applyInPandas` that doesn't apply to `pandas_udf`
(Series-to-Series)?**

<details>
<summary>Answer</summary>

`applyInPandas` hands an *entire group* to a single task as one in-memory pandas DataFrame, with no
way to split a group across multiple tasks — unlike a shuffle, which can spread arbitrary rows
across as many tasks as you want. If groups are skewed (Module 06's data-skew problem), the task
handling the largest group must materialize that whole group as a pandas DataFrame in one
executor's memory, which can OOM that single task even while every other task finishes quickly.
Always know your group-size distribution before using `applyInPandas` at production scale.
</details>

---

**9. In `rank_by_salary`, `pdf["salary"].rank(ascending=False, method="min")` gives Mona Farouk
(the `NULL`-salary row) a `NaN` rank inside the pandas function. What value actually lands in the
Spark `DoubleType` column after `applyInPandas` returns — is it kept as a literal `NaN`, or does it
become something else?**

<details>
<summary>Answer</summary>

Verified: it becomes Spark `NULL` (Python `None` when collected), not a literal float `NaN`.
Arrow's conversion back from a pandas `NaN` in a nullable numeric column produces a genuine null,
not a NaN value sitting inside a non-null double. This matters if you're writing a check for this
case — comparing for `NaN`-not-equal-itself won't work in PySpark the way it would in raw pandas;
you need `col("x").isNull()` or `is None` after collecting.
</details>

---

**10. Given everything measured in this module, what's the correct decision order when you need
custom per-row or per-group logic in a Spark pipeline?**

<details>
<summary>Answer</summary>

1. A built-in Spark SQL function first, always — Catalyst-visible, codegen-fused, null-safe by
   default, verified fastest and safest in every lesson here.
2. `pandas_udf` (or `applyInPandas` for whole-group logic) if no built-in covers it — vectorized,
   Arrow-backed, not Catalyst-visible but avoids per-row pickling.
3. A plain row-at-a-time `@udf` only as a genuine last resort, when the logic can't be expressed as
   a vectorized pandas operation at all.

Every step down this list trades away optimizer visibility and/or performance for expressive
freedom — pay that cost deliberately, not by default.
</details>

---

Check the boxes in [`PROGRESS.md`](../PROGRESS.md) and move on to Module 09 when it's built.
