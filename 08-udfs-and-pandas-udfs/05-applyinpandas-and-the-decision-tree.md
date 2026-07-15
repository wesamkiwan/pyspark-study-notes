# Lesson 5 — applyInPandas and the Decision Tree

`pandas_udf` (Lesson 4) transforms a Series into a Series — one row in, one row out, just
vectorized. Sometimes you genuinely need a whole *group's* data together to compute a result — a
per-department z-score, where every row's answer depends on that department's mean and standard
deviation. That's `DataFrame.groupBy(...).applyInPandas(func, schema)`: Spark hands your function
an entire group as one `pandas.DataFrame`, and you return a (possibly different-shaped) pandas
DataFrame back.

## Per-department salary z-score, verified against real data

```python
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

out_schema = StructType([
    StructField("emp_id", IntegerType()),
    StructField("name", StringType()),
    StructField("department", StringType()),
    StructField("salary", DoubleType()),
    StructField("salary_zscore", DoubleType()),
])

def zscore(pdf: pd.DataFrame) -> pd.DataFrame:
    mean = pdf["salary"].mean()
    std = pdf["salary"].std()
    pdf["salary_zscore"] = (pdf["salary"] - mean) / std
    return pdf[["emp_id", "name", "department", "salary", "salary_zscore"]]

result = employees.groupBy("department").applyInPandas(zscore, schema=out_schema)
```

Verified output (Engineering department shown, includes Mona Farouk's genuine `NULL` salary row):

```
+------+------------+-----------+--------+--------------------+
|emp_id|        name| department|  salary|       salary_zscore|
+------+------------+-----------+--------+--------------------+
|     1|  Alice Chen|Engineering|125000.0|  0.8395234054377574|
|     2|  Bob Okafor|Engineering| 98000.0| -0.5951051987913217|
|     3| Carol Nunez|Engineering|101000.0|-0.43570202054364626|
|     9| Ines Moreau|Engineering|133000.0|  1.2645985474315586|
|    10| Jamal Smith|Engineering| 89000.0|  -1.073314733534348|
|    13| Mona Farouk|Engineering|    NULL|                NULL|
+------+------------+-----------+--------+--------------------+
```

Two things worth noting, both verified:

- **`pandas.Series.mean()`/`.std()` skip `NULL`s by default**, so Mona Farouk's missing salary
  doesn't corrupt the other five Engineering employees' z-scores. But her *own* row correctly comes
  back `NULL` — `NULL - mean` is still `NULL`. If your downstream logic assumes every row in a group
  got a real z-score, a `NULL` in the input group is a silent way to violate that.
- **`schema=` must be declared up front and must match what your function actually returns** — this
  is the grouped-map equivalent of Lesson 2's return-type trap, except here it's the whole output
  DataFrame's shape, not just one column's type.

## The production risk unique to `applyInPandas`: one group, one task, no split

`groupBy(...).applyInPandas(...)` hands an **entire group** to a single task as one in-memory pandas
DataFrame — there's no way to split a group across multiple tasks the way a wide transformation's
shuffle can spread rows arbitrarily. If your groups are skewed (Module 06's data-skew problem, now
compounded), the task handling the largest group has to materialize that entire group as a pandas
DataFrame in one executor's memory. A department with 10 rows is nothing; a customer ID with 50
million rows in a skewed real-world dataset can OOM that one task while every other task finishes
in seconds. Always know your group-size distribution before reaching for `applyInPandas` on
production-scale data.

## The decision tree

By this point in the module you've verified three tiers, in order of preference:

1. **A built-in Spark SQL function** (Lesson 1) — Catalyst-visible, codegen-fused, null-safe by
   default. Always check here first.
2. **`pandas_udf`** (Lesson 4), or `applyInPandas` if you need whole-group context — vectorized,
   Arrow-backed, still not visible to Catalyst but avoids per-row pickling.
3. **A plain row-at-a-time `@udf`** (Lessons 1-3) — last resort. Reach for this only when the logic
   is genuinely row-by-row, can't be expressed as a vectorized pandas operation, and no built-in
   function covers it.

Every tier down this list trades Catalyst-optimizability and/or performance for expressive freedom.
Pay that cost deliberately, not by default.

---
This is the last lesson in Module 08. Continue to [`exercises/`](exercises/), then
[`solutions/`](solutions/), then [`quiz.md`](quiz.md).
