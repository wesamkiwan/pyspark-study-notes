# Lesson 3 — DataFrame Equality with chispa, Verified

Comparing two DataFrames for equality in a test sounds trivial — it isn't. `chispa`'s
`assert_df_equality` is the standard tool for this, and this lesson verifies its actual default
behavior on four things a test author needs to know precisely: row order, column order, nullable
flags, and floating-point values. All four defaults are **stricter** than "same data" — verified
directly, not assumed from the docs.

## Setup

```python
from chispa.dataframe_comparer import assert_df_equality, assert_approx_df_equality
```

## 1. Row order — fails by default, verified

```python
df1 = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
df2 = spark.createDataFrame([(2, "b"), (1, "a")], ["id", "name"])   # same rows, different order

assert_df_equality(df1, df2)                          # verified: raises DataFramesNotEqualError
assert_df_equality(df1, df2, ignore_row_order=True)    # verified: passes
```

**Why this matters:** Spark makes almost no guarantee about row order unless you explicitly sort —
a `groupBy`/`join`/any parallel operation can legitimately return rows in a different order between
runs. If your transformation doesn't specifically guarantee an order, use
`ignore_row_order=True` — otherwise your test is quietly asserting an ordering guarantee your
actual pipeline never promised, and it can start failing for a reason that has nothing to do with
a real bug.

## 2. Column order — fails by default, verified

```python
df3 = spark.createDataFrame([(1, "a")], ["id", "name"])
df4 = spark.createDataFrame([("a", 1)], ["name", "id"])   # same data, different column order

assert_df_equality(df3, df4)   # verified: raises SchemasNotEqualError
```

Column order is part of the schema as far as `assert_df_equality` is concerned. If your
transformation's column order genuinely doesn't matter for a given test, `.select()` both
DataFrames into the same explicit column order before comparing, rather than reaching for a
same-named "ignore column order" flag — being explicit about the columns you actually expect is
usually the more valuable assertion anyway.

## 3. Nullable flags — fails by default, verified, even with identical data

```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema_nullable = StructType([StructField("id", IntegerType(), True), StructField("name", StringType(), True)])
schema_not_nullable = StructType([StructField("id", IntegerType(), False), StructField("name", StringType(), False)])

df5 = spark.createDataFrame([(1, "a")], schema=schema_nullable)
df6 = spark.createDataFrame([(1, "a")], schema=schema_not_nullable)

assert_df_equality(df5, df6)                        # verified: raises SchemasNotEqualError
assert_df_equality(df5, df6, ignore_nullable=True)   # verified: passes
```

Verified, and genuinely easy to be caught out by: **the actual data in both DataFrames is
identical** — only the schema's `nullable` metadata differs — and the default comparison still
fails. This matters in practice because different code paths can produce different `nullable`
flags for logically-the-same result (e.g. a literal you construct by hand vs. reading the same
shape from a file), so `ignore_nullable=True` is often the right default for comparing *data*, not
schema strictness — unless the test's actual point is to verify nullability itself.

## 4. Floating-point columns — exact equality genuinely fails, verified

```python
df7 = spark.createDataFrame([(1, 0.1 + 0.2)], ["id", "value"])
df8 = spark.createDataFrame([(1, 0.3)], ["id", "value"])

print((0.1 + 0.2) == 0.3)   # False -- verified, classic float representation error

assert_df_equality(df7, df8)                                  # verified: raises DataFramesNotEqualError
assert_approx_df_equality(df7, df8, precision=0.0001)          # verified: passes
```

`0.1 + 0.2` is not bit-for-bit `0.3` in IEEE 754 floating point — this isn't a Spark quirk, it's
universal to floating-point arithmetic, but it means **any test comparing computed float columns
with exact equality is fragile by construction.** `assert_approx_df_equality`'s `precision`
parameter is the fix, verified to correctly treat values within tolerance as equal.

## Summary: chispa's defaults, verified

| Difference | Default behavior | Fix |
|---|---|---|
| Row order | **Fails** | `ignore_row_order=True` |
| Column order | **Fails** | `.select()` both into the same order first |
| Nullable flag | **Fails**, even with identical data | `ignore_nullable=True` |
| Float precision | **Fails** on any representation difference | `assert_approx_df_equality(precision=...)` |

## Best-practice callout

Default to `assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)` for
most transformation tests, and reach for `assert_approx_df_equality` specifically whenever a
compared column involves floating-point arithmetic — treat the stricter defaults as intentional
guard rails only when your test's actual point is to verify order, column layout, or nullability
itself.

---
**Next:** [Lesson 4 — Testing Edge Cases](04-testing-edge-cases.md)
