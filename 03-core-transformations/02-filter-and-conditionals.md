# Lesson 2 — Filtering and Conditional Logic

`filter` and `where` are exact aliases for each other — pick whichever reads better in context
(`where` reads more like SQL, `filter` more like general Python/functional code). The real
substance of this lesson is how to combine conditions correctly, and how to compute a column
*conditionally* with `when`/`otherwise`.

## Combining conditions: the operator-precedence trap

You can't use Python's `and`/`or`/`not` with Spark `Column` conditions — Spark overloads `&`, `|`,
and `~` instead, since `and`/`or` can't be overridden in Python. That single fact creates a classic,
easy-to-hit bug:

```python
employees.filter(col("salary") > 70000 & col("department") == "Sales")
```

Verified — this fails, but not with an obviously helpful message:

```
Py4JError: An error occurred while calling o88.and. Trace:
py4j.Py4JException: Method and([class java.lang.Integer]) does not exist
```

The cause is **Python operator precedence**, not Spark: `&` binds *tighter* than `>` and `==` in
Python, so this actually gets parsed as `col("salary") > (70000 & col("department")) == "Sales"` —
nonsense that tries to `&` an `Integer` with a `Column` before the comparisons even happen. This is
one of the most common real-world PySpark bugs, and worse, it doesn't always fail loudly like this
— depending on the exact expression, a precedence mistake can sometimes produce a *wrong result
that runs without error* instead of an exception.

**The fix: wrap every individual condition in parentheses.**

```python
employees.filter((col("salary") > 70000) & (col("department") == "Sales")).select("name", "salary", "department")
```

```
+-------------+-------+----------+
|         name| salary|department|
+-------------+-------+----------+
|    David Kim|72000.0|     Sales|
|Farid Haidari|75000.0|     Sales|
+-------------+-------+----------+
```

Treat this as a hard rule, not a style preference: **every comparison combined with `&`, `|`, or
`~` gets its own parentheses**, no exceptions, even when you're confident about precedence in a
given case. The cost of an extra pair of parens is nothing; the cost of this bug reaching
production is a silently wrong filter.

## Nulls: `isNull`, `isNotNull`, `coalesce`

Mona Farouk's `salary` is `NULL` in the source data (a genuinely missing value, not a parsing
artifact this time). Ordinary comparisons against `NULL` don't behave like you might expect from
plain Python — `salary > 70000` is neither `True` nor `False` for a null salary, it's null too,
and null conditions are treated as *not matching* a filter (not as an error, and not as included).
To find it, or handle it, you need the null-aware predicates:

```python
employees.filter(col("salary").isNull()).select("name", "department", "salary")
```

```
+-----------+-----------+------+
|       name| department|salary|
+-----------+-----------+------+
|Mona Farouk|Engineering|  NULL|
+-----------+-----------+------+
```

To substitute a default value instead of leaving it null, use `coalesce` (returns the first
non-null argument) — this is the DataFrame-API equivalent of SQL's `COALESCE`:

```python
from pyspark.sql.functions import coalesce, lit

employees.withColumn("salary_filled", coalesce(col("salary"), lit(0.0))) \
    .filter(col("name") == "Mona Farouk").select("name", "salary", "salary_filled")
```

```
+-----------+------+-------------+
|       name|salary|salary_filled|
+-----------+------+-------------+
|Mona Farouk|  NULL|          0.0|
+-----------+------+-------------+
```

> **Note:** `df.na.fill(0.0, subset=["salary"])` does the same job for filling nulls across an
> entire DataFrame column at once, and `df.na.drop(subset=["salary"])` drops rows with a null
> there. Reach for `coalesce` when you're computing one new column inline; reach for `.na.fill`/
> `.na.drop` when the operation is "clean this DataFrame's nulls," not "compute one column."

## `when`/`otherwise`: conditional column values

`when`/`otherwise` is Spark's `CASE WHEN`. Chain multiple `.when()` calls for multiple conditions
— they're evaluated **in order**, and the first match wins, exactly like a SQL `CASE WHEN` or an
`if`/`elif`/`elif`/`else` chain:

```python
from pyspark.sql.functions import when

tiered = employees.withColumn(
    "salary_band",
    when(col("salary") >= 100000, "Senior")
    .when(col("salary") >= 70000, "Mid")
    .when(col("salary").isNull(), "Unknown")
    .otherwise("Junior")
)
```

```
+--------------+--------+-----------+
|          name|  salary|salary_band|
+--------------+--------+-----------+
|    Alice Chen|125000.0|     Senior|
|    Bob Okafor| 98000.0|        Mid|
|   Carol Nunez|101000.0|     Senior|
|     David Kim| 72000.0|        Mid|
| Elena Petrova| 68000.0|     Junior|
| Farid Haidari| 75000.0|        Mid|
|     Grace Lin| 64000.0|     Junior|
|    Hassan Ali| 61000.0|     Junior|
|   Ines Moreau|133000.0|     Senior|
|   Jamal Smith| 89000.0|        Mid|
| Katya Ivanova| 70000.0|        Mid|
|  Liam O'Brien| 59000.0|     Junior|
|   Mona Farouk|    NULL|    Unknown|
|Noah Bergstrom| 88000.0|        Mid|
|   Olivia Tran| 85000.0|        Mid|
+--------------+--------+-----------+
```

Notice Mona Farouk (`NULL` salary) correctly lands in `"Unknown"` rather than silently falling
through to `"Junior"` — that's *only* true because the `isNull()` branch was placed deliberately
before `.otherwise(...)`. If you forget a null-handling branch, `NULL >= 100000` and every other
numeric comparison against null evaluates to null (not `False`), so the row falls through every
`.when()` and lands in `.otherwise()` by default — meaning a genuinely missing value silently gets
mislabeled as your *lowest* tier rather than flagged as unknown. Always decide deliberately what a
null should map to; don't let `.otherwise()` make that decision for you by accident.

---
**Next:** [Lesson 3 — Grouping and Aggregating](03-groupby-and-agg.md)
