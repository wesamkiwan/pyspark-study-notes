# Lesson 1 — Selecting and Creating Columns

Every transformation pipeline starts with two questions: which columns do I keep, and what new
columns do I need to compute? `select` answers the first, `withColumn` answers the second. Both
lean on the same underlying idea — a **column expression** — which is worth understanding properly
before you use either.

## Column expressions: `col()`, string names, and `df["..."]`

Three ways to refer to a column, and for simple single-DataFrame code they're interchangeable:

```python
employees.select("name", "department")            # bare strings
employees.select(col("name"), col("department"))    # col() expressions
employees.select(employees["name"], employees["department"])   # df["..."] attribute-style
```

```
+-----------+-----------+
|       name| department|
+-----------+-----------+
| Alice Chen|Engineering|
| Bob Okafor|Engineering|
|Carol Nunez|Engineering|
+-----------+-----------+
only showing top 3 rows
```

They stop being interchangeable the moment you need to build an **expression** (arithmetic,
conditions, function calls) rather than just naming a column — `"salary" > 70000` is just a string
compared to a number in plain Python and does nothing useful, while `col("salary") > 70000`
builds an actual Spark `Column` object representing that comparison. Prefer `col()` once you're
doing anything beyond plain column selection — it's also the only option that still works once a
column is disambiguated by a table alias, which matters once you start joining (Module 05).

## `withColumn`: add or replace a column

```python
employees.withColumn("salary_k", col("salary") / 1000).select("name", "salary_k")
```

```
+-----------+--------+
|       name|salary_k|
+-----------+--------+
| Alice Chen|   125.0|
| Bob Okafor|    98.0|
|Carol Nunez|   101.0|
+-----------+--------+
only showing top 3 rows
```

If the name you pass to `withColumn` already exists, it's **replaced**, not duplicated — no error,
no warning:

```python
employees.withColumn("salary", col("salary") / 1000).select("name", "salary")
```

```
+-----------+------+
|       name|salary|
+-----------+------+
| Alice Chen| 125.0|
| Bob Okafor|  98.0|
|Carol Nunez| 101.0|
+-----------+------+
only showing top 3 rows
```

That's exactly what happened above — `salary` now holds thousands, silently overwriting the
original dollar figure in this DataFrame. This is normal, expected `withColumn` behavior, not a
bug — but it means a typo that accidentally matches an existing column name won't raise an error;
it'll just quietly clobber that column. Give new columns clearly distinct names unless overwriting
is exactly what you intend.

## Chaining `withColumn` calls

```python
result = (
    employees
    .withColumn("annual_bonus", round(col("salary") * 0.1, 2))
    .withColumn("full_comp", col("salary") + col("annual_bonus"))
    .select("name", "salary", "annual_bonus", "full_comp")
)
```

```
+-----------+--------+------------+---------+
|       name|  salary|annual_bonus|full_comp|
+-----------+--------+------------+---------+
| Alice Chen|125000.0|     12500.0| 137500.0|
| Bob Okafor| 98000.0|      9800.0| 107800.0|
|Carol Nunez|101000.0|     10100.0| 111100.0|
+-----------+--------+------------+---------+
only showing top 3 rows
```

Each `withColumn` call just adds a node to the (still lazy — see Module 01, Lesson 5) logical
plan; Spark's Catalyst optimizer collapses this chain before any actual execution, so a readable
chain of several `withColumn` calls doesn't cost you a separate pass over the data per call.
Prefer readability — one `withColumn` per logical new column — over cramming everything into one
giant `select` with a dozen inline expressions.

## The gotcha: a raw Python literal is not a `Column`

```python
employees.withColumn("bonus_flag", True)
```

Verified — this fails immediately, before any data is touched:

```
PySparkTypeError: [NOT_COLUMN] Argument `col` should be a Column, got bool.
```

`withColumn`'s second argument must be a `Column` expression, not a plain Python value. Wrap
literal values with `lit()`:

```python
from pyspark.sql.functions import lit

employees.withColumn("bonus_flag", lit(True)).select("name", "bonus_flag")
```

```
+-----------+----------+
|       name|bonus_flag|
+-----------+----------+
| Alice Chen|      true|
| Bob Okafor|      true|
|Carol Nunez|      true|
+-----------+----------+
only showing top 3 rows
```

This is the single most common first mistake with `withColumn` — the error message is clear once
you've seen it, but confusing the first time, since in plain Python `True` and `70000` are
perfectly normal values. In Spark's world, anything going into a column expression needs to
*itself* be a column expression — `lit()` is what promotes a constant into one.

## `drop` and `withColumnRenamed`

```python
employees.drop("manager_id")                          # remove a column
employees.withColumnRenamed("emp_id", "employee_id")     # rename without changing values
```

Both return a new DataFrame — like everything in Spark's DataFrame API, these are non-destructive;
the original `employees` DataFrame is untouched (transformations are always immutable, matching
the RDD immutability you saw in Module 01).

---
**Next:** [Lesson 2 — Filtering and Conditional Logic](02-filter-and-conditionals.md)
