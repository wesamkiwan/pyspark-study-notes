# Lesson 4 — Bridging SQL and the DataFrame API: expr(), selectExpr(), and CAST

You don't have to choose one API for an entire pipeline. `expr()` and `selectExpr()` let you drop a
SQL expression into DataFrame-API code wherever it reads more naturally — a `CASE WHEN`, a date
function, a `CAST` — without registering a temp view or leaving `spark.sql(...)`.

## `selectExpr()`: SQL expressions as strings, DataFrame-API chaining

```python
employees.selectExpr(
    "name",
    "hire_date",
    "YEAR(hire_date) AS hire_year",
    "DATEDIFF(DATE'2024-01-01', hire_date) AS days_employed_by_2024",
    "CASE WHEN salary >= 100000 THEN 'Senior' "
    "WHEN salary >= 70000 THEN 'Mid' ELSE 'Junior' END AS band",
).orderBy("emp_id").show(6)
```

```
+-------------+----------+---------+----------------------+------+
|         name| hire_date|hire_year|days_employed_by_2024|  band|
+-------------+----------+---------+----------------------+------+
|   Alice Chen|2019-03-14|     2019|                  1754|Senior|
|   Bob Okafor|2020-07-01|     2020|                  1279|   Mid|
|  Carol Nunez|2021-01-11|     2021|                  1085|Senior|
|    David Kim|2018-05-23|     2018|                  2049|   Mid|
|Elena Petrova|2022-02-15|     2022|                   685|Junior|
|Farid Haidari|2021-09-09|     2021|                   844|   Mid|
+-------------+----------+---------+----------------------+------+
only showing top 6 rows
```

This `CASE WHEN ... END` is functionally identical to the `when(...).when(...).otherwise(...)`
chain from Module 03 Lesson 2 — same logic, SQL syntax instead of chained Python calls. Which one
reads better here is a judgment call; `selectExpr` tends to win when you're translating an existing
SQL expression directly, or the expression involves several SQL-only built-ins (like `DATEDIFF`)
that would otherwise need a separate `from pyspark.sql.functions import ...` for each one.

## `expr()`: the same thing, usable anywhere a `Column` is expected

`selectExpr` only works for whole `select()` calls. `expr()` wraps a single SQL expression as a
`Column`, so it can go inside `select()`, `filter()`, `withColumn()` — anywhere a `Column` object is
expected:

```python
from pyspark.sql.functions import expr

employees.select(
    "name",
    expr("CASE WHEN salary >= 100000 THEN 'Senior' ELSE 'Other' END AS band"),
).show(5)
```

```
+-------------+------+
|         name|  band|
+-------------+------+
|   Alice Chen|Senior|
|   Bob Okafor| Other|
|  Carol Nunez|Senior|
|    David Kim| Other|
|Elena Petrova| Other|
+-------------+------+
only showing top 5 rows
```

`filter("salary > 70000")` — passing a plain SQL-expression string directly, without even calling
`expr()` — also works; `filter`/`where` accept a bare SQL string as shorthand and parse it as one:

```python
employees.filter("salary > 70000").select("name", "salary").orderBy("emp_id").show(3)
```

```
+-----------+--------+
|       name|  salary|
+-----------+--------+
| Alice Chen|125000.0|
| Bob Okafor| 98000.0|
|Carol Nunez|101000.0|
+-----------+--------+
only showing top 3 rows
```

## The `CAST` gotcha: a failed conversion is `NULL`, not an error

SQL comparisons across types get implicitly coerced when it's unambiguous — `salary > '70000'`
above worked even though `'70000'` is a string literal, because Spark coerced it to match `salary`'s
type before comparing. That convenience has an edge that bites in data-quality-sensitive code: an
**explicit** `CAST` that can't actually parse the value doesn't raise an error — it silently
produces `NULL`.

```python
spark.sql("""
    SELECT CAST('not_a_number' AS DOUBLE) AS bad_cast,
           CAST('125000' AS DOUBLE) AS good_cast
""").show()
```

```
+--------+---------+
|bad_cast|good_cast|
+--------+---------+
|    NULL|125000.0|
+--------+---------+
```

No exception, no job failure — a genuinely malformed value (a typo, a corrupted upstream field, a
unit label that snuck into a numeric column) just becomes an ordinary `NULL`, indistinguishable
from data that was legitimately missing to begin with. This is the same category of silent failure
Module 02 covered for `inferSchema` and malformed rows: Spark consistently prefers "keep running,
turn the bad value into null" over raising, and every layer of the API (`CAST`, numeric
comparisons, `inferSchema`) inherits that default. If you need to tell "genuinely missing" apart
from "failed to parse," check for unexpected nulls *before* the cast — e.g. count rows where the
raw string column is non-null but the cast result is null — rather than trusting the cast to fail
loudly.

---
**Next:** [Lesson 5 — The Catalog API: Temp Views vs Managed vs External Tables](05-catalog-managed-external.md)
