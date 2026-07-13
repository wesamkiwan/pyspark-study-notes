# Lesson 5 — DataFrame API vs Spark SQL

Everything in Modules 01–03 so far has used the DataFrame API. Spark offers a second, equally
first-class way to express the exact same transformations: SQL strings executed via `spark.sql()`.
They compile to the **same underlying query plan** — this is a stylistic and ergonomic choice, not
a performance one.

## They produce identical plans

```python
employees.createOrReplaceTempView("employees")

sql_result = spark.sql("""
    SELECT department, COUNT(*) AS headcount, ROUND(AVG(salary), 2) AS avg_salary
    FROM employees
    GROUP BY department
    ORDER BY avg_salary DESC
""")
```

```
+-----------+---------+----------+
| department|headcount|avg_salary|
+-----------+---------+----------+
|Engineering|        6|  109200.0|
|    Finance|        2|   86500.0|
|      Sales|        4|   71250.0|
|  Marketing|        3|  61333.33|
+-----------+---------+----------+
```

Identical result, identical numbers, to the `dept_stats` DataFrame built with `.groupBy().agg()`
in Lesson 3 — because `spark.sql(...)` parses the string into the *same* Catalyst logical plan
that the DataFrame API builds directly. Neither is "the fast one"; picking between them is about
which is more readable and maintainable for a given piece of logic, not about performance.

## When each one tends to win

**SQL tends to read better for:**
- Multi-table joins with several conditions — a `JOIN ... ON ...` clause is often more scannable
  than a chain of `.join()` calls, especially with 3+ tables.
- Analysts or teammates who think in SQL first — a query they can read and modify directly.
- Ports of existing SQL logic from a warehouse — sometimes the fastest, safest migration is a
  near-literal translation, deferring a DataFrame-API rewrite to later.

**The DataFrame API tends to win for:**
- Programmatically *building* a query — e.g., adding a `.filter()` conditionally based on a
  parameter, or looping to add N columns. Doing this by string-concatenating SQL is exactly the
  kind of thing that leads to the injection risk below.
  - Compare: `df.filter(col("region").isin(*allowed_regions))` — safe, no string ever built —
    versus assembling a SQL `WHERE region IN (...)` string by hand from a Python list.
- Reusable, composable transformation functions — a plain Python function that takes a DataFrame
  and returns a DataFrame is naturally testable and chainable; a function that builds SQL strings
  is not.
- Type-level clarity from the schema you already defined (Module 02) flowing through `col()`
  expressions your editor and Spark can both reason about statically-ish (column names at least).

There's no rule against mixing both in the same pipeline — read with the DataFrame API, register a
view, do one gnarly multi-join in SQL, then continue in the DataFrame API. That's normal, common
practice, not a code smell.

## Security note: never build a SQL string from untrusted input

If any part of a `spark.sql(...)` string is built by concatenating or f-string-interpolating a
value that came from a user, an API request, or any other untrusted source, you have a SQL
injection vulnerability — identical in nature to the classic SQL-injection problem in any other
system that builds queries from strings. Spark does not sanitize this for you.

```python
# DON'T: region comes from a request parameter
region = request.args["region"]
spark.sql(f"SELECT * FROM orders WHERE region = '{region}'")   # injectable
```

```python
# DO: stay in the DataFrame API for anything driven by external input
orders.filter(col("region") == region)   # `region` is just a value, never parsed as code
```

If you must build dynamic SQL, validate the value against a known allow-list (e.g., an actual set
of valid region names) before it ever reaches a string that gets executed — never trust it as-is.

## Temp views are session-scoped

```python
employees.createOrReplaceTempView("employees")
```

registers `employees` against the **current `SparkSession` only**. A different session — even in
the same JVM/application — cannot see it:

```python
spark2 = spark.newSession()
spark2.sql("SELECT * FROM employees")
```

Verified:

```
AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `employees` cannot be found.
```

If you genuinely need a view visible across multiple sessions in the same Spark application, use
`createOrReplaceGlobalTempView(...)` instead, and query it via the `global_temp` database prefix:
`spark.sql("SELECT * FROM global_temp.employees")`. This matters less in typical single-session
scripts, but shows up the moment you're doing anything with multiple sessions (some testing setups,
some multi-tenant patterns) and wonder why a view you just registered "doesn't exist."

`spark.catalog.listTables()` is the quick way to check what's actually registered and visible in
the current session at any point:

```python
[t.name for t in spark.catalog.listTables()]
# ['employees', 'orders']
```

---
This closes out Module 03. Next: [`exercises/`](exercises/), then [`quiz.md`](quiz.md).
