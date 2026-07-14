# Lesson 1 — Parameterized SQL: The Injection-Safe Way to Build Dynamic Queries

Module 03's closing lesson warned about building `spark.sql(...)` strings from untrusted input by
f-string interpolation, and recommended staying in the DataFrame API whenever the query needs to
be driven by external input. That's still good advice — but if you specifically need SQL text with
a value plugged in (a report parameter, a filter picked from a dropdown, a value from a config),
Spark 3.4+ gives you a second, equally safe option: **named parameters**.

## The problem, restated

```python
# DON'T: value comes from outside the program
dept = get_department_from_request()
spark.sql(f"SELECT * FROM employees WHERE department = '{dept}'")   # injectable
```

If `dept` is ever attacker-controlled and happens to contain SQL syntax, this string is parsed as
part of the query, not as a plain value. That's SQL injection.

## The fix: `args=` with named parameters

```python
dept = "Sales"
min_salary = 70000

result = spark.sql(
    "SELECT name, department, salary FROM employees "
    "WHERE department = :dept AND salary > :min_salary "
    "ORDER BY salary DESC",
    args={"dept": dept, "min_salary": min_salary},
)
```

```
+-------------+----------+-------+
|         name|department| salary|
+-------------+----------+-------+
|Farid Haidari|     Sales|75000.0|
|    David Kim|     Sales|72000.0|
+-------------+----------+-------+
```

The `:dept` and `:min_salary` placeholders are bound to actual values *after* the query text has
already been parsed as SQL — exactly like a parameterized query in any other SQL system (e.g.
`?`-placeholders in a JDBC `PreparedStatement`). The value is never re-parsed as SQL syntax, no
matter what it contains.

## Verified: an injection attempt as a parameter value is inert

```python
evil = "Sales' OR '1'='1"
spark.sql(
    "SELECT count(*) AS n FROM employees WHERE department = :dept",
    args={"dept": evil},
).show()
```

```
+---+
|  n|
+---+
|  0|
+---+
```

Zero rows — because `:dept` is bound to the literal string `"Sales' OR '1'='1"` and compared as
data against the `department` column, which has no row equal to that exact (nonsense) string. If
this had instead been f-string-concatenated into the query text, `OR '1'='1'` would have been
parsed as SQL and matched every row.

## When to reach for this vs the DataFrame API

Nothing here changes Module 03's guidance for *programmatically built* queries — conditionally
adding filters, looping to add columns, etc. is still more natural in the DataFrame API. Named
parameters are for the specific case where you're already committed to SQL text (a ported query, a
multi-table join that reads better as SQL) and need to plug in a value safely, without dropping
back to string concatenation to do it.

---
**Next:** [Lesson 2 — CTEs, Subqueries, and the UNION vs UNION ALL Trap](02-ctes-subqueries-union.md)
