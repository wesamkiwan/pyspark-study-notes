# Lesson 2 — CTEs, Subqueries, and the UNION vs UNION ALL Trap

## CTEs: naming an intermediate result with `WITH`

A CTE (Common Table Expression) lets you name an intermediate query and reference it later in the
same statement — the SQL equivalent of assigning a DataFrame to a variable before using it again:

```python
spark.sql("""
WITH dept_avg AS (
    SELECT department, ROUND(AVG(salary), 2) AS avg_salary
    FROM employees
    GROUP BY department
)
SELECT e.name, e.department, e.salary, d.avg_salary
FROM employees e
JOIN dept_avg d ON e.department = d.department
WHERE e.salary > d.avg_salary
ORDER BY e.department, e.salary DESC
""").show()
```

```
+--------------+-----------+--------+----------+
|          name| department|  salary|avg_salary|
+--------------+-----------+--------+----------+
|   Ines Moreau|Engineering|133000.0|  109200.0|
|    Alice Chen|Engineering|125000.0|  109200.0|
|Noah Bergstrom|    Finance| 88000.0|   86500.0|
|     Grace Lin|  Marketing| 64000.0|  61333.33|
| Farid Haidari|      Sales| 75000.0|   71250.0|
|     David Kim|      Sales| 72000.0|   71250.0|
+--------------+-----------+--------+----------+
```

`dept_avg` isn't a table or a view — it only exists for the duration of this one `spark.sql(...)`
call. This reads far more clearly than nesting the same aggregation as an inline subquery in the
`FROM` clause, especially once you need to reference it more than once.

## Correlated subqueries: `EXISTS`

An `EXISTS` subquery checks, per outer row, whether *any* matching inner row exists — it references
a column from the outer query (`e.emp_id`), which makes it a **correlated** subquery, re-evaluated
conceptually once per outer row:

```python
spark.sql("""
SELECT e.name, e.department
FROM employees e
WHERE EXISTS (
    SELECT 1 FROM orders o WHERE o.emp_id = e.emp_id
)
""").show()
```

```
+-------------+----------+
|         name|department|
+-------------+----------+
|    David Kim|     Sales|
|Elena Petrova|     Sales|
|Farid Haidari|     Sales|
|Katya Ivanova|     Sales|
+-------------+----------+
```

Only the four employees who have actually placed an order come back — this reads better than a
join-then-`distinct()` when all you need is a yes/no "does at least one related row exist," not the
actual matched columns from the other side.

## The trap: SQL's `UNION` deduplicates. The DataFrame's `.union()` does not.

Module 03 Lesson 4 established that the DataFrame method `.union()` combines two DataFrames purely
by position and keeps every row, duplicates included. **SQL's bare `UNION` keyword does something
different by default: it deduplicates.** Same English word, same general "combine two things"
idea, opposite default behavior depending on which API you're in.

Verified — running the exact same query against itself:

```python
spark.sql("""
SELECT department FROM employees WHERE department = 'Engineering'
UNION
SELECT department FROM employees WHERE department = 'Engineering'
""").show()
```

```
+-----------+
| department|
+-----------+
|Engineering|
+-----------+
```

One row — even though the underlying query matches 6 employees on each side (12 rows total), plain
`UNION` deduplicates them down to the single distinct value. Swap in `UNION ALL`:

```python
spark.sql("""
SELECT department FROM employees WHERE department = 'Engineering'
UNION ALL
SELECT department FROM employees WHERE department = 'Engineering'
""").show()
```

```
+-----------+
| department|
+-----------+
|Engineering|
|Engineering|
|Engineering|
|Engineering|
|Engineering|
|Engineering|
|Engineering|
|Engineering|
|Engineering|
|Engineering|
|Engineering|
|Engineering|
+-----------+
```

Twelve rows, all duplicates kept — this is the one that matches the DataFrame API's `.union()`
semantics (and `.unionByName()`'s, for that matter — neither ever deduplicates).

**The rule:** in SQL text, default to `UNION ALL` unless you specifically want deduplication and
have thought about the cost of it (deduplication requires a full shuffle to compare rows across the
whole dataset, same as calling `.distinct()` afterward would). Reaching for bare `UNION` out of SQL
habit, on data you expected to behave like the DataFrame `.union()` you already know, is how rows
quietly disappear from a result set with no error to point at why.

---
**Next:** [Lesson 3 — NULL and Three-Valued Logic in SQL](03-null-three-valued-logic.md)
