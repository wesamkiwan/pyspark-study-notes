# Lesson 3 — NULL and Three-Valued Logic in SQL

Module 03 Lesson 2 showed that `col("salary") > 70000` evaluates to `NULL`, not `False`, when
`salary` is null, and that `filter()` silently drops `NULL`-condition rows rather than including
them. That's not a DataFrame-API quirk — it's SQL's **three-valued logic** (`TRUE` / `FALSE` /
`UNKNOWN`), and Spark SQL text reproduces it exactly, with one extra trap that's specific to `IN`.

## `NOT` doesn't flip `NULL` to `TRUE`

The instinct is that `NOT (condition)` should return exactly the rows the plain `condition` didn't
match — including the null ones. It doesn't:

```python
spark.sql("SELECT name, salary FROM employees WHERE salary > 70000").show()
```

```
+-------------+--------+
|         name|  salary|
+-------------+--------+
|   Alice Chen|125000.0|
|   Bob Okafor| 98000.0|
|  Carol Nunez|101000.0|
|    David Kim| 72000.0|
|Farid Haidari| 75000.0|
|  Ines Moreau|133000.0|
|  Jamal Smith| 89000.0|
|Noah Bergstrom| 88000.0|
|  Olivia Tran| 85000.0|
+-------------+--------+
```

```python
spark.sql("SELECT name, salary FROM employees WHERE NOT (salary > 70000)").show()
```

```
+-------------+-------+
|         name| salary|
+-------------+-------+
|Elena Petrova|68000.0|
|    Grace Lin|64000.0|
|   Hassan Ali|61000.0|
|Katya Ivanova|70000.0|
| Liam O'Brien|59000.0|
+-------------+-------+
```

`employees.csv` has 15 rows; these two results only add up to 14. **Mona Farouk, whose `salary` is
null, appears in neither.** `salary > 70000` is `NULL` for her row, and `NOT (NULL)` is *also*
`NULL` — not `TRUE` — so she's excluded from both the condition and its negation. If you want rows
with a null salary included on the "doesn't exceed 70000" side, you have to ask for that
explicitly: `WHERE NOT (salary > 70000) OR salary IS NULL`.

## The `NOT IN` + `NULL` trap

This is one of the most infamous gotchas in SQL generally, and Spark reproduces it faithfully. A
plain `NOT IN` against a literal list with no nulls in it works as expected:

```python
spark.sql("SELECT name FROM employees WHERE manager_id NOT IN (1, 4, 7)").show()
```

```
+-----------+
|       name|
+-----------+
|Olivia Tran|
+-----------+
```

Now the realistic version — a candidate list built from a subquery, where the subquery's column
happens to contain a `NULL` (four employees here have no `manager_id`):

```python
spark.sql("""
SELECT name FROM employees
WHERE emp_id NOT IN (SELECT manager_id FROM employees)
""").show()
```

```
+----+
|name|
+----+
```

**Zero rows — for every single employee, including ones who are obviously not anyone's manager.**
`x NOT IN (a, b, NULL)` is evaluated as `x != a AND x != b AND x != NULL`. The last comparison is
always `NULL` (three-valued logic again: nothing is unequal-or-equal to `NULL`), and
`anything AND NULL` is `NULL` unless one of the other terms is already `FALSE` — which it never is
here for any row, since every `emp_id` is unequal to every non-null `manager_id`. So the whole
expression collapses to `NULL` for every row, and `NULL` never satisfies `WHERE`. **The moment even
one `NULL` sneaks into a `NOT IN` candidate list, the entire query silently returns nothing — no
error, no warning.**

The fix: filter the nulls out of the candidate list before comparing, or use `NOT EXISTS` instead
(which doesn't have this failure mode because it checks row existence, not equality against a
list):

```python
spark.sql("""
SELECT name FROM employees e
WHERE NOT EXISTS (
    SELECT 1 FROM employees m WHERE m.manager_id = e.emp_id
)
ORDER BY name
""").show(20, truncate=False)
```

```
+-------------+
|name         |
+-------------+
|Bob Okafor   |
|Carol Nunez  |
|Elena Petrova|
|Farid Haidari|
|Hassan Ali   |
|Ines Moreau  |
|Jamal Smith  |
|Katya Ivanova|
|Liam O'Brien |
|Mona Farouk  |
|Olivia Tran  |
+-------------+
```

Correctly the 11 employees who are nobody's manager (everyone except Alice Chen, David Kim, Grace
Lin, and Noah Bergstrom, who each show up as a `manager_id` for someone else) — where the `NOT IN`
version above returned nothing at all.

**The rule: never use `NOT IN` against a subquery (or any list) unless you've positively confirmed
that column can never contain `NULL`. Default to `NOT EXISTS` for "is not related to any row in..."
logic instead** — it's both immune to this trap and, for large inner tables, often the better
query plan.

## `COALESCE` and `NULLIF`: the tools for handling this deliberately

`COALESCE(col, default)` returns the first non-null argument — the standard way to substitute a
default for a null without a `CASE WHEN ... IS NULL` chain:

```python
spark.sql("SELECT name, COALESCE(salary, 0) AS salary_or_zero FROM employees WHERE salary IS NULL").show()
```

```
+-----------+--------------+
|       name|salary_or_zero|
+-----------+--------------+
|Mona Farouk|           0.0|
+-----------+--------------+
```

`NULLIF(a, b)` is the reverse: it returns `NULL` if `a` equals `b`, otherwise `a`. It's the tool for
turning a sentinel value (a system that uses `-1` or `"N/A"` to mean "no data," instead of an actual
null) *into* a real null so the rest of your NULL-handling logic (including everything in this
lesson) applies to it correctly — e.g. `NULLIF(salary, -1)` would convert a `-1` sentinel to `NULL`.

---
**Next:** [Lesson 4 — Bridging SQL and the DataFrame API: expr(), selectExpr(), and CAST](04-expr-selectexpr-cast.md)
