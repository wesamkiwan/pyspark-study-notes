# Module 04 Quiz

Answer each yourself before expanding the answer.

---

**1. Why is `spark.sql(f"... WHERE department = '{dept}' ...")` dangerous when `dept` comes from
outside the program, and what's the fix that keeps you in SQL text (rather than switching to the
DataFrame API)?**

<details>
<summary>Answer</summary>

f-string/concatenation builds `dept`'s value directly into the query text, so if it contains SQL
syntax, that syntax gets parsed and executed — SQL injection. The fix that stays in SQL text is
named parameters: `spark.sql("... WHERE department = :dept ...", args={"dept": dept})`. Verified —
even a value like `"Sales' OR '1'='1"` passed this way is compared as a literal string, never
re-parsed as SQL, and matches nothing.
</details>

---

**2. What's a CTE (`WITH ... AS (...)`) actually give you that an inline subquery in the `FROM`
clause doesn't?**

<details>
<summary>Answer</summary>

A name you can reference more than once within the same statement, and a query that reads
top-to-bottom instead of nested inside-out. It doesn't persist anywhere — it exists only for the
duration of that one `spark.sql(...)` call, unlike a temp view or table.
</details>

---

**3. `west.union(east)` (the DataFrame method) never deduplicates. Does plain SQL `UNION` behave
the same way? What did the verified example in Lesson 2 show?**

<details>
<summary>Answer</summary>

No — this is the trap. SQL's bare `UNION` keyword deduplicates by default; `UNION ALL` is the one
that keeps every row, matching the DataFrame `.union()`'s behavior. Verified: unioning the same
6-row `Engineering` query with itself via `UNION` returned 1 row; the identical query with
`UNION ALL` returned 12. Default to `UNION ALL` in SQL text unless you specifically want the
dedup and have thought about its cost.
</details>

---

**4. A column has a genuinely missing (null) `salary`. Does that row show up under
`WHERE salary > 70000`? Does it show up under `WHERE NOT (salary > 70000)`? Why?**

<details>
<summary>Answer</summary>

Neither. `salary > 70000` evaluates to `NULL` (not `FALSE`) when `salary` is null, and
`NOT (NULL)` is also `NULL` — not `TRUE`. SQL's three-valued logic means the row satisfies neither
the condition nor its negation; both silently exclude it. Getting it into either result requires
an explicit `OR salary IS NULL` (or `IS NOT NULL`).
</details>

---

**5. `WHERE emp_id NOT IN (SELECT manager_id FROM employees)` returns zero rows for every
employee, even ones who obviously aren't a manager. What actually happened, and what's the safe
replacement?**

<details>
<summary>Answer</summary>

The subquery's `manager_id` column contains at least one `NULL` (four employees have no manager).
`x NOT IN (a, b, NULL)` expands to `x != a AND x != b AND x != NULL`; the last comparison is always
`NULL`, which collapses the whole `AND` chain to `NULL` for every row unless one of the other terms
is already `FALSE` — which never happens here. `NULL` never satisfies `WHERE`, so the query
silently returns nothing. The safe replacement is `NOT EXISTS (SELECT 1 FROM employees m WHERE
m.manager_id = e.emp_id)`, which checks row existence instead of list equality and isn't affected
by nulls in the compared column. Rule: never use `NOT IN` against a subquery/list unless you've
confirmed that column can't contain `NULL`.
</details>

---

**6. What's the difference between `expr()` and `selectExpr()` — when would you reach for one over
the other?**

<details>
<summary>Answer</summary>

`selectExpr(...)` takes SQL-expression strings directly as the arguments to a whole `select()`
call. `expr(...)` wraps a single SQL expression as a `Column` object, so it can be used anywhere a
`Column` is expected — inside `select()`, `filter()`, `withColumn()`, mixed with ordinary
DataFrame-API column expressions in the same call. Use `selectExpr` when the whole projection is
naturally SQL; use `expr()` when you want one SQL-flavored expression mixed into otherwise
DataFrame-API code.
</details>

---

**7. `CAST('not_a_number' AS DOUBLE)` doesn't raise an exception. What does it actually return, and
why does that matter for a data-quality pipeline?**

<details>
<summary>Answer</summary>

Verified: it returns `NULL`, with no error and no warning — same behavior category as
`inferSchema`'s handling of malformed rows (Module 02) and a numeric comparison against a null
value (Module 03). A genuinely corrupted or mistyped value becomes indistinguishable from data that
was legitimately missing. If the distinction matters, check for it before casting — e.g. count rows
where the raw string is non-null but the cast result is null — rather than trusting a failed cast
to raise.
</details>

---

**8. You register a DataFrame with `createOrReplaceTempView("x")` and separately save it with
`.write.saveAsTable("y")`. A colleague opens a brand-new Spark session against the same metastore.
Which one, if either, can they query — `x`, `y`, both, or neither?**

<details>
<summary>Answer</summary>

Only `y`. A temp view (`x`) is scoped to the exact `SparkSession` that created it (Module 03 Lesson
5) — a different session can't see it at all. A table created with `saveAsTable` (`y`) is
registered in the actual metastore (`isTemporary=False`, `tableType='MANAGED'`), so any session
connected to that metastore can query it.
</details>

---

**9. You `DROP TABLE` a table and the underlying Parquet files vanish from disk. You do the exact
same `DROP TABLE` on a different table and the files are untouched. What's the difference between
the two tables, and how do you check which kind you're looking at before dropping one?**

<details>
<summary>Answer</summary>

The first is a `MANAGED` table (created via `saveAsTable`, or `CREATE TABLE` with no `LOCATION`) —
Spark owns the data's location and lifecycle, so `DROP TABLE` deletes the files too, verified. The
second is an `EXTERNAL` table (`CREATE TABLE ... LOCATION '...'` pointing at data that already
existed) — Spark only owns the metastore reference, so `DROP TABLE` removes that reference and
leaves the files alone, also verified. Check `DESCRIBE EXTENDED <table>` (or
`spark.catalog.listTables()`'s `tableType`) for `MANAGED` vs `EXTERNAL` before dropping anything —
dropping a `MANAGED` table is a real, irreversible data delete.
</details>

---

**10. Does mixing SQL text (`spark.sql(...)`, `selectExpr(...)`, `expr(...)`) with the DataFrame
API in the same pipeline cost anything at execution time?**

<details>
<summary>Answer</summary>

No — as established in Module 03 Lesson 5, every path (DataFrame API calls, `spark.sql()` strings,
`expr()`/`selectExpr()` fragments) compiles down to the same Catalyst logical plan before
execution. Mixing them is purely a readability/maintainability choice made per expression, not a
performance trade-off.
</details>

---

Check the boxes in [`PROGRESS.md`](../PROGRESS.md) and move on to Module 05 when it's built.
