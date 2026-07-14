# Module 05 Quiz

Answer each yourself before expanding the answer.

---

**1. `employees.join(orders, on="emp_id", how="left").count()` returns 25 rows, even though only
4 of the 15 employees have ever placed an order. Where do the other 21 rows come from?**

<details>
<summary>Answer</summary>

14 rows are actual matches (the 4 employees with orders, one output row per order — some placed
several), and 11 rows are the employees with zero matching orders, each appearing once with every
`orders` column as `NULL`. `LEFT` keeps every left row at least once; it doesn't collapse the
matched ones down to one row per employee.
</details>

---

**2. Why does `right.filter(col("name").isNull())` on a `RIGHT` join surface the order placed
under `emp_id = 999`, and what real-world problem does this query pattern detect?**

<details>
<summary>Answer</summary>

`RIGHT` keeps every row from `orders`, filling `NULL` for `employees` columns wherever no matching
`emp_id` exists — the `emp_id = 999` order has no such employee, so its `employees`-side columns
(including `name`) come back `NULL`. Filtering for that reveals it. This is exactly how you find
referential-integrity violations: child rows whose parent key doesn't actually exist in the parent
table.
</details>

---

**3. `LEFT SEMI` and `INNER` both only return rows for employees who have at least one order. What's
actually different between them?**

<details>
<summary>Answer</summary>

`INNER` returns one row per *matching order* (an employee with 3 orders produces 3 rows, joined
with the right side's columns). `LEFT SEMI` returns one row per matching *employee*, regardless of
how many orders they have, using only the left side's columns — it's a membership check, not a
row-multiplying join.
</details>

---

**4. `employees.join(orders, employees.emp_id == orders.emp_id, how="inner").select("emp_id")`
raises `AnalysisException: [AMBIGUOUS_REFERENCE]`. Why, and what's the simplest fix here?**

<details>
<summary>Answer</summary>

Joining on an explicit condition (rather than a column-name string) keeps both sides' `emp_id`
columns in the result — verified, `joined.columns` shows `emp_id` twice. `"emp_id"` as a bare
string doesn't say which copy you mean. The simplest fix, whenever the key has the same name on
both sides: join with `on="emp_id"` instead of a condition — Spark then coalesces it into a single
column automatically.
</details>

---

**5. You need to join on `employees.emp_id == orders.manager_id` — different column names, so
`on="col_name"` can't express it. How do you reference the columns afterward without hitting the
ambiguity error?**

<details>
<summary>Answer</summary>

Alias both DataFrames (`e = employees.alias("e")`, `o = orders.alias("o")`), join on
`col("e.emp_id") == col("o.manager_id")`, and reference every column through its alias afterward
(`col("e.emp_id")`, `col("o.manager_id")`) rather than by bare name.
</details>

---

**6. `employees.join(orders, on="emp_id").explain()` shows `BroadcastHashJoin` even though you
never called `broadcast()`. Why, and what config controls this?**

<details>
<summary>Answer</summary>

`spark.sql.autoBroadcastJoinThreshold` defaults to 10MB — Spark estimates each side's size and
automatically broadcasts one if it's under that threshold, with no hint required. Both course CSVs
are tiny, so this kicks in by default. Verified: setting the threshold to `-1` (disabling automatic
broadcast) switches the identical query's plan to `SortMergeJoin` with an `Exchange` shuffle on each
side — same query, same data, different plan, purely from the config.
</details>

---

**7. When is forcing `broadcast()` a bad idea, even though it skips a shuffle?**

<details>
<summary>Answer</summary>

When the "small" side isn't actually small enough to fit comfortably in every executor's memory.
Broadcasting copies the *entire* side to *every* executor — if that side is bigger than it looks
(e.g. Spark's size estimate is stale after several transformations), you get executor
out-of-memory errors instead of the shuffle you were trying to avoid.
</details>

---

**8. What symptom in a running job suggests data skew specifically, rather than just "the cluster
is slow"?**

<details>
<summary>Answer</summary>

A stage that's nearly finished except one (or a handful) of tasks that stay "running" far longer
than the rest of that same stage's tasks, typically right after a `join` or `groupBy` — because one
key's disproportionate share of rows landed on a single partition/task while every other task
finished quickly and now sits idle.
</details>

---

**9. `a.join(b, on="k", how="inner")` where both `a` and `b` have exactly one row with `k = NULL`
returns zero rows for that pair. Why don't the two `NULL` keys match each other?**

<details>
<summary>Answer</summary>

A join condition is an equality check, and SQL's three-valued logic (Module 04 Lesson 3) makes
`NULL == NULL` evaluate to `NULL`, not `TRUE`. A join condition that evaluates to `NULL` is treated
as non-matching, exactly like `WHERE`. Verified: two rows that both literally have a null key still
don't join to each other.
</details>

---

**10. `a.select("x").join(b.select("y")).count()` (no condition, no `on=`) runs with no error and
returns `len(a) * len(b)` rows. Why does this not fail immediately, and how do you make a missing
join condition fail loudly instead?**

<details>
<summary>Answer</summary>

`spark.sql.crossJoin.enabled` defaults to `true`, so a join with no condition silently executes as
a full cartesian product rather than raising an error. Verified: setting
`spark.sql.crossJoin.enabled` to `"false"` makes the identical call raise
`AnalysisException: Detected implicit cartesian product` immediately instead. If you genuinely want
a cartesian product, use `.crossJoin(...)` explicitly, which works regardless of the setting.
</details>

---

Check the boxes in [`PROGRESS.md`](../PROGRESS.md) and move on to Module 06 when it's built.
