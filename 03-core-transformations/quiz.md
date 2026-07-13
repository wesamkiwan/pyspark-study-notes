# Module 03 Quiz

Answer each yourself before expanding the answer.

---

**1. `employees.withColumn("bonus_flag", True)` fails immediately. Why, and what's the fix?**

<details>
<summary>Answer</summary>

`withColumn`'s second argument must be a `Column` expression, not a raw Python value — verified,
this raises `PySparkTypeError: [NOT_COLUMN] Argument \`col\` should be a Column, got bool.`
Wrap the constant with `lit()`: `withColumn("bonus_flag", lit(True))`.
</details>

---

**2. If you call `.withColumn("salary", ...)` on a DataFrame that already has a `salary` column,
what happens? Is this an error?**

<details>
<summary>Answer</summary>

No error — the existing `salary` column is silently replaced by the new expression. This is
normal, documented `withColumn` behavior, but it means an accidental name collision (e.g. a typo)
won't raise a warning; it'll just quietly overwrite that column.
</details>

---

**3. `employees.filter(col("salary") > 70000 & col("department") == "Sales")` fails. What's
actually wrong, and what's the general rule to avoid this?**

<details>
<summary>Answer</summary>

Python operator precedence: `&` binds tighter than `>`/`==`, so this parses as
`col("salary") > (70000 & col("department")) == "Sales"` — nonsense, verified to fail with a
`Py4JError` about calling `.and()` on an Integer. The rule: every individual condition combined
with `&`/`|`/`~` must have its own parentheses, always, with no exceptions.
</details>

---

**4. A column has a genuinely missing (null) salary. You write `.filter(col("salary") > 70000)`
without an `isNull()` check. Does that row show up in the result, and why?**

<details>
<summary>Answer</summary>

No — a comparison against `NULL` evaluates to `NULL`, not `False`, and `NULL` conditions are
treated as non-matching by `filter`. The row is silently excluded, not included and not an error.
</details>

---

**5. In a `when().when().otherwise()` chain, you forget to add a branch that explicitly handles a
null input column. Where does a row with that null value end up, and why is that dangerous?**

<details>
<summary>Answer</summary>

It falls through to `.otherwise(...)` — every numeric/string comparison against null evaluates to
null (not `False`), so the row never matches any `.when()` branch and lands in the default case.
This is dangerous because a genuinely-missing value silently gets mislabeled as whatever your
`otherwise` case represents (e.g. the lowest tier), rather than being flagged as unknown/missing.
</details>

---

**6. `employees.groupBy("department").agg(avg("salary"), count("*"))` produces columns named
`avg(salary)` and `count(1)`. What's the fix, and why does it matter beyond aesthetics?**

<details>
<summary>Answer</summary>

Alias every aggregate: `.agg(avg("salary").alias("avg_salary"), count("*").alias("headcount"))`.
Un-aliased names are awkward to reference downstream (need backticks), unreadable in a larger
pipeline, and coupled to the exact expression that produced them — renaming/refactoring the
expression later silently changes the column name everywhere it's referenced.
</details>

---

**7. How do you filter a `groupBy().agg(...)` result down to only groups meeting some condition on
the aggregated value (SQL's `HAVING`)? Is there a separate `.having()` method?**

<details>
<summary>Answer</summary>

There's no separate `.having()` — you just call `.filter()` (or `.where()`) *after* `.agg()`,
which then operates on the aggregated result. The position of `.filter()` in the chain (before vs.
after grouping) is what determines whether it behaves like `WHERE` or `HAVING`.
</details>

---

**8. `west.union(east)` runs with no error, but a filter on `combined.order_id` for known east-side
order IDs returns zero rows. What actually went wrong, and what's the fix?**

<details>
<summary>Answer</summary>

Verified: `union()` matches columns by **position**, not by name. `east`'s columns were in a
different order than `west`'s, so `east`'s `order_id` values landed under whatever column `west`
had in that position, silently mislabeling the data — no error, because the types happened to be
compatible. The fix is `unionByName(...)`, which aligns columns by name instead of position.
</details>

---

**9. `spark.sql(...)` and the equivalent `.groupBy().agg()` DataFrame-API code produce identical
results here. Is one faster than the other? What should actually drive the choice between them?**

<details>
<summary>Answer</summary>

Neither is faster — both compile to the same underlying Catalyst logical plan. The choice should
be about readability and maintainability for the specific logic (SQL often reads better for
multi-table joins; the DataFrame API wins when a query needs to be built programmatically or
reused as composable functions) — not performance.
</details>

---

**10. Why is building a `spark.sql(f"... WHERE region = '{user_input}' ...")` string from
untrusted input dangerous, and what's the safe alternative?**

<details>
<summary>Answer</summary>

It's a SQL injection vulnerability, identical in nature to injection risks in any other system
that builds queries from string concatenation — Spark does not sanitize this. The safe
alternative is to stay in the DataFrame API for anything driven by external input, e.g.
`orders.filter(col("region") == user_input)`, where the value is only ever compared, never parsed
as part of the query text.
</details>

---

**11. A temp view registered with `employees.createOrReplaceTempView("employees")` can't be found
from `spark.newSession()`. Why, and what would fix that specific need?**

<details>
<summary>Answer</summary>

Temp views are scoped to the `SparkSession` that created them, verified: a new session raises
`AnalysisException: [TABLE_OR_VIEW_NOT_FOUND]`. Use `createOrReplaceGlobalTempView(...)` instead,
and query it via the `global_temp` prefix (`SELECT * FROM global_temp.employees`) if it needs to
be visible across multiple sessions in the same application.
</details>

---

Check the boxes in [`PROGRESS.md`](../PROGRESS.md) and move on to Module 04 when it's built.
