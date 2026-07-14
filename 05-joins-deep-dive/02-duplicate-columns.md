# Lesson 2 — The Duplicate-Column Trap

Every join in Lesson 1 used `on="emp_id"` — a column-name **string**. That was deliberate. The
other common way to write a join, an explicit equality **condition**, looks almost identical but
behaves differently the moment you try to reference the join key afterward.

## Verified: joining on a condition keeps both sides' key columns

```python
joined = employees.join(orders, employees.emp_id == orders.emp_id, how="inner")
joined.columns
```

```
['emp_id', 'name', 'department', 'salary', 'hire_date', 'manager_id',
 'order_id', 'emp_id', 'product', 'category', 'amount', 'order_date', 'region']
```

`emp_id` appears **twice** — once from `employees`, once from `orders`. Both DataFrames had a
column with that name, and joining on an explicit condition (rather than a name) keeps both intact
rather than merging them. Now try to reference it:

```python
joined.select("emp_id").show()
```

```
AnalysisException: [AMBIGUOUS_REFERENCE] Reference `emp_id` is ambiguous, could be: [`emp_id`, `emp_id`].
```

Verified — this is not a hypothetical error message, it's exactly what Spark 3.5.3 raises. `"emp_id"`
as a bare string doesn't tell Spark which of the two identically-named columns you mean.

## Fix 1 (preferred when it applies): use `on="col_name"` instead of a condition

```python
employees.join(orders, on="emp_id", how="inner").columns
```

```
['emp_id', 'name', 'department', 'salary', 'hire_date', 'manager_id',
 'order_id', 'product', 'category', 'amount', 'order_date', 'region']
```

One `emp_id` column — Spark automatically coalesces the two sides' join columns into a single
output column when you join with a name string. This only works when the join key has the **same
name on both sides** and you want a single equi-join column; it can't express `employees.emp_id ==
orders.manager_id` or a multi-condition join with an `OR`.

## Fix 2 (when you need a condition): alias both sides, then select explicitly

For anything a name-string join can't express — different column names, non-equality conditions,
or you specifically want to keep both original columns (e.g. to compare them) — alias each
DataFrame and always reference columns through the alias:

```python
e = employees.alias("e")
o = orders.alias("o")

result = e.join(o, col("e.emp_id") == col("o.emp_id")) \
    .select(col("e.emp_id"), "name", "product")
result.show(3)
```

```
+------+---------+--------+
|emp_id|     name| product|
+------+---------+--------+
|     4|David Kim|Gadget Y|
|     4|David Kim|Widget A|
|     4|David Kim|Widget B|
+------+---------+--------+
only showing top 3 rows
```

`col("e.emp_id")` unambiguously picks the `employees` side's copy, which is otherwise indistinguishable
from `orders`'s copy by name alone.

**The rule:** default to `on="col_name"` (or `on=["col1", "col2"]` for a multi-column equi-join)
whenever the key has the same name on both sides — it sidesteps this entire class of error. Reach
for the alias-and-condition form only when you genuinely need it (different key names, a
non-equality condition, or intentionally keeping both sides' columns).

---
**Next:** [Lesson 3 — Broadcast vs Sort-Merge Joins](03-broadcast-vs-sortmerge.md)
