# Lesson 5 — Schema Evolution and Mismatches

Real tables change shape over time — a new column gets added upstream, and your pipeline now has
old files with one schema and new files with another, sitting in the same directory. This lesson
covers what actually happens when you read across that mismatch, verified directly — including a
genuinely dangerous default behavior you need to know about.

## Setting up the scenario

```python
v1 = employees.select("emp_id", "name", "department")               # 3 columns — "old" data
v2 = employees.select("emp_id", "name", "department", "salary")      # 4 columns — "new" data

v1.write.mode("overwrite").parquet("data/evolving/v1")
v2.write.mode("overwrite").parquet("data/evolving/v2")

# reading both directories together, as a real pipeline would after a schema change upstream
combined = spark.read.parquet("data/evolving/v1", "data/evolving/v2")
```

## What happens without `mergeSchema` — verified, and worth taking seriously

```python
combined.printSchema()
combined.show()
```

Verified output:

```
root
 |-- emp_id: integer (nullable = true)
 |-- name: string (nullable = true)
 |-- department: string (nullable = true)

+------+--------------+-----------+
|emp_id|          name| department|
+------+--------------+-----------+
|     1|    Alice Chen|Engineering|
...
```

**No error. No warning. The `salary` column is simply gone** — not just from the v1 rows (where
it doesn't exist, which would make sense), but from **every row, including the v2 rows that
genuinely have a salary value on disk.** Spark picked one file's schema (here, whichever file it
happened to encounter first) and applied it uniformly to the entire read, silently discarding any
column that schema doesn't mention — even from files where that column actually exists.

**This is the real risk of schema drift with Parquet: it doesn't fail loudly, it loses data
quietly.** A downstream consumer sees a `salary` column go missing (or a table go from 4 columns
to 3) with nothing in the logs to explain why, because from Spark's point of view nothing failed
— the read "succeeded."

## The fix: `mergeSchema`

```python
combined = spark.read.option("mergeSchema", "true").parquet("data/evolving/v1", "data/evolving/v2")
combined.printSchema()
combined.show()
```

Verified output:

```
root
 |-- emp_id: integer (nullable = true)
 |-- name: string (nullable = true)
 |-- department: string (nullable = true)
 |-- salary: integer (nullable = true)

+------+--------------+-----------+------+
|emp_id|          name| department|salary|
+------+--------------+-----------+------+
|     1|    Alice Chen|Engineering|125000|
...
|    13|   Mona Farouk|Engineering|  NULL|   <- v1 rows correctly get null for the column they lack
```

With `mergeSchema=true`, Spark scans the schema of **every** file being read (a real cost —
an extra metadata pass — which is why it isn't the default) and produces the union of all
columns seen, filling `null` for any file/column combination that doesn't apply. This is the
behavior you actually want when reading data that spans a schema change.

## Best practice: don't rely on `mergeSchema` as your primary strategy

`mergeSchema` is a safety net for reading across an *already-occurred* schema change — it is not
a substitute for managing schema evolution deliberately:

- Prefer **additive-only** changes (new nullable columns) over renaming or changing the type of
  existing columns — additive changes merge cleanly; renames/type changes don't merge sensibly
  at all (Spark can't know `old_name` and `new_name` are "the same" column).
- For a table you control end-to-end, consider **explicitly migrating old files** to the new
  schema (a one-time backfill job) rather than leaning on `mergeSchema` to paper over the
  difference indefinitely — every read paying the merge-scan cost forever adds up.
- Module 11 (Delta Lake) covers a much stronger answer to this entire problem: Delta tables track
  schema explicitly and support controlled schema evolution (`mergeSchema` at write time, schema
  enforcement by default) rather than leaving it to chance at every read.

---
**Module 02 lessons complete.** Head to [`exercises/`](exercises/) before checking
[`solutions/`](solutions/), then take the [quiz](quiz.md).
