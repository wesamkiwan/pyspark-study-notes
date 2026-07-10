# Lesson 3 — Reading Data Like a Pro

Real data is messy: wrong types, missing fields, extra fields, encoding issues. Spark gives you
explicit control over what happens when a record doesn't match your schema — but only if you
know the options exist. This lesson uses a deliberately messy file,
[`data/messy_orders.csv`](../data/messy_orders.csv), which has three broken rows:

```csv
order_id,emp_id,product,category,amount,order_date,region
...
1003,5,Widget A,Hardware,not_a_number,2023-02-02,East      <- amount isn't numeric
1005,11,Widget C,Hardware,150.75,2023-03-01                <- missing the region field
1007,4,Widget A,Hardware,250.00,2023-04-02,West,UNEXPECTED_EXTRA_FIELD  <- one field too many
...
```

## The three parse modes

```python
schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("emp_id", IntegerType()),
    StructField("product", StringType()),
    StructField("category", StringType()),
    StructField("amount", DoubleType()),
    StructField("order_date", DateType()),
    StructField("region", StringType()),
])

# PERMISSIVE (the default): keep every row, null out fields that don't fit
spark.read.csv(path, header=True, schema=schema, mode="PERMISSIVE")

# DROPMALFORMED: silently drop any row that doesn't fit the schema
spark.read.csv(path, header=True, schema=schema, mode="DROPMALFORMED")

# FAILFAST: raise an exception the moment a bad row is found
spark.read.csv(path, header=True, schema=schema, mode="FAILFAST")
```

Verified output for **PERMISSIVE** (the default if you don't specify `mode` at all):

```
+--------+------+--------+-----------+------+----------+------+
|order_id|emp_id|product |category   |amount|order_date|region|
+--------+------+--------+-----------+------+----------+------+
|1001    |4     |Widget A|Hardware   |250.0 |2023-01-05|West  |
|1002    |4     |Widget B|Hardware   |410.5 |2023-01-19|West  |
|1003    |5     |Widget A|Hardware   |NULL  |2023-02-02|East  |   <- bad amount -> null
|1004    |6     |Gadget X|Electronics|899.99|2023-02-11|West  |
|1005    |11    |Widget C|Hardware   |150.75|2023-03-01|NULL  |   <- missing field -> null
|1006    |5     |Gadget Y|Electronics|1200.0|2023-03-14|East  |
|1007    |4     |Widget A|Hardware   |250.0 |2023-04-02|West  |   <- extra field just dropped
|1008    |6     |Widget B|Hardware   |410.5 |2023-04-19|West  |
+--------+------+--------+-----------+------+----------+------+
```

Notice: PERMISSIVE doesn't tell you *anything* went wrong here by default — bad values just
become `null`, silently, mixed in with genuinely-null values you might have expected in your
data. That ambiguity is the whole problem with trusting PERMISSIVE blindly.

## Recovering *which* rows were bad: `_corrupt_record`

Add a matching column to your schema and point `columnNameOfCorruptRecord` at it:

```python
schema_with_corrupt = schema.add("_corrupt_record", StringType())
spark.read.csv(
    path, header=True,
    schema=schema_with_corrupt,
    mode="PERMISSIVE",
    columnNameOfCorruptRecord="_corrupt_record",
)
```

Verified output — the `_corrupt_record` column holds the *original raw line* for any row that
didn't fully match the schema, and is `null` for rows that parsed cleanly:

```
+--------+------+--------+-----------+------+----------+------+------------------------------------------------------------+
|order_id|emp_id|product |category   |amount|order_date|region|_corrupt_record                                              |
+--------+------+--------+-----------+------+----------+------+------------------------------------------------------------+
|1001    |4     |Widget A|Hardware   |250.0 |2023-01-05|West  |NULL                                                         |
|1003    |5     |Widget A|Hardware   |NULL  |2023-02-02|East  |1003,5,Widget A,Hardware,not_a_number,2023-02-02,East        |
|1005    |11    |Widget C|Hardware   |150.75|2023-03-01|NULL  |1005,11,Widget C,Hardware,150.75,2023-03-01                  |
|1007    |4     |Widget A|Hardware   |250.0 |2023-04-02|West  |1007,...,UNEXPECTED_EXTRA_FIELD                              |
+--------+------+--------+-----------+------+----------+------+------------------------------------------------------------+
```

This is the **production-grade pattern**: read with `_corrupt_record` captured, then explicitly
branch — write good rows to your target table, and write `_corrupt_record IS NOT NULL` rows to a
quarantine/dead-letter table for someone to investigate. Never let bad data disappear silently,
and never let it flow downstream silently either.

> **Gotcha, verified the hard way:** `StructType.add(...)` mutates the schema **in place** and
> also returns it — it is not a safe, side-effect-free builder call. `schema_with_corrupt =
> schema.add(...)` also silently modifies the original `schema` variable. If you reuse `schema`
> elsewhere after calling `.add()` on a copy of it, you'll be surprised to find the corrupt-record
> column already there. Build schemas for different purposes from independent `StructType([...])`
> literals, or explicitly copy first, rather than deriving one from another with `.add()`.

## A dangerous surprise: `.count()` lies when a corrupt-record column is involved

This is the single most important warning in this lesson, and it goes further than you'd expect
— verified directly against this exact file, not assumed.

**First symptom — `DROPMALFORMED`:**

```python
result = spark.read.csv(path, header=True, schema=schema, mode="DROPMALFORMED")
result.show()          # correctly shows only the 5 clean rows
result.count()          # returns 8 -- NOT 5!
len(result.collect())    # returns 5 -- the correct number
```

**It gets worse.** The bug isn't limited to `DROPMALFORMED` mode — it follows the
`_corrupt_record` column itself, even through an explicit filter you write yourself:

```python
schema_with_corrupt = schema.add("_corrupt_record", StringType())
df = spark.read.csv(path, header=True, schema=schema_with_corrupt,
                     mode="PERMISSIVE", columnNameOfCorruptRecord="_corrupt_record")

clean = df.filter(col("_corrupt_record").isNull())
quarantine = df.filter(col("_corrupt_record").isNotNull())

clean.count()             # -> 8  (WRONG — should be 5)
len(clean.collect())        # -> 5  (correct)
quarantine.count()         # -> 0  (WRONG — should be 3!)
len(quarantine.collect())    # -> 3  (correct)
```

Verified control cases that rule out "filters after a read are broken in general": filtering on
an ordinary column (`region == "West"`) and calling `.count()` gives the correct answer (5 == 5).
Reading the **same file** with a schema that has **no** `_corrupt_record` column at all, then
filtering and counting, is also correct (7 == 7). **The unreliable `.count()` behavior is
specifically tied to having a `_corrupt_record` (or any `columnNameOfCorruptRecord`) column in
the schema at all** — once that column exists, `.count()` on that DataFrame or anything derived
from it becomes untrustworthy, even after you've filtered it yourself.

**The fix is specifically about forcing real materialization — not about which counting API you
use.** Verified: swapping `.count()` for `agg(count("*"))` alone does **not** help
(`clean.agg(count("*")).collect()[0][0]` still returns the wrong `8`) — Catalyst applies the same
shortcut through an aggregate just as easily as through `.count()`. What actually works is forcing
Spark to materialize the filtered rows for real, *before* asking for a count:

```python
clean.cache()
clean.count()                # -> 5, correct, because cache() forces real materialization first

# equally reliable — checkpointing also forces materialization:
clean.localCheckpoint().count()   # -> 5, correct
```

**The practical rule this produces: the moment you request a corrupt-record column, stop trusting
any count on that DataFrame (or anything derived from it) unless you've first forced real
materialization** — `.cache()` or `.localCheckpoint()` before counting, `len(df.collect())` for
small data, or counting rows after an actual `.write.*()`. Changing *how* you count (`.count()`
vs. `agg(count("*"))`) does nothing on its own; what matters is forcing materialization so the
optimizer's shortcut never has a chance to apply. This is exactly the kind of landmine that
silently inflates a "rows processed" metric
in a monitoring dashboard while the real output table — or worse, your quarantine/dead-letter
table — has far fewer rows than reported. A `quarantine.count()` silently reporting `0` when 3
rows actually need human review is a genuine, serious data-quality incident waiting to happen.

## FAILFAST for pipelines that must not tolerate bad data

```python
try:
    spark.read.csv(path, header=True, schema=schema, mode="FAILFAST").show()
except Exception as e:
    print("Bad data detected:", e)
```

Verified: this raises immediately with `[MALFORMED_RECORD_IN_PARSING...]`, naming the exact
offending row. Use `FAILFAST` for pipelines where malformed input should stop the pipeline and
page someone, rather than silently produce a partial or null-filled result.

---
**Next:** [Lesson 4 — Writing Data Like a Pro](04-writing-data-like-a-pro.md)
