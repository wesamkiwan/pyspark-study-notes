# Lesson 5 — NULL Join Keys and the Accidental Cartesian Product

Two independent traps to close out this module — one about what a `NULL` join key does (or rather,
never does), and one about what happens when a join has no real condition at all.

## `NULL` never matches `NULL` in a join key — not even to itself

Module 04 Lesson 3 established SQL's three-valued logic: `NULL = NULL` evaluates to `NULL`, not
`TRUE`. A join's matching condition is an equality check, so this applies directly to join keys —
verified with two tiny DataFrames that both have a `NULL` in their key column:

```python
from pyspark.sql import Row

a = spark.createDataFrame([Row(k=1, v="a"), Row(k=None, v="b")])
b = spark.createDataFrame([Row(k=1, v2="x"), Row(k=None, v2="y")])

a.join(b, on="k", how="inner").show()
```

```
+---+---+---+
|  k|  v| v2|
+---+---+---+
|  1|  a|  x|
+---+---+---+
```

Only the `k=1` row matched. **The two `NULL` rows did not join to each other**, even though both
literally have a `NULL` in the same column — because `NULL == NULL` is `NULL`, not `TRUE`, and a
join condition that evaluates to `NULL` is treated as non-matching, same as `WHERE`. If your data
genuinely has "unknown"/missing keys on both sides that you *want* to associate with each other,
this join will not do that for you; you'd need to explicitly handle it (e.g. `COALESCE` both keys
to the same real sentinel value before joining, deliberately, only if that's actually the semantics
you want).

This is usually the behavior you *want* — two genuinely-unrelated "missing key" rows shouldn't
silently pair up — but it's worth knowing explicitly, rather than being surprised when rows with
null keys vanish from an inner join and you assumed nulls would at least match each other.

## The accidental cartesian product: joins default to allowing it, silently

`.join()` with **no condition and no `on=`** doesn't raise an error by default — it executes as a
full cartesian product, pairing every row on the left with every row on the right:

```python
employees.select("emp_id", "name").join(orders.select("order_id", "product")).count()
```

```
225
```

15 employees × 15 orders = 225 — every possible pairing, almost certainly not what was intended if
this `.join()` call was missing a condition by mistake (a typo'd column name that silently produced
no condition, a refactor that dropped the `on=` argument). **There is no error, no warning — just a
result set that's dramatically and silently larger than expected**, which on real-sized tables
(millions × thousands of rows) is the kind of mistake that can produce a multi-billion-row result
and either run for hours or exhaust cluster memory before anyone notices the join condition was
wrong.

This is allowed by default because `spark.sql.crossJoin.enabled` defaults to `true`. Setting it to
`false` turns a missing condition into an immediate, loud failure instead of a silent (and
expensive) surprise:

```python
spark.conf.set("spark.sql.crossJoin.enabled", "false")
employees.select("emp_id", "name").join(orders.select("order_id", "product")).count()
```

```
AnalysisException: Detected implicit cartesian product for INNER join between logical plans
...
Join condition is missing or trivial.
Either: use the CROSS JOIN syntax to allow cartesian products between these relations, ...
```

Verified — with the config disabled, the exact same call is blocked immediately with a clear
explanation, rather than quietly computing 225 rows.

**The rule:** if you genuinely want a cartesian product (rare, but real — e.g. generating every
combination of a small set of dimensions), say so explicitly with `.crossJoin(...)`, which works
regardless of this setting because it states intent directly. For everything else, consider setting
`spark.sql.autoBroadcastJoinThreshold` aside for a moment and specifically flipping
`spark.sql.crossJoin.enabled` to `false` in any pipeline where an accidentally-missing join
condition would be expensive or dangerous — it converts a silent correctness bug into a loud,
immediate one, which is almost always the trade you want.

---
This closes out Module 05. Next: [`exercises/`](exercises/), then [`quiz.md`](quiz.md).
