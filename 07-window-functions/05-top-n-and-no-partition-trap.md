# Lesson 5 — Top-N Per Group, and the No-partitionBy Performance Trap

This closing lesson puts Lesson 1's `row_number()` (with its tiebreaker fix) to work on the single
most common real use of window functions — "top N rows per group" — and then looks at what a
window function actually costs physically, and the one config choice that turns that cost into a
scalability problem.

## Top-N per group: `row_number()` + `filter()`

```python
w = Window.partitionBy("category").orderBy(col("amount").desc(), col("order_id"))

top2 = orders.withColumn("rn", row_number().over(w)).filter(col("rn") <= 2)
top2.select("category", "product", "amount", "order_id", "rn").orderBy("category", "rn").show()
```

```
+-----------+--------+------+--------+---+
|   category| product|amount|order_id| rn|
+-----------+--------+------+--------+---+
|Electronics|Gadget Y|1200.0|    1006|  1|
|Electronics|Gadget Y|1200.0|    1011|  2|
|   Hardware|Widget B| 410.5|    1002|  1|
|   Hardware|Widget B| 410.5|    1008|  2|
+-----------+--------+------+--------+---+
```

Verified — the two highest-amount orders in each category, with `order_id` added to `orderBy` as
the tiebreaker Lesson 1 recommended, so which specific `Widget B` order lands at rank 1 vs 2 is
deterministic and reproducible, not left to chance. This pattern — `row_number()` over a
`partitionBy`+`orderBy` that expresses "best/most-recent per group," then `filter(rn <= N)` — is
the standard, idiomatic way to do "top N per group" in Spark; there's no dedicated
`top_n()` function, this combination *is* the tool.

## What a window function actually costs: a sort per partition, always

```python
orders.withColumn("rn", row_number().over(Window.partitionBy("category").orderBy(col("amount").desc()))).explain()
```

```
== Physical Plan ==
...
+- Window [row_number() ... ], [category], [amount DESC NULLS LAST]
   +- Sort [category ASC NULLS FIRST, amount DESC NULLS LAST], false, 0
      +- Exchange hashpartitioning(category, 8), ENSURE_REQUIREMENTS, [plan_id=...]
         +- FileScan csv [...]
```

Verified — every window function forces a `Sort` (by `partitionBy` column(s), then `orderBy`
column(s)) after an `Exchange` that hash-partitions the data by the `partitionBy` columns (Module
06 Lesson 2's hash partitioning, doing exactly the "same key, same partition" job it always does).
This is inherent to how window functions work — Spark has to physically group and order each
partition's rows before it can compute a rank or a running value across them, same wide-transform
shuffle cost as a `groupBy` or `join`.

## The trap: no `partitionBy` at all means everything shuffles to ONE partition

```python
orders.withColumn("global_rank", row_number().over(Window.orderBy(col("amount").desc()))).explain()
```

```
== Physical Plan ==
...
+- Window [row_number() ...], [amount DESC NULLS LAST]
   +- Sort [amount DESC NULLS LAST], false, 0
      +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=...]
         +- FileScan csv [...]
```

Verified — `Exchange SinglePartition`, in place of the `hashpartitioning(category, 8)` from the
partitioned version above. A window with no `partitionBy` has no grouping key to hash-partition by,
so Spark funnels **every row in the entire dataset** into a single partition to sort and rank it —
there's no way to parallelize a genuinely-global ranking across partitions, since row 1's rank
depends on knowing about every other row. On this course's 15-row `orders.csv` that's invisible.
On a real dataset of any real size, a window with no `partitionBy` is a guaranteed full-data
single-partition bottleneck — exactly the same "everything funnels into one task" shape as Module
06's skew problem, except here it's not an accident of key distribution, it's structurally
unavoidable given the query as written.

**The rule: if you find yourself writing `Window.orderBy(...)` with no `partitionBy` at all on
data of any real size, stop and ask whether the ranking genuinely needs to be global.** Often it
doesn't — "rank within each region," "top N within each customer" — and adding the `partitionBy`
you actually meant turns an unavoidable single-partition bottleneck back into a parallelizable,
per-partition sort. If the ranking truly is global (a leaderboard across all data, with no natural
grouping), that single-partition cost is inherent to the problem, not a mistake — but it's worth
recognizing it as a deliberate, known scalability ceiling rather than discovering it as a surprise
when the job that ran fine on a sample suddenly doesn't on the full dataset.

---
This closes out Module 07. Next: [`exercises/`](exercises/), then [`quiz.md`](quiz.md).
