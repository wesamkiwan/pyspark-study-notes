# Lesson 1 — Ranking Functions: row_number, rank, and dense_rank

A window function computes a value across a set of related rows (the "window") without collapsing
them into one row per group, the way `groupBy` does. Every window function needs a `WindowSpec`:
which rows belong together (`partitionBy`) and what order they're considered in (`orderBy`).

## Defining a window and the three ranking functions

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, rank, dense_rank

w = Window.partitionBy("category").orderBy(col("amount").desc())

ranked = orders.select(
    "category", "product", "amount",
    row_number().over(w).alias("row_number"),
    rank().over(w).alias("rank"),
    dense_rank().over(w).alias("dense_rank"),
).orderBy("category", col("amount").desc())
```

`orders.csv` has genuine ties — five `Widget A` orders all at `250.0`, three `Widget B` orders all
at `410.5` — which is exactly what exposes how these three functions actually differ:

```
+-----------+--------+------+----------+----+----------+
|   category| product|amount|row_number|rank|dense_rank|
+-----------+--------+------+----------+----+----------+
|Electronics|Gadget Y|1200.0|         1|   1|         1|
|Electronics|Gadget Y|1200.0|         2|   1|         1|
|Electronics|Gadget X|899.99|         3|   3|         2|
|Electronics|Gadget X|899.99|         4|   3|         2|
|Electronics|Gadget X|899.99|         5|   3|         2|
|   Hardware|Widget B| 410.5|         1|   1|         1|
|   Hardware|Widget B| 410.5|         2|   1|         1|
|   Hardware|Widget B| 410.5|         3|   1|         1|
|   Hardware|Widget A| 250.0|         4|   4|         2|
|   Hardware|Widget A| 250.0|         5|   4|         2|
|   Hardware|Widget A| 250.0|         6|   4|         2|
|   Hardware|Widget A| 250.0|         7|   4|         2|
|   Hardware|Widget A| 250.0|         8|   4|         2|
|   Hardware|Widget C|150.75|         9|   9|         3|
|   Hardware|Widget C|150.75|        10|   9|         3|
+-----------+--------+------+----------+----+----------+
```

Verified — three genuinely different tie-handling rules:

- **`row_number()`**: always 1, 2, 3, 4, ... — every row gets a distinct number, ties broken
  arbitrarily (more on this below). Use when you need exactly one row per rank position, no matter
  what — the top-N-per-group pattern (Lesson 5) depends on this property specifically.
- **`rank()`**: tied rows share the same rank, and the **next** rank *skips* ahead by the number of
  tied rows — three `Widget B`s tie at rank 1, so the next distinct value (`Widget A`) starts at
  rank 4, not 2. This matches "1st, 1st, 1st, 4th" — the standard competition-ranking convention.
- **`dense_rank()`**: tied rows also share the same rank, but the next rank is always **one more**
  than the previous, regardless of how many rows tied — `Widget A` gets dense_rank 2 right after
  three-way-tied `Widget B` at 1. Use this when you want "how many distinct values outrank this
  one" rather than "how many rows outrank this one."

## The trap: `row_number()` isn't deterministic among ties without a tiebreaker

Look again at the five `Widget A` rows above — they all show `amount = 250.0`, and `row_number()`
assigned them `4, 5, 6, 7, 8` in *some* order. Which physical row gets `4` versus `8` is not
guaranteed by this window spec at all — Spark is free to break the tie however the data happens to
land across partitions and tasks, and that can differ between runs, or after a seemingly unrelated
change elsewhere in the pipeline (a different number of input partitions, a different Spark
version's tie-breaking behavior). If you've ever seen a "top 1 row per group" query return a
*different* row on a re-run of the identical pipeline against identical data, this is almost always
why.

**The fix: always add enough columns to `orderBy` to make the ordering fully deterministic** when
it matters which specific tied row wins — a unique ID column is the simplest guarantee:

```python
w_deterministic = Window.partitionBy("category").orderBy(col("amount").desc(), col("order_id"))
```

Now every row within a partition has a unique `(amount, order_id)` combination, so `row_number()`'s
assignment is fully determined and reproducible — no ties left for Spark to break arbitrarily.

---
**Next:** [Lesson 2 — Aggregate Window Functions: Running Totals](02-running-totals.md)
