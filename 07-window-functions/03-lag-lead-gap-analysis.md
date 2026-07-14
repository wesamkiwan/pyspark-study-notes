# Lesson 3 — lag/lead: Period-over-Period and Gap Analysis

`lag(col, n)` reaches back `n` rows *within the current partition, in the current order* and
brings that row's value forward; `lead(col, n)` reaches forward the same way. Both default to
`n=1`. Together they're the standard tool for "compare this row to the previous/next one" —
period-over-period changes, and finding gaps between events.

## Verified: previous order's amount and the day-gap since it, per category

```python
from pyspark.sql.functions import lag, datediff

w = Window.partitionBy("category").orderBy("order_date")

gapped = orders.select(
    "category", "order_date", "amount",
    lag("amount").over(w).alias("prev_amount"),
    lag("order_date").over(w).alias("prev_date"),
).withColumn("days_since_prev", datediff(col("order_date"), col("prev_date"))) \
 .withColumn("amount_change", col("amount") - col("prev_amount"))

gapped.orderBy("category", "order_date").show()
```

```
+-----------+----------+------+-----------+----------+---------------+-------------+
|   category|order_date|amount|prev_amount| prev_date|days_since_prev|amount_change|
+-----------+----------+------+-----------+----------+---------------+-------------+
|Electronics|2023-02-11|899.99|       NULL|      NULL|           NULL|         NULL|
|Electronics|2023-03-14|1200.0|     899.99|2023-02-11|             31|       300.01|
|Electronics|2023-05-05|899.99|     1200.0|2023-03-14|             52|      -300.01|
|Electronics|2023-06-01|1200.0|     899.99|2023-05-05|             27|       300.01|
|Electronics|2023-07-19|899.99|     1200.0|2023-06-01|             48|      -300.01|
|   Hardware|2023-01-05| 250.0|       NULL|      NULL|           NULL|         NULL|
|   Hardware|2023-01-19| 410.5|      250.0|2023-01-05|             14|        160.5|
|   Hardware|2023-02-02| 250.0|      410.5|2023-01-19|             14|       -160.5|
|   Hardware|2023-03-01|150.75|      250.0|2023-02-02|             27|       -99.25|
|   Hardware|2023-04-02| 250.0|     150.75|2023-03-01|             32|        99.25|
|   Hardware|2023-04-19| 410.5|      250.0|2023-04-02|             17|        160.5|
|   Hardware|2023-05-22|150.75|      410.5|2023-04-19|             33|      -259.75|
|   Hardware|2023-06-10| 250.0|     150.75|2023-05-22|             19|        99.25|
|   Hardware|2023-06-15| 250.0|      250.0|2023-06-10|              5|          0.0|
|   Hardware|2023-07-01| 410.5|      250.0|2023-06-15|             16|        160.5|
+-----------+----------+------+-----------+----------+---------------+-------------+
```

Verified. Two things worth noticing:

- **The first row of every partition is `NULL`** for both `lag` columns — there's no row before
  the first one to look back at. This is expected, not missing data; `lag`/`lead` return `NULL`
  whenever the requested offset falls outside the partition. `lag("col", 1, default_value)` takes
  a third argument if you'd rather substitute something else (`0`, an empty string) instead of
  `NULL` for that edge.
- **`amount_change` can legitimately be negative** — that's the whole point of `lag` for
  period-over-period comparisons: it's just `current - previous`, and a decrease is real, valid
  information (e.g. flagging that Electronics revenue dropped between two consecutive orders),
  not an error condition.

## `lead`: the mirror image, for "what comes next"

```python
from pyspark.sql.functions import lead

orders.select("category", "order_date", lead("order_date").over(w).alias("next_date")) \
    .orderBy("category", "order_date").show(5)
```

```
+-----------+----------+----------+
|   category|order_date| next_date|
+-----------+----------+----------+
|Electronics|2023-02-11|2023-03-14|
|Electronics|2023-03-14|2023-05-05|
|Electronics|2023-05-05|2023-06-01|
|Electronics|2023-06-01|2023-07-19|
|Electronics|2023-07-19|      NULL|
+-----------+----------+----------+
only showing top 5 rows
```

The *last* row of a partition gets `NULL` from `lead`, symmetric to `lag`'s `NULL` on the first row
— there's nothing after the last order to look forward to. `datediff(lead("order_date").over(w),
col("order_date"))` would give you "days until the *next* order," the forward-looking twin of the
`days_since_prev` column above — same technique, opposite direction, useful when the question is
naturally phrased as "how long until..." rather than "how long since...".

---
**Next:** [Lesson 4 — rowsBetween vs rangeBetween: The Default Frame Trap](04-rows-vs-range-frame-trap.md)
