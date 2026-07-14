# Lesson 2 — Aggregate Window Functions: Running Totals

Ordinary aggregate functions (`sum`, `avg`, `min`, `max`, `count`) work as window functions too —
`.over(windowSpec)` instead of collapsing into a `groupBy` result, they compute the aggregate
**up to and including the current row**, keeping every row in the output.

## `rowsBetween`: an explicit, row-position-based frame

```python
from pyspark.sql.functions import sum as spark_sum

w = Window.partitionBy("category").orderBy("order_date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

orders.select("category", "order_date", "amount",
              spark_sum("amount").over(w).alias("running_total")) \
    .orderBy("category", "order_date").show()
```

```
+-----------+----------+------+------------------+
|   category|order_date|amount|     running_total|
+-----------+----------+------+------------------+
|Electronics|2023-02-11|899.99|            899.99|
|Electronics|2023-03-14|1200.0|           2099.99|
|Electronics|2023-05-05|899.99|2999.9799999999996|
|Electronics|2023-06-01|1200.0|           4199.98|
|Electronics|2023-07-19|899.99| 5099.969999999999|
|   Hardware|2023-01-05| 250.0|             250.0|
|   Hardware|2023-01-19| 410.5|             660.5|
|   Hardware|2023-02-02| 250.0|             910.5|
|   Hardware|2023-03-01|150.75|           1061.25|
|   Hardware|2023-04-02| 250.0|           1311.25|
|   Hardware|2023-04-19| 410.5|           1721.75|
|   Hardware|2023-05-22|150.75|            1872.5|
|   Hardware|2023-06-10| 250.0|            2122.5|
|   Hardware|2023-06-15| 250.0|            2372.5|
|   Hardware|2023-07-01| 410.5|            2783.0|
+-----------+----------+------+------------------+
```

Verified — a genuine running total per category, ordered by date, one incrementing value per row.
`Window.unboundedPreceding` means "every row from the start of this partition"; `Window.currentRow`
caps it at the current row — together, exactly the SQL `ROWS BETWEEN UNBOUNDED PRECEDING AND
CURRENT ROW` frame.

## Other frame shapes worth knowing

`rowsBetween` takes any two boundaries, not just "start to here":

```python
# a trailing 3-row moving average, including the current row
w_moving = Window.partitionBy("category").orderBy("order_date").rowsBetween(-2, Window.currentRow)

# total for the ENTIRE partition, repeated on every row (no ordering effect at all)
w_partition_total = Window.partitionBy("category").rowsBetween(
    Window.unboundedPreceding, Window.unboundedFollowing
)
```

The moving-average form is exactly how you'd implement a trailing-N-period metric (a 7-day moving
average, a trailing-3-order average) — `-2` means "2 rows before this one," so combined with
`currentRow` that's a 3-row window sliding forward. The partition-total form is a common way to
compute "this row's amount as a percentage of its category's total" without a separate `groupBy` +
join back to the original rows.

Every one of these is per-`category`, thanks to `partitionBy("category")` — this lesson deliberately
sidesteps a frame-definition trap specific to what happens when you *don't* pass an explicit
`rowsBetween` at all, which Lesson 4 covers once `lag`/`lead` (Lesson 3) round out the toolkit.

---
**Next:** [Lesson 3 — lag/lead: Period-over-Period and Gap Analysis](03-lag-lead-gap-analysis.md)
