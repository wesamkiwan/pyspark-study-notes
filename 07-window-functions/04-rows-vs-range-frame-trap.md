# Lesson 4 — rowsBetween vs rangeBetween: The Default Frame Trap

Lesson 2's running total used an explicit `.rowsBetween(Window.unboundedPreceding,
Window.currentRow)`. What happens if you skip that and just write `Window.partitionBy(...).orderBy(...)`
with no frame at all? Spark doesn't leave the frame undefined — it silently picks one for you, and
that default is **not** the same as `rowsBetween`.

## Verified: the same running-total query, with and without an explicit frame

```python
# WITH an explicit ROWS frame (Lesson 2's version, ordered by amount this time)
w_rows = Window.partitionBy("category").orderBy("amount") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

orders.select("category", "product", "amount",
              spark_sum("amount").over(w_rows).alias("running_total_rows")) \
    .orderBy("category", "amount").show()
```

```
+-----------+--------+------+------------------+
|   category| product|amount|running_total_rows|
+-----------+--------+------+------------------+
|Electronics|Gadget X|899.99|            899.99|
|Electronics|Gadget X|899.99|           1799.98|
|Electronics|Gadget X|899.99|2699.9700000000003|
|Electronics|Gadget Y|1200.0|3899.9700000000003|
|Electronics|Gadget Y|1200.0|           5099.97|
|   Hardware|Widget C|150.75|            150.75|
|   Hardware|Widget C|150.75|             301.5|
|   Hardware|Widget A| 250.0|             551.5|
|   Hardware|Widget A| 250.0|             801.5|
|   Hardware|Widget A| 250.0|            1051.5|
|   Hardware|Widget A| 250.0|            1301.5|
|   Hardware|Widget A| 250.0|            1551.5|
|   Hardware|Widget B| 410.5|            1962.0|
|   Hardware|Widget B| 410.5|            2372.5|
|   Hardware|Widget B| 410.5|            2783.0|
+-----------+--------+------+------------------+
```

Now the **same query with no explicit frame at all** — just `partitionBy` + `orderBy`:

```python
w_default = Window.partitionBy("category").orderBy("amount")   # no .rowsBetween(...)!

orders.select("category", "product", "amount",
              spark_sum("amount").over(w_default).alias("running_total_default")) \
    .orderBy("category", "amount").show()
```

```
+-----------+--------+------+---------------------+
|   category| product|amount|running_total_default|
+-----------+--------+------+---------------------+
|Electronics|Gadget X|899.99|   2699.9700000000003|
|Electronics|Gadget X|899.99|   2699.9700000000003|
|Electronics|Gadget X|899.99|   2699.9700000000003|
|Electronics|Gadget Y|1200.0|              5099.97|
|Electronics|Gadget Y|1200.0|              5099.97|
|   Hardware|Widget C|150.75|                301.5|
|   Hardware|Widget C|150.75|                301.5|
|   Hardware|Widget A| 250.0|               1551.5|
|   Hardware|Widget A| 250.0|               1551.5|
|   Hardware|Widget A| 250.0|               1551.5|
|   Hardware|Widget A| 250.0|               1551.5|
|   Hardware|Widget A| 250.0|               1551.5|
|   Hardware|Widget B| 410.5|               2783.0|
|   Hardware|Widget B| 410.5|               2783.0|
|   Hardware|Widget B| 410.5|               2783.0|
+-----------+--------+------+---------------------+
```

**Look at the five tied `Widget A` rows.** With the explicit `ROWS` frame, they correctly show an
incrementing sequence: `551.5, 801.5, 1051.5, 1301.5, 1551.5` — one more `Widget A` added at each
step. With no explicit frame, **all five show the exact same value, `1551.5`** — the sum of *all
five combined*, on every single one of those rows.

## Why: the default is `RANGE`, and `RANGE` treats ties as one unit

An `orderBy(...)` with no frame defaults to
`RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` — and `RANGE` (unlike `ROWS`) doesn't count
*rows*, it groups by *value*. Every row sharing the current row's `ORDER BY` value (its "peers") is
included in the frame together, all at once, regardless of which physical row is being evaluated.
Since all five `Widget A` rows share `amount = 250.0`, `RANGE`'s frame for *every one of them*
extends through all five — so they all get the identical cumulative sum through that whole tied
group. `ROWS`, by contrast, only looks at physical row position — "the current row and everything
before it," which is what actually produces a genuine, incrementing running total.

**The rule: never rely on the default frame for a running total or any row-by-row cumulative
calculation.** Always write `.rowsBetween(Window.unboundedPreceding, Window.currentRow)` (or
whatever explicit `ROWS` frame you mean) explicitly, rather than trusting `Window.partitionBy(...)
.orderBy(...)` alone to mean what you think it means. This bug is especially dangerous because it's
**silent and data-dependent** — it produces a perfectly plausible-looking number on every row, and
only shows up as wrong the moment your ordering column happens to contain a duplicate, which may
not happen in your test data at all.

`RANGE` isn't useless — it's the correct choice specifically when your ordering column is something
like a timestamp and you want "every row within N units of this row's value" (a true time-based
window, via `rangeBetween` with numeric offsets on an appropriately-typed column) rather than a
row-position-based one. The trap is only in taking the *default* frame without deciding
deliberately which one — `ROWS` or `RANGE` — your calculation actually needs.

---
**Next:** [Lesson 5 — Top-N Per Group, and the No-partitionBy Performance Trap](05-top-n-and-no-partition-trap.md)
