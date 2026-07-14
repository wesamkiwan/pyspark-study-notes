# Module 07 Quiz

Answer each yourself before expanding the answer.

---

**1. Three `Widget B` orders in `orders.csv` all share `amount = 410.5`. Over
`Window.partitionBy("category").orderBy(col("amount").desc())`, what rank does each of the three
get from `rank()`, and what rank does the next-lower-amount row get?**

<details>
<summary>Answer</summary>

Verified: all three tied `Widget B` rows get `rank() = 1`. The next distinct amount
(`Widget A` at `250.0`) gets `rank() = 4`, not `2` — `rank()` skips ahead by the number of tied
rows. `dense_rank()` would instead give `Widget A` a `2`, since it always increments by exactly 1
regardless of how many rows tied for the previous rank.
</details>

---

**2. Why does `row_number()` need an explicit tiebreaker column (like a unique ID) added to
`orderBy` to be safe for production use, when `Window.partitionBy("category").orderBy(col("amount").desc())`
already runs without error?**

<details>
<summary>Answer</summary>

When multiple rows share the same `orderBy` value, `row_number()`'s specific assignment among those
tied rows (which one gets `4` vs `5` vs `6`) is not deterministic — Spark can break the tie
differently depending on partition layout, task scheduling, or even Spark version, without any
error being raised. Adding a unique column (e.g. `order_id`) to `orderBy` removes all ties, making
the assignment fully deterministic and reproducible.
</details>

---

**3. What's the difference between `Window.partitionBy("category").orderBy("order_date").rowsBetween(-2, Window.currentRow)`
and `.rowsBetween(Window.unboundedPreceding, Window.currentRow)`?**

<details>
<summary>Answer</summary>

`rowsBetween(-2, Window.currentRow)` is a trailing 3-row window (2 rows before the current one,
plus the current row) — a moving average/sum over the last 3 rows, sliding forward with each row.
`rowsBetween(Window.unboundedPreceding, Window.currentRow)` includes every row from the start of
the partition through the current row — a genuine cumulative running total, growing without bound
as you move through the partition.
</details>

---

**4. `lag("amount").over(w)` returns `NULL` for the first row of every partition. Is that a bug, a
sign of missing data, or expected behavior?**

<details>
<summary>Answer</summary>

Expected behavior, not a bug or missing data — there's no row before the first row of a partition
for `lag` to look back at, so it returns `NULL` for that position (verified). `lead()` is the
mirror image: the *last* row of a partition gets `NULL`, since there's nothing after it. Both
support a third argument to substitute a different default instead of `NULL`.
</details>

---

**5. Five `Widget A` rows all share `amount = 250.0`. A running-total query with an explicit
`rowsBetween(Window.unboundedPreceding, Window.currentRow)` frame shows 5 different, incrementing
totals for them. The identical query with NO explicit frame shows all 5 rows with the exact same
total. What default frame is Spark actually using in the second case, and why does that produce
this result?**

<details>
<summary>Answer</summary>

`orderBy(...)` with no explicit frame defaults to `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT
ROW`. Unlike `ROWS`, which counts physical row position, `RANGE` groups by *value* — every row that
shares the current row's `orderBy` value (its "peers") is included in the frame together, for every
one of those peer rows. Since all five `Widget A` rows share the same `amount`, `RANGE`'s frame
for each of them extends through all five, giving each an identical cumulative sum through the
whole tied group — verified, all five show `1551.5`, the sum of all five combined.
</details>

---

**6. Given Question 5's answer, what's the rule for writing a running-total or other row-by-row
cumulative calculation safely?**

<details>
<summary>Answer</summary>

Always specify an explicit `ROWS` frame (`.rowsBetween(Window.unboundedPreceding,
Window.currentRow)` or whatever specific `ROWS` frame is intended) rather than relying on the
default. This bug is especially dangerous because it's silent and data-dependent — it produces a
plausible-looking number on every row and only reveals itself as wrong once the ordering column
happens to contain a duplicate, which test data might not happen to include.
</details>

---

**7. Is `RANGE` ever the *correct* choice, rather than always a trap to avoid?**

<details>
<summary>Answer</summary>

Yes — `RANGE` (via `rangeBetween` with numeric offsets on an appropriately-typed ordering column,
e.g. a timestamp) is the right tool specifically when you want "every row within N units of this
row's *value*," a true value-based window, rather than a row-position-based one. The trap isn't
`RANGE` itself; it's taking the *default* frame without deliberately deciding whether the
calculation actually needs `ROWS` or `RANGE` semantics.
</details>

---

**8. `orders.withColumn("rn", row_number().over(Window.partitionBy("category").orderBy(...))).explain()`
shows `Exchange hashpartitioning(category, 8)`. The same query with NO `partitionBy` at all shows
`Exchange SinglePartition` instead. What does that difference mean for how the job scales?**

<details>
<summary>Answer</summary>

`hashpartitioning(category, 8)` means the shuffle spreads rows across up to 8 partitions by
category, so the subsequent sort-and-rank work can run in parallel across categories.
`SinglePartition` means every row in the entire dataset gets funneled into one partition, because
there's no grouping key to hash-partition by — a global ranking has no way to be computed without
first bringing every row together in one place. On small data this is invisible; on real-sized
data it's a guaranteed single-task bottleneck, structurally unavoidable given a window with no
`partitionBy`.
</details>

---

**9. Why is "top N rows per group" typically implemented with `row_number()` + `filter(rn <= N)`
rather than `rank()` or `dense_rank()` + the same filter?**

<details>
<summary>Answer</summary>

`row_number()` guarantees exactly one row per rank position, so `filter(rn <= 2)` always returns
exactly 2 rows per group. `rank()` or `dense_rank()` can return *more* than N rows for a given
group if there's a tie at the boundary (e.g. three rows tied for rank 2 would all pass a
`rank() <= 2` filter) — fine if that's the intended semantics ("all rows tied for a top-2 spot"),
but not what "exactly the top 2" usually means.
</details>

---

**10. Does adding `partitionBy` to a window function make the shuffle disappear entirely?**

<details>
<summary>Answer</summary>

No — every window function requires a shuffle-and-sort of some kind (`Exchange` +
`Sort` in the physical plan, verified for both the partitioned and non-partitioned cases). Adding
`partitionBy` changes the *shape* of that shuffle from "everything into one partition" to
"hash-partitioned into several, sortable in parallel" — it doesn't eliminate the wide-transformation
cost that Module 01 first introduced and Module 06 covered in depth.
</details>

---

Check the boxes in [`PROGRESS.md`](../PROGRESS.md) and move on to Module 08 when it's built.
