# Module 07 — Window Functions

Window functions solve a category of problem that `groupBy` fundamentally can't: computing a value
*relative to a row's neighbors* — its rank, a running total up to that point, the previous row's
value — while still keeping every original row in the output. `orders.csv` has genuine tied
amounts within a category, which turns out to be exactly the right data to expose the two most
common window-function bugs: non-deterministic ranking, and a running-total default that silently
does the wrong thing whenever ties are present.

## Learning objectives

By the end of this module you can:
- Choose correctly between `row_number()`, `rank()`, and `dense_rank()` based on how you want ties
  handled, and know why `row_number()` needs an explicit tiebreaker to be deterministic
- Compute running totals and other cumulative aggregates with `rowsBetween`
- Use `lag`/`lead` for period-over-period comparisons and gap analysis
- Explain the difference between `rowsBetween` and the default `rangeBetween` frame, and why that
  default silently gives the wrong running total the moment your ORDER BY column has duplicates
- Implement the top-N-per-group pattern, and recognize the single-partition performance trap of a
  window with no `partitionBy` at all

## Lessons

1. [Ranking Functions: row_number, rank, and dense_rank](01-ranking-functions.md)
2. [Aggregate Window Functions: Running Totals](02-running-totals.md)
3. [lag/lead: Period-over-Period and Gap Analysis](03-lag-lead-gap-analysis.md)
4. [rowsBetween vs rangeBetween: The Default Frame Trap](04-rows-vs-range-frame-trap.md)
5. [Top-N Per Group, and the No-partitionBy Performance Trap](05-top-n-and-no-partition-trap.md)

Then: [`exercises/`](exercises/) before [`solutions/`](solutions/), then [`quiz.md`](quiz.md).

Uses `/data/orders.csv`, which has genuine duplicate `amount` values within a `category` (five
`Widget A` orders all at `250.0`, three `Widget B` orders all at `410.5`) — real ties, not
synthetic ones, used throughout this module to demonstrate exactly how ranking and running-total
behavior diverges once duplicates are involved. Every code example and output in this module was
run against PySpark 3.5.3 — nothing here is hypothetical.
