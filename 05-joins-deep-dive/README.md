# Module 05 — Joins Deep Dive

Every prior module has quietly avoided joins. This module confronts them directly: the six join
types and what each one actually keeps or drops, a genuinely common `AMBIGUOUS_REFERENCE` error and
its fixes, how Spark actually executes a join physically (broadcast vs shuffle) and how to read
that off `.explain()`, what data skew is and why it turns one join into your slowest job, and two
NULL-related traps specific to join keys.

## Learning objectives

By the end of this module you can:
- Choose the right join type (`inner`/`left`/`right`/`full`/`left_semi`/`left_anti`) instead of
  defaulting to `inner` and filtering afterward
- Avoid and fix the `AMBIGUOUS_REFERENCE` error that comes from joining on a condition instead of a
  column-name string
- Read `.explain()` well enough to tell a broadcast hash join from a sort-merge join, and force one
  with `broadcast()` or `spark.sql.autoBroadcastJoinThreshold`
- Recognize data skew, explain why it's the classic "one task takes forever" symptom, and know
  Adaptive Query Execution's skew-join handling exists as a mitigation
- Reason correctly about `NULL` join keys, and know why an un-conditioned `.join()` silently
  produces a full cartesian product by default

## Lessons

1. [The Six Join Types](01-join-types.md)
2. [The Duplicate-Column Trap](02-duplicate-columns.md)
3. [Broadcast vs Sort-Merge Joins](03-broadcast-vs-sortmerge.md)
4. [Data Skew and Adaptive Query Execution](04-data-skew-and-aqe.md)
5. [NULL Join Keys and the Accidental Cartesian Product](05-null-keys-and-cross-joins.md)

Then: [`exercises/`](exercises/) before [`solutions/`](solutions/), then [`quiz.md`](quiz.md).

Uses `/data/employees.csv` and `/data/orders.csv` — including `orders.csv`'s row with
`emp_id = 999`, an order placed under an employee ID that doesn't exist in `employees.csv`, which
this module uses deliberately to demonstrate outer-join and anti-join behavior. Every code example
and output in this module was run against these exact files with PySpark 3.5.3 — nothing here is
hypothetical.
