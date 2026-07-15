# Module 09 — Performance Tuning

Modules 05-08 each used `.explain()` and caching in passing, to prove a specific point about
joins, shuffling, or UDFs. This module steps back and treats those tools as the subject: how to
read a physical plan thoroughly (including the surprising difference between a plan you print
*before* running a query versus *after*), how caching and persistence actually work under the
hood (verified with a real recompute-counter, not just asserted), which storage level a bare
`.cache()` actually uses on a DataFrame (it's not what you'd guess), and which Spark configs
genuinely change performance versus which ones are cosmetic.

## Learning objectives

By the end of this module you can:
- Read `.explain()` in all three useful modes (`simple`, `formatted`, `cost`), and know why the
  plan looks different before an action runs versus after, when Adaptive Query Execution is enabled
- Explain, with a verified experiment (not just the docs' word for it), that `.cache()`/`.persist()`
  are lazy and that a *second* action against a cached DataFrame skips recomputation entirely —
  and that `.unpersist()` immediately gives that recomputation back
- State which storage level a plain `.cache()` actually applies to a DataFrame, verified — it is
  **not** `MEMORY_ONLY`, despite that being the RDD-API default most tutorials describe
- Choose a storage level deliberately (`MEMORY_ONLY` vs `MEMORY_AND_DISK` vs `DISK_ONLY` vs
  `OFF_HEAP`) based on what each one's `useMemory`/`useDisk`/`deserialized` flags actually mean
- Read the handful of Spark SQL configs that materially affect performance
  (`spark.sql.files.maxPartitionBytes`, `spark.sql.shuffle.partitions`,
  `spark.sql.autoBroadcastJoinThreshold`, the `spark.sql.adaptive.*` family,
  `spark.default.parallelism`) and verify their effects directly rather than trusting defaults
- Use the Spark UI (`http://localhost:4040`) alongside `.explain()` and caching decisions as one
  coherent performance-debugging workflow

## Lessons

1. [Reading Physical Plans Like a Pro](01-reading-physical-plans.md)
2. [Caching and Persistence, Verified](02-caching-and-persistence.md)
3. [Storage Levels Deep Dive](03-storage-levels-deep-dive.md)
4. [Spark Configs That Matter](04-spark-configs-that-matter.md)
5. [A Performance Debugging Workflow](05-performance-debugging-workflow.md)

Then: [`exercises/`](exercises/) before [`solutions/`](solutions/), then [`quiz.md`](quiz.md).

Uses `/data/orders.csv` and `/data/employees.csv`. Every code example and output in this module was
run against PySpark 3.5.3 with Adaptive Query Execution enabled (the 3.5.3 default) — nothing here
is hypothetical, including the specific byte thresholds and partition counts.

---
**Next:** [Module 10 — Structured Streaming](../10-structured-streaming/)
