# Module 06 — Partitioning & Shuffling

Every prior module has quietly set `.config("spark.sql.shuffle.partitions", "8")` on every
`SparkSession` without explaining why. This module explains why — and goes deep on the mechanics
Module 01 introduced only conceptually: what a partition actually is, how many you get and why,
what `repartition`/`coalesce` really do to the physical layout (Module 02 covered the basics; this
module covers the trade-offs), the infamous 200-partition shuffle default and how Adaptive Query
Execution now papers over a lot of it automatically, and — closing the loop from Module 05's
deferred promise — a fully verified, numbers-backed walkthrough of diagnosing data skew and fixing
it with salting.

## Learning objectives

By the end of this module you can:
- Explain where a DataFrame's partition count comes from (file splits, `maxPartitionBytes`,
  `defaultParallelism`) and read it directly with `.rdd.getNumPartitions()`
- Tell round-robin `repartition(n)` apart from hash-partitioning `repartition(n, col)`, and know
  why key colocation matters and why over-partitioning has a real cost, not just a shuffle cost
- Explain why `spark.sql.shuffle.partitions` defaults to 200, why that's wrong for most workloads,
  and see it directly in a `.explain()` plan
- Read an AQE-adaptive physical plan well enough to see it shrink 200 shuffle partitions down to a
  sane number automatically (`AQEShuffleRead coalesced`)
- Detect skewed partition sizes directly (not just infer them from a slow task) and fix a skewed
  aggregation with the salting technique, with before/after partition sizes and a correctness check

## Lessons

1. [Partitions 101: What They Are and Where They Come From](01-partitions-101.md)
2. [repartition(n, col): Hash Partitioning and Key Colocation](02-repartition-hash-partitioning.md)
3. [spark.sql.shuffle.partitions: The 200 Default Trap](03-shuffle-partitions-default.md)
4. [Adaptive Query Execution: Automatic Partition Coalescing](04-aqe-partition-coalescing.md)
5. [Data Skew in Practice: Detecting It and Fixing It With Salting](05-skew-and-salting.md)

Then: [`exercises/`](exercises/) before [`solutions/`](solutions/), then [`quiz.md`](quiz.md).

Uses `/data/employees.csv` for the smaller, file-partitioning-focused lessons, and a synthetic
skewed dataset generated in-script (via `spark.range(...)`) for Lesson 5, since genuine skew needs
tens of thousands of rows with a lopsided key distribution to actually show up — this course's
15-row CSVs can't reproduce it. Every code example and output in this module was run against
PySpark 3.5.3 — nothing here is hypothetical.
