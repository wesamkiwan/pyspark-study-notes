# Progress Tracker

Check items off as you complete them (edit this file, put an `x` in the brackets, commit).
GitHub renders these as interactive-looking checkboxes on the repo page.

## Module 00 — Setup & Environment
- [ ] Read `00-setup/README.md`
- [ ] Local venv created and PySpark installed
- [ ] `verify_install.py` runs and prints a DataFrame
- [ ] (Optional) Tried the Docker setup
- [ ] (Optional) Tried Databricks Community Edition

## Module 01 — Fundamentals & Architecture
- [ ] Lesson 1: What Spark is and why it exists
- [ ] Lesson 2: Architecture deep dive (driver/executor/cluster manager)
- [ ] Lesson 3: RDD vs DataFrame vs Dataset
- [ ] Lesson 4: SparkSession and your first program
- [ ] Lesson 5: Lazy evaluation and execution plans
- [ ] Exercise 1 completed (attempted before checking solution)
- [ ] Exercise 2 completed (attempted before checking solution)
- [ ] Quiz: scored honestly, reviewed wrong answers

## Module 02 — Reading, Writing & Schemas
- [ ] Lesson 1: Why explicit schemas (the ZIP-code inferSchema bug)
- [ ] Lesson 2: File formats deep dive (CSV/JSON/Parquet/ORC/Avro)
- [ ] Lesson 3: Reading data like a pro (parse modes, the `.count()` gotcha)
- [ ] Lesson 4: Writing data like a pro (write modes, partitioning, file count control)
- [ ] Lesson 5: Schema evolution and mismatches (`mergeSchema`)
- [ ] Exercise 1 completed (attempted before checking solution)
- [ ] Exercise 2 completed (attempted before checking solution)
- [ ] Quiz: scored honestly, reviewed wrong answers

## Module 03 — Core DataFrame Transformations
- [ ] Lesson 1: Selecting and creating columns (select, withColumn, col() vs lit())
- [ ] Lesson 2: Filtering and conditional logic (operator precedence, when/otherwise, nulls)
- [ ] Lesson 3: Grouping and aggregating (groupBy/agg, aliasing, filtering aggregates)
- [ ] Lesson 4: Sorting, deduplication, and combining DataFrames (the union column-order trap)
- [ ] Lesson 5: DataFrame API vs Spark SQL (when to use which, injection safety, temp view scope)
- [ ] Exercise 1 completed (attempted before checking solution)
- [ ] Exercise 2 completed (attempted before checking solution)
- [ ] Quiz: scored honestly, reviewed wrong answers

## Module 04 — Spark SQL
- [ ] Lesson 1: Parameterized SQL (args=) — the injection-safe way to build dynamic queries
- [ ] Lesson 2: CTEs, subqueries (EXISTS), and the UNION vs UNION ALL dedup trap
- [ ] Lesson 3: NULL and three-valued logic (NOT, NOT IN + NULL trap, COALESCE/NULLIF)
- [ ] Lesson 4: expr()/selectExpr(), CASE WHEN, and the silent-NULL CAST gotcha
- [ ] Lesson 5: Catalog API — temp views vs managed vs external tables, DROP TABLE data-loss trap
- [ ] Exercise 1 completed (attempted before checking solution)
- [ ] Exercise 2 completed (attempted before checking solution)
- [ ] Quiz: scored honestly, reviewed wrong answers

## Module 05 — Joins Deep Dive
- [ ] Lesson 1: The six join types (inner/left/right/full/left_semi/left_anti)
- [ ] Lesson 2: The duplicate-column trap (on="col" vs a join condition, AMBIGUOUS_REFERENCE)
- [ ] Lesson 3: Broadcast vs sort-merge joins, reading .explain(), autoBroadcastJoinThreshold
- [ ] Lesson 4: Data skew and Adaptive Query Execution's skew-join handling
- [ ] Lesson 5: NULL join keys never match, and the accidental cartesian product default
- [ ] Exercise 1 completed (attempted before checking solution)
- [ ] Exercise 2 completed (attempted before checking solution)
- [ ] Quiz: scored honestly, reviewed wrong answers

## Module 06 — Partitioning & Shuffling
- [ ] Lesson 1: Partitions 101 (getNumPartitions, maxPartitionBytes, defaultParallelism)
- [ ] Lesson 2: repartition(n, col) hash partitioning, key colocation, over-partitioning cost
- [ ] Lesson 3: spark.sql.shuffle.partitions — the 200 default trap, verified on 15 rows
- [ ] Lesson 4: AQE automatic partition coalescing (AQEShuffleRead coalesced)
- [ ] Lesson 5: Data skew detection and salting, with verified before/after partition sizes
- [ ] Exercise 1 completed (attempted before checking solution)
- [ ] Exercise 2 completed (attempted before checking solution)
- [ ] Quiz: scored honestly, reviewed wrong answers

## Module 07 — Window Functions
- [ ] Lesson 1: Ranking functions (row_number/rank/dense_rank) and the tiebreaker trap
- [ ] Lesson 2: Aggregate window functions — running totals with rowsBetween
- [ ] Lesson 3: lag/lead for period-over-period comparisons and gap analysis
- [ ] Lesson 4: rowsBetween vs the default RANGE frame — verified running-total trap
- [ ] Lesson 5: Top-N per group, and the no-partitionBy single-partition performance trap
- [ ] Exercise 1 completed (attempted before checking solution)
- [ ] Exercise 2 completed (attempted before checking solution)
- [ ] Quiz: scored honestly, reviewed wrong answers

## Module 08 — UDFs & Pandas UDFs
- [ ] Lesson 1: Why UDFs are a last resort (BatchEvalPython vs codegen, verified)
- [ ] Lesson 2: Registering UDFs and the silent return-type-mismatch trap
- [ ] Lesson 3: NULL handling inside UDFs (verified crash on real NULL salary)
- [ ] Lesson 4: Pandas UDFs and Arrow (ArrowEvalPython, verified timing numbers)
- [ ] Lesson 5: applyInPandas and the built-in > Pandas UDF > UDF decision tree
- [ ] Exercise 1 completed (attempted before checking solution)
- [ ] Exercise 2 completed (attempted before checking solution)
- [ ] Quiz: scored honestly, reviewed wrong answers

## Module 09 — Performance Tuning
- [ ] Lesson 1: Reading physical plans like a pro (explain modes, AQE final vs initial plan)
- [ ] Lesson 2: Caching and persistence, verified (accumulator recompute-counter)
- [ ] Lesson 3: Storage levels deep dive (verified DataFrame .cache() default level)
- [ ] Lesson 4: Spark configs that matter (maxPartitionBytes, default.parallelism trap)
- [ ] Lesson 5: A performance debugging workflow (Spark UI tour)
- [ ] Exercise 1 completed (attempted before checking solution)
- [ ] Exercise 2 completed (attempted before checking solution)
- [ ] Quiz: scored honestly, reviewed wrong answers

## Module 10 — Structured Streaming
- [ ] Lesson 1: The Structured Streaming mental model (unbounded table, streaming CSV header gotcha)
- [ ] Lesson 2: Triggers, verified (default, processingTime, availableNow)
- [ ] Lesson 3: Watermarks and windowed aggregations (verified late-data accept vs silent drop)
- [ ] Lesson 4: Checkpointing and fault tolerance (verified across two separate processes/JVMs)
- [ ] Lesson 5: foreachBatch and the debugging workflow (verified uncached-action recompute bug)
- [ ] Exercise 1 completed (attempted before checking solution)
- [ ] Exercise 2 completed (attempted before checking solution)
- [ ] Quiz: scored honestly, reviewed wrong answers

## Module 11 — Delta Lake / Lakehouse
- [ ] Lesson 1: ACID tables and time travel (verified versionAsOf, transaction log history)
- [ ] Lesson 2: MERGE and upserts (verified upsert + append+mergeSchema duplicate-row trap)
- [ ] Lesson 3: OPTIMIZE and VACUUM (verified file counts before/after, retention safety check)
- [ ] Lesson 4: Concurrency, RESTORE, and constraints (verified real concurrent-write conflict)
- [ ] Lesson 5: Medallion architecture (verified bronze/silver/gold pipeline on orders.csv)
- [ ] Exercise 1 completed (attempted before checking solution)
- [ ] Exercise 2 completed (attempted before checking solution)
- [ ] Quiz: scored honestly, reviewed wrong answers

## Module 12 — Data Engineering Patterns
- [ ] Lesson 1: Idempotent pipelines (verified append-doubles vs replaceWhere stays-same)
- [ ] Lesson 2: Slow Changing Dimensions / SCD Type 2 (verified full history via MERGE)
- [ ] Lesson 3: Data quality gates (verified raise-before-write, bad batch never lands)
- [ ] Lesson 4: Error handling and dead letters at scale (verified two corrupt-record failure shapes)
- [ ] Lesson 5: Putting it together (verified combined pipeline stays idempotent on retry)
- [ ] Exercise 1 completed (attempted before checking solution)
- [ ] Exercise 2 completed (attempted before checking solution)
- [ ] Quiz: scored honestly, reviewed wrong answers

## Module 13 — Testing PySpark Code
- [ ] _(module not built yet)_

## Module 14 — Production & Deployment
- [ ] _(module not built yet)_

## Module 15 — Capstone Projects
- [ ] _(module not built yet)_

## Module 16 — Interview Prep & Cheat Sheets
- [ ] _(module not built yet)_

---
*Last updated: 2026-07-15 — Modules 00–12 built.*
