# Module 11 — Delta Lake / Lakehouse

Every write in Modules 02-10 was a plain file write — CSV, JSON, or Parquet, with no transaction
log and no history. Delta Lake adds a transaction log on top of ordinary Parquet files, which is
what turns "a folder of files" into a real table: ACID writes, time travel, row-level `MERGE`,
and safe concurrent access from multiple writers. This module verifies every one of those claims
directly — including catching a genuine concurrent-write conflict live, with real threads.

## Learning objectives

By the end of this module you can:
- Explain what a Delta table actually is (Parquet files + a `_delta_log`), and use time travel
  (`versionAsOf`/`timestampAsOf`) to read a table's exact past state, verified against the
  transaction log's own history
- Use `MERGE` to perform a real upsert, and precisely distinguish it from `append` +
  `mergeSchema` — verified to produce a **duplicate row**, not an update, when misused for the job
  `MERGE` is actually meant to do
- State exactly what `OPTIMIZE` does and doesn't do to files on disk, and why `VACUUM` is a
  separate, genuinely destructive step with its own safety check — verified with real file counts
  before/after each
- Reproduce a real concurrent-write conflict with two threads racing to update the same row, and
  explain Delta's optimistic concurrency control from the actual exception it raises
- Use `RESTORE` as a safe, auditable undo, verified to add a new version rather than erase history
- Design a bronze/silver/gold (medallion) pipeline and explain what data-quality job belongs at
  each layer, verified end-to-end against the shared `orders.csv` fixture including a real
  duplicate-record scenario

## Lessons

1. [ACID Tables and Time Travel](01-acid-tables-and-time-travel.md)
2. [MERGE and Upserts](02-merge-and-upserts.md)
3. [OPTIMIZE and VACUUM](03-optimize-and-vacuum.md)
4. [Concurrency, RESTORE, and Constraints](04-concurrency-and-restore.md)
5. [Medallion Architecture](05-medallion-architecture.md)

Then: [`exercises/`](exercises/) before [`solutions/`](solutions/), then [`quiz.md`](quiz.md).

Uses `/data/orders.csv` for Lesson 5, and small synthetic DataFrames elsewhere (a fresh Delta table
per lesson keeps each concept isolated and reproducible). Every code example and output in this
module was run against PySpark 3.5.3 with `delta-spark==3.3.0` (the Delta release line compatible
with Spark 3.5.x — Delta 4.x requires Spark 4.0) on the local Windows venv.

> **New dependency this module:** `delta-spark==3.3.0`, added to `requirements.txt`. The first run
> of any Delta code on a machine needs internet access — `configure_spark_with_delta_pip(...)`
> resolves the Delta JARs from Maven Central via Ivy the first time (cached under `~/.ivy2` after
> that), exactly like any other first-time dependency resolution.

---
**Next:** [Module 12 — Data Engineering Patterns](../12-data-engineering-patterns/)
