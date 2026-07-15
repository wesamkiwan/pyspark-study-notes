# Module 12 — Data Engineering Patterns

Modules 01-11 taught you Spark's mechanics. This module is about the patterns that separate a
pipeline that works once from one that survives being re-run, fed bad data, or run against a table
that already has today's data in it — the actual day-to-day of production data engineering. Every
pattern here is verified against a real, small pipeline, not just described.

## Learning objectives

By the end of this module you can:
- Explain why "just re-run the job" is dangerous by default, and build a genuinely idempotent load
  using partition-scoped overwrite — verified: a naive `append`-based job run twice silently
  doubles its output, while a `replaceWhere` overwrite-based job run twice does not
- Implement Slow Changing Dimension Type 2 (full history, `is_current`/`effective_start`/
  `effective_end`) using Delta `MERGE`, verified end to end with a real attribute change, an
  untouched record, and a brand new key
- Build data quality gates that fail a pipeline *before* bad data reaches a downstream consumer,
  verified by deliberately feeding rows that should and shouldn't pass
- Apply a dead-letter pattern for malformed records, so one bad row doesn't sink an entire batch —
  verified by processing a batch with a genuinely malformed row and confirming the good rows still
  land correctly
- Combine these patterns into one coherent pipeline shape you'd recognize in a real job

## Lessons

1. [Idempotent Pipelines](01-idempotent-pipelines.md)
2. [Slow Changing Dimensions (SCD Type 2)](02-slow-changing-dimensions.md)
3. [Data Quality Gates](03-data-quality-gates.md)
4. [Error Handling and Dead Letters at Scale](04-error-handling-and-dead-letters.md)
5. [Putting It Together: A Production-Shaped Pipeline](05-putting-it-together.md)

Then: [`exercises/`](exercises/) before [`solutions/`](solutions/), then [`quiz.md`](quiz.md).

Builds on Module 11's Delta Lake (`MERGE`, ACID writes) throughout — these patterns are far more
awkward to implement correctly on plain Parquet. Every code example and output in this module was
run against PySpark 3.5.3 + `delta-spark==3.3.0` on the local Windows venv.

---
**Next:** [Module 13 — Testing PySpark Code](../13-testing-pyspark-code/) *(not yet built)*
