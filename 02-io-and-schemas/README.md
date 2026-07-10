# Module 02 — Reading, Writing & Schemas

Every real pipeline starts with "read some data" and ends with "write some data." This module is
about doing both **correctly** — with schemas you control instead of ones Spark guesses, formats
chosen deliberately instead of by habit, and an understanding of what actually happens to
malformed or evolving data instead of hoping for the best.

## Learning objectives

By the end of this module you can:
- Explain why production pipelines define explicit schemas instead of using `inferSchema=True`
- Choose the right file format (CSV/JSON/Parquet/ORC/Avro) for a given situation, with reasons
- Handle malformed records deliberately (PERMISSIVE/DROPMALFORMED/FAILFAST) instead of by accident
- Control output layout: partitioning, file counts, and write modes
- Recognize schema drift between files and handle it safely with `mergeSchema`

## Lessons

1. [Why Explicit Schemas](01-why-explicit-schemas.md)
2. [File Formats Deep Dive](02-file-formats-deep-dive.md)
3. [Reading Data Like a Pro](03-reading-data-like-a-pro.md)
4. [Writing Data Like a Pro](04-writing-data-like-a-pro.md)
5. [Schema Evolution and Mismatches](05-schema-evolution-and-mismatches.md)

Then: [`exercises/`](exercises/) before [`solutions/`](solutions/), then [`quiz.md`](quiz.md).

Uses `/data/employees.csv`, `/data/employees.json`, `/data/employees_pretty.json`, and
`/data/messy_orders.csv` (deliberately malformed, for the parse-mode lesson).
