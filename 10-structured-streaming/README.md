# Module 10 — Structured Streaming

Every module so far has run a batch job: read a fixed dataset, transform it, write a result, done.
Structured Streaming reuses the *exact same* DataFrame API you already know, but points it at data
that keeps arriving — and Spark keeps re-running (a differential version of) your query as new data
shows up. This module builds a real, working streaming pipeline entirely on the local venv (no
Kafka, no cluster) using the file source, and verifies every non-obvious behavior by actually
starting queries, feeding them data over time, and reading back what happened — including two
findings that would be easy to get wrong from the docs alone: which late-arriving rows a watermark
silently drops, and a real recompute bug `foreachBatch` code can quietly introduce.

## Learning objectives

By the end of this module you can:
- Explain Structured Streaming's core mental model — an unbounded table Spark incrementally
  re-queries — and why a plain `.count()`/`.show()` is rejected on a streaming DataFrame, verified
  by the actual `AnalysisException`
- Build a file-source streaming pipeline (`readStream`/`writeStream`), and choose the right output
  mode (`append`/`update`/`complete`) for a given query
- Pick the right trigger (default micro-batch, fixed-interval `processingTime`, or `availableNow`
  for a batch-like one-shot run) and state precisely how each behaves, verified with real batch
  timestamps rather than assumed from the docs
- Use `withWatermark` and windowed aggregations for event-time processing, and state exactly what
  happens to a late-arriving row on both sides of the watermark boundary — verified: accepted and
  correctly merged into an already-open window, versus silently dropped once that window has closed
- Explain why checkpointing is what makes a stream fault-tolerant, verified by killing a streaming
  query's process entirely and restarting a **brand new process** against the same checkpoint —
  proving already-processed files are never reprocessed and no duplicate output is produced
- Use `foreachBatch` as an escape hatch to arbitrary batch-DataFrame logic, and avoid a real,
  verified recompute bug it can introduce — plus read `StreamingQuery.lastProgress`/`recentProgress`
  as your primary debugging tool

## Lessons

1. [The Structured Streaming Mental Model](01-the-streaming-mental-model.md)
2. [Triggers, Verified](02-triggers-verified.md)
3. [Watermarks and Windowed Aggregations](03-watermarks-and-windows.md)
4. [Checkpointing and Fault Tolerance](04-checkpointing-and-fault-tolerance.md)
5. [foreachBatch and the Streaming Debugging Workflow](05-foreachbatch-and-debugging.md)

Then: [`exercises/`](exercises/) before [`solutions/`](solutions/), then [`quiz.md`](quiz.md).

Uses a small amount of synthetic order data generated in-script (the shared `/data` fixtures are
static files, which don't fit a module about data that arrives over time) written to temporary
directories. Every code example and output in this module was run against PySpark 3.5.3 on the
local Windows venv — nothing here is hypothetical, including the exact row counts, batch
timestamps, and metric values.

> **No Kafka in this module, on purpose.** The file source (`readStream...csv(...)`) is a genuine
> production streaming source (plenty of real pipelines watch an S3/ADLS/HDFS landing directory),
> and it lets every example run locally with zero extra infrastructure. Everything you learn here —
> triggers, watermarks, checkpointing, `foreachBatch` — applies unchanged to a Kafka source; only
> the `readStream.format(...)` call and its options would differ.

---
**Next:** [Module 11 — Delta Lake / Lakehouse](../11-delta-lake/)
