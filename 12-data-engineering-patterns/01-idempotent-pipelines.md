# Lesson 1 — Idempotent Pipelines

A pipeline is **idempotent** if running it twice with the same input produces the same result as
running it once. This sounds obvious until you consider how often a real job actually *does* get
run twice: a scheduler retry after a transient failure, an on-call engineer re-triggering a job
that looked stuck, a backfill that overlaps a day already loaded. This lesson verifies a naive
pipeline shape that silently breaks under exactly this scenario, and a fix that doesn't.

```mermaid
flowchart TB
    subgraph Naive["Naive: mode(\"append\")"]
        direction TB
        A1["Run 1: load 2024-01-01"] --> A2["2 rows"]
        A3["Run 2 (retry): load 2024-01-01 again"] --> A4["4 rows -- DOUBLED"]
    end
    subgraph Idempotent["Idempotent: replaceWhere overwrite"]
        direction TB
        B1["Run 1: load 2024-01-01"] --> B2["2 rows"]
        B3["Run 2 (retry): load 2024-01-01 again"] --> B4["2 rows -- unchanged"]
    end
```

## The naive version, verified broken

```python
def daily_batch():
    return spark.createDataFrame(
        [(1, "alice", 10.0, "2024-01-01"), (2, "bob", 20.0, "2024-01-01")],
        ["order_id", "customer", "amount", "load_date"],
    )

daily_batch().write.format("delta").mode("append").save(table_path)
daily_batch().write.format("delta").mode("append").save(table_path)   # a retry -- same input again
```

Verified: **4 rows**, not 2. A perfectly reasonable-looking `append`-based daily load job silently
double-counts every order the moment it's run twice for the same day — no error, no warning,
just wrong downstream numbers. This is one of the most common real-world data quality incidents:
"the totals don't match" traced back to a job that got retried.

## The idempotent version, verified correct

```python
(
    daily_batch()
    .write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", "load_date = '2024-01-01'")
    .partitionBy("load_date")
    .save(table_path)
)
# run it again, unchanged:
(
    daily_batch()
    .write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", "load_date = '2024-01-01'")
    .partitionBy("load_date")
    .save(table_path)
)
```

Verified: **2 rows**, both times. `replaceWhere` tells Delta "atomically replace exactly the rows
matching this condition, leave everything else in the table untouched" — so re-running the same
day's load simply produces the same partition's data again, not a second copy of it.

## Why this is the right default, not just a workaround

- **The job becomes safe to retry.** A scheduler (Module 14) that automatically retries a failed
  task doesn't need special "has this already run?" logic — retrying an idempotent job is always
  safe by construction.
- **Backfills become safe.** Re-running last month's loads to fix a bug doesn't require first
  manually deleting last month's data — `replaceWhere` does that atomically as part of the write.
- **`replaceWhere` requires the condition to actually match the data being written** — Delta
  validates that every row in the new data satisfies the `replaceWhere` predicate, refusing a write
  that would silently leave rows outside the intended partition. This is a real safety net against
  a classic bug: accidentally overwriting the wrong partition's condition string.

## When `replaceWhere` isn't the right tool

- **Row-level upserts** (a key already exists somewhere in the table, not cleanly isolated to one
  partition condition) are `MERGE`'s job (Module 11, Lesson 2), not `replaceWhere` — `MERGE` is
  also inherently idempotent for a `whenMatchedUpdateAll()`/`whenNotMatchedInsertAll()` shape: the
  same incoming batch merged twice produces the same end state, since matched rows just get
  updated to the same values again rather than duplicated.
- **Streaming sinks** (Module 10) get their idempotency from checkpointing instead — a resumed
  stream doesn't reprocess already-committed micro-batches at all, which is a different mechanism
  solving the same underlying problem.

## Best-practice callout

**Design every batch load around "what happens if this runs twice?" from the start**, not as an
afterthought once a double-counting incident happens in production. The fix (`replaceWhere`,
`MERGE`, or a checkpoint) is nearly always cheaper to build in from day one than to retrofit once
downstream dashboards and dependent jobs have already been quietly wrong for a while.

---
**Next:** [Lesson 2 — Slow Changing Dimensions (SCD Type 2)](02-slow-changing-dimensions.md)
