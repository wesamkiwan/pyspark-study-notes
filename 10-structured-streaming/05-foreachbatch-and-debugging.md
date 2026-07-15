# Lesson 5 — foreachBatch and the Streaming Debugging Workflow

`foreachBatch` is the escape hatch: it hands you each micro-batch as a plain, ordinary **batch**
DataFrame, so anything you already know how to do with `.write`, multiple sinks, upserts, or extra
logging is fair game — no special streaming-only API needed. This lesson verifies that escape hatch
works exactly as advertised, then verifies a real, easy-to-write recompute bug it can introduce
if you're not careful — a direct streaming cousin of Module 09's caching lesson.

## foreachBatch, verified as a genuine batch DataFrame

```python
def process_batch(batch_df, batch_id):
    n = batch_df.count()                              # an ordinary batch action
    big_orders = batch_df.filter("amount > 15").count()  # a SECOND ordinary batch action
    print(f"batch_id={batch_id} rows={n} big_orders={big_orders}")

query = stream_df.writeStream.foreachBatch(process_batch).outputMode("append").start()
```

Verified: `process_batch` is called once per micro-batch, in order, with the exact `batch_id`
that also appears in `query.recentProgress` — `.count()`, `.filter()`, even writing to two
different sinks from inside the same call all work exactly like they would on any batch DataFrame.

## The bug: calling more than one action without caching, verified to double real work

Here's the same code above, isolated and measured against `query.recentProgress`:

```python
def one_action(batch_df, batch_id):
    n = batch_df.count()

def two_actions_uncached(batch_df, batch_id):
    n = batch_df.count()
    m = batch_df.filter("amount > 15").count()

def two_actions_cached(batch_df, batch_id):
    batch_df = batch_df.cache()
    n = batch_df.count()
    m = batch_df.filter("amount > 15").count()
    batch_df.unpersist()
```

Same 2-row input batch fed to each, verified `recentProgress[0]["numInputRows"]`:

| Function | `numInputRows` reported | Actual rows |
|---|---|---|
| `one_action` | **2** | 2 |
| `two_actions_uncached` | **4** | 2 |
| `two_actions_cached` | **2** | 2 |

Verified, precisely: calling a second uncached action against `batch_df` doesn't just cost extra
CPU time in the abstract — it makes Spark **re-execute the source scan from scratch**, once per
action, and that re-scan gets counted again into the trigger's `numInputRows` metric. Two actions,
uncached, doubled the metric exactly. Caching `batch_df` at the top of `process_batch` (then
`.unpersist()` once done with it, Module 09) brought the number back down to the true row count —
the fix is the exact same rule from Module 09's caching lesson, just now inside a streaming
callback where it's easy to forget because "it's just a small batch DataFrame."

**This is a real production trap, not an academic one:** the natural pattern — log a row count for
observability, *then* write the actual sink — is precisely the two-uncached-actions shape above.
Every micro-batch, forever, silently does double the real work (and reports a misleadingly inflated
`numInputRows`, undermining the very metric you'd use to debug it) until someone notices and adds
`.cache()`.

```python
def process_batch(batch_df, batch_id):
    batch_df = batch_df.cache()
    print(f"batch {batch_id}: {batch_df.count()} rows")   # action 1
    batch_df.write.mode("append").parquet("/output")       # action 2 -- reuses the cached data
    batch_df.unpersist()
```

## The debugging workflow: `lastProgress` / `recentProgress`

Every field you need to know a stream is healthy is already on the query object — no extra
instrumentation required:

```python
query.lastProgress          # dict for the most recent micro-batch
query.recentProgress         # the last several, as a list of the same dict shape
```

Verified fields worth watching, from an actual `lastProgress`:

```python
{
  "batchId": 1,
  "numInputRows": 2,
  "inputRowsPerSecond": 117.6,       # arrival rate -- is the source keeping up with itself?
  "processedRowsPerSecond": 2.2,     # your query's throughput -- is IT keeping up?
  "durationMs": {
    "addBatch": 171,          # time actually running your query + writing the sink
    "latestOffset": 232,      # time spent asking the source "what's new?"
    "walCommit": 235,         # time spent durably committing the checkpoint (Lesson 4)
    ...
  }
}
```

If `inputRowsPerSecond` consistently outpaces `processedRowsPerSecond`, the query is falling
behind its source — a growing backlog that will eventually show up as unbounded checkpoint/state
growth or OOM, long before it shows up as an obvious crash.

**Best-practice callout:** for anything you'd actually run in production, attach a
`StreamingQueryListener` (or poll `recentProgress` on a schedule) and alert on sustained
`inputRowsPerSecond > processedRowsPerSecond`, not just "is the process still running" — a stream
can be alive and still be silently losing the race against its own input.

The **Spark UI's "Structured Streaming" tab** (`http://localhost:4040`, same UI used since Module
09) shows this same progress data as live graphs per running query — worth a look any time you're
debugging a real stream rather than reading `lastProgress` dicts by hand.

---
Check the boxes in [`PROGRESS.md`](../PROGRESS.md), then: [`exercises/`](exercises/) before
[`solutions/`](solutions/), then [`quiz.md`](quiz.md).
