# Module 10 Quiz

Answer each yourself before expanding the answer.

---

**1. Calling `.count()` directly on a streaming DataFrame (before `.writeStream.start()`) raises an
`AnalysisException`: "Queries with streaming sources must be executed with writeStream.start();".
Why does Spark enforce this instead of just... running it?**

<details>
<summary>Answer</summary>

A streaming DataFrame represents an unbounded, incrementally-arriving table, not a materialized
result — there's no fixed answer for `.count()` to return, because more data could always still
arrive. Spark rejects every ordinary batch action (`.collect()`, `.count()`, `.show()`,
`.toPandas()`, verified) outright rather than letting you call something that would hang or return
a meaningless partial answer. The only way to actually run a streaming query is
`.writeStream...start()`, which commits you to an explicit output mode and sink instead.
</details>

---

**2. A streaming CSV read (`spark.readStream.schema(...).csv(path)`) silently parses the header row
of every incoming file as a literal data row unless you add one specific option. Which one, and why
is this easy to forget?**

<details>
<summary>Answer</summary>

`.option("header", "true")` — verified: without it, every file's header line becomes a data row
with garbage/NULL values in every typed column. It's easy to forget specifically because streaming
reads introduce several new, unfamiliar options (`maxFilesPerTrigger`, schema requirements) that
draw your attention away from this one ordinary CSV-reading rule (Module 02) that still applies
unchanged.
</details>

---

**3. With no explicit `.trigger(...)` call (the default), three files dropped into a source
directory roughly 2 seconds apart produced three real micro-batches whose timestamps tracked the
file-arrival gaps almost exactly. What does the default trigger actually do?**

<details>
<summary>Answer</summary>

It starts the next micro-batch immediately after the previous one finishes, whenever there's new
data — back-to-back, with no fixed waiting interval between triggers. Verified: batch timestamps
lined up with file-arrival times (2.3s gap, then 0.9s gap), not a fixed cadence.
</details>

---

**4. With `Trigger.ProcessingTime("6 seconds")` against an initially-empty source, the very first
micro-batch fired essentially immediately (`numInputRows=0`), and a file dropped in 2 seconds after
that first batch wasn't picked up until roughly 5.7 seconds later. What two facts does this verify
about `processingTime`?**

<details>
<summary>Answer</summary>

First, the very first trigger always fires immediately on start, regardless of the configured
interval — the interval only governs the gap *between* triggers, not a delay before the first one.
Second, data that arrives mid-interval waits for the *next scheduled tick* rather than triggering
an immediate batch — verified, the file sat untouched for several seconds until the next 6-second
boundary rather than being picked up the moment it appeared.
</details>

---

**5. After starting a query with `.trigger(availableNow=True)` against a directory that already had
3 files sitting in it, `query.awaitTermination()` returned on its own and `query.isActive` was
`False` afterward — without ever calling `query.stop()`. How is this different from every other
trigger, and what production use case is it meant for?**

<details>
<summary>Answer</summary>

Every other trigger (default or `processingTime`) keeps the query running indefinitely, waiting for
more data forever. `availableNow=True` processes everything currently available (possibly across
several micro-batches for a large backlog) and then the query **stops itself** — verified,
`isActive` became `False` with no explicit `.stop()` call. This is the trigger for a scheduled,
batch-like job (e.g. run hourly via Airflow/cron): you get Structured Streaming's checkpoint-based
exactly-once bookkeeping without needing a permanently-running process.
</details>

---

**6. In a windowed aggregation with `withWatermark("event_time", "5 seconds")` and 10-second tumbling
windows, a late row with `event_time=3s` arrived while the watermark was at `7s` and was
successfully merged into the still-open `[0,10)` window (verified: `cnt` went from 1 to 2). A second
late row with `event_time=4s` arrived later, after the watermark had advanced to `20s`, and was
silently dropped instead. What determines which side of that line a late row falls on?**

<details>
<summary>Answer</summary>

Whether the window it belongs to (by its event_time, not arrival time) has already closed as of the
current watermark. A window is still open exactly as long as the watermark is less than the
window's end boundary — the first late row's window `[0,10)` end=10 was still greater than the
watermark (7), so it was accepted; by the time the second late row arrived, the watermark (20) had
already passed that same window's end (10), so the window was considered permanently finalized and
the row was dropped.
</details>

---

**7. The late-and-dropped row from question 6 still showed up as `numInputRows=1` in that trigger's
`query.lastProgress` — Spark didn't reject it at read time. Where exactly did it get discarded, and
what does that imply about trusting `numInputRows` alone as a correctness signal?**

<details>
<summary>Answer</summary>

It was read successfully as input, then discarded during the windowing/watermark stage of that
trigger's execution — verified, with zero error and zero warning anywhere. This means
`numInputRows` only tells you how much data Spark *read*, not how much of it actually made it into
your output — a watermark that's too tight can silently and permanently undercount your
aggregates while every input-side metric still looks perfectly healthy.
</details>

---

**8. A streaming query was started, processed two files, and stopped cleanly (checkpoint intact). A
**brand new Python process and a brand new JVM** were then started against the exact same
`checkpointLocation`, `input` directory (still containing the original two files, untouched), and
`output` path. A third, genuinely new file was also dropped in. The final output had exactly 3 rows,
not 5. What does this prove about checkpointing, and why does the test need to be a separate process
rather than reusing the same query object?**

<details>
<summary>Answer</summary>

It proves the checkpoint's committed offsets are durable on disk and survive a full process/JVM
death, not just an in-memory object — the restarted query, with zero memory of the first process's
run, correctly skipped reprocessing the two already-committed files and only processed the genuinely
new one. Reusing the same Python query object across a "restart" would only prove the object
remembers its own history, which isn't the actual guarantee that matters — what matters in
production is surviving a real crash, deploy, or process restart, which is exactly what a separate
process/JVM test verifies.
</details>

---

**9. Feeding the same 2-row micro-batch into three different `foreachBatch` callbacks —
calling one action, calling two actions without caching, and calling two actions with caching —
produced `numInputRows` values of 2, 4, and 2 respectively. What's actually happening, and how does
`.cache()` fix it?**

<details>
<summary>Answer</summary>

`foreachBatch` hands you a plain, non-materialized batch DataFrame — every action called against it
(`.count()`, `.filter().count()`, etc.) re-executes the underlying source scan from scratch, since
nothing is cached. Two uncached actions genuinely doubled the real read/compute work, and that
doubling is exactly what inflated the reported `numInputRows` metric to 4 for 2 actual rows.
Calling `.cache()` on `batch_df` at the top of the callback (and `.unpersist()` once done, same rule
as Module 09) made the second action reuse the already-computed result instead of re-scanning,
bringing the metric back down to the true row count of 2.
</details>

---

**10. Question 9's bug is described as "a real production trap, not an academic one" because of a
specific common coding pattern. What is that pattern, and why does it hide the bug from whoever
wrote it?**

<details>
<summary>Answer</summary>

Logging a row count for observability, then writing the actual data to the sink — two actions
against the same uncached `batch_df` in the same `foreachBatch` callback. It hides the bug because
the code looks completely reasonable (a natural logging habit) and produces correct *results* every
single trigger — the only symptom is silently doubled real work and an inflated `numInputRows`
metric, which is easy to never notice unless you're specifically comparing reported row counts
against known input volumes.
</details>

---

Check the boxes in [`PROGRESS.md`](../PROGRESS.md) and move on to Module 11 when it's built.
