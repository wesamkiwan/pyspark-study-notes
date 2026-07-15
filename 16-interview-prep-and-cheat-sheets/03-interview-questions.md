# Interview Questions

Answer each yourself before expanding — same discipline as every module's quiz. Organized
fundamentals → intermediate → advanced → system-design-style. Every model answer draws on a
specific verified fact from this course, and where it's useful, a note on *what a strong answer
sounds like* in an actual interview, not just the correct content.

---

## Fundamentals

**1. "Walk me through what happens when I call `df.filter(...).groupBy(...).count()`."**

<details>
<summary>Model answer</summary>

Nothing happens yet when you write it — `.filter()` and `.groupBy().count()` are transformations,
lazily recorded into a logical plan (Module 01). Only when an action is called (here, `.count()`
itself is the action that triggers execution) does Catalyst optimize the accumulated logical plan
into a physical plan, and only then do executors actually run tasks against real data partitions.
`groupBy` introduces a shuffle — a stage boundary — since rows need to be redistributed by key
before the count can be computed per group.

**Interview note:** naming the shuffle/stage-boundary explicitly, and being precise about *when*
each piece of your one-liner actually executes, is what separates "I've used Spark" from "I
understand Spark's execution model."
</details>

---

**2. "Why is `.collect()` dangerous, and when is it actually fine to use?"**

<details>
<summary>Model answer</summary>

`.collect()` pulls every row into the driver's single-process memory — on real-sized data this
risks an OOM on the one machine that isn't horizontally scaled the way executors are. It's fine
specifically when you know the result is genuinely small (an aggregated summary, a handful of
distinct values, test assertions in Module 13's style) — the risk is proportional to how much data
you're pulling, not the method itself being universally forbidden.
</details>

---

**3. "What's the difference between a transformation and an action, and name one of each that
might surprise someone."**

<details>
<summary>Model answer</summary>

Transformations build the plan lazily; actions trigger execution. The surprising one: `.count()`
against a corrupt-record-derived DataFrame is an action that can return a WRONG number even after
an explicit `.filter()` — verified in Module 02, Spark's optimizer can skip the filter via a fast
path for certain text-source counts. On the transformation side: `.cache()` itself does nothing
until the next action runs — verified in Module 09, a call counter read right after `.cache()`
still shows zero.
</details>

---

**4. "CSV vs Parquet — why would you ever choose one over the other, beyond 'Parquet is faster'?"**

<details>
<summary>Model answer</summary>

Parquet is columnar and typed, so it supports column pruning (skip reading unrequested columns
entirely, verified) and doesn't need `inferSchema`'s extra scan pass. CSV is human-readable and
universally producible/consumable by non-Spark systems — a reasonable choice for a landing zone
receiving files from external, non-Spark sources, or for genuinely small reference data where the
performance difference is irrelevant. The real answer to "why not always Parquet" is usually about
what's producing or consuming the file, not a performance argument.
</details>

---

## Intermediate

**5. "A `groupBy("customer_id").agg(sum("amount"))` on 15 rows produces 200 output partitions.
Why, and is that actually a problem?"**

<details>
<summary>Model answer</summary>

`spark.sql.shuffle.partitions` defaults to 200, a fixed number unrelated to data size — verified
directly in Module 06. Whether it's a *problem* depends on scale: for tiny data, yes, hundreds of
near-empty partitions each pay fixed per-task scheduling overhead for near-zero work. With AQE
enabled (the 3.5.3 default), the shuffle still WRITES 200 partitions' worth of output, but AQE
coalesces the small ones back down on read — reducing the downstream cost without eliminating the
upstream write cost. The fix, if it matters at your data's actual scale, is setting
`shuffle.partitions` deliberately rather than trusting the default.
</details>

---

**6. "Explain data skew — how would you detect it, and how would you fix it?"**

<details>
<summary>Model answer</summary>

Skew is one (or a few) keys holding disproportionately more rows than others, so the partition(s)
holding those keys take far longer than the rest — verified symptom: a stage nearly finished except
one task still running long after its siblings completed. Detection: `.rdd.glom().map(len)` gives
exact per-partition row counts directly (Module 06), more reliable than inferring from wall-clock
time. Fix: salting — split the hot key into N sub-keys before the shuffle, then a two-stage
aggregation (aggregate by `(key, salt)` first, then re-aggregate by `key` alone) to get the correct
total back, verified to match the unsalted result exactly.
</details>

---

**7. "What's the actual difference between `RANGE` and `ROWS` frames in a window function, and
why does it matter?"**

<details>
<summary>Model answer</summary>

`ROWS` counts physical row position (a literal N-rows-back frame); `RANGE` groups by the ordering
column's VALUE — every row sharing the current row's value is a "peer" included together. It
matters because Spark's DEFAULT frame (no explicit `rowsBetween`) is `RANGE BETWEEN UNBOUNDED
PRECEDING AND CURRENT ROW` — verified to produce a silent running-total bug: tied rows all get the
identical cumulative sum instead of incrementing one at a time. Safe running totals need an
explicit `.rowsBetween(Window.unboundedPreceding, Window.currentRow)`.
</details>

---

**8. "When would you force a broadcast join, and when would that backfire?"**

<details>
<summary>Model answer</summary>

Force it when you know one side is genuinely small and Spark's automatic threshold
(`autoBroadcastJoinThreshold`, 10MB default, verified to auto-trigger with zero hint) either isn't
kicking in or you want to guarantee it regardless of Spark's size estimate. It backfires when the
"small" side isn't actually small — broadcasting copies the ENTIRE side to EVERY executor, so a
stale or wrong size estimate (common after several transformations) risks executor OOM instead of
the shuffle you were trying to avoid.
</details>

---

**9. "How do you write a unit test for a PySpark transformation without standing up a real
cluster or real files?"**

<details>
<summary>Model answer</summary>

Structure the transformation as a pure function — DataFrame in, DataFrame out, no hardcoded paths
or `SparkSession` creation inside it (Module 13). Test it with a small, hand-built DataFrame via a
`session`-scoped `SparkSession` pytest fixture shared across the whole test suite (verified to
amortize the ~2.8s JVM startup cost across many tests instead of paying it per test), asserting the
result with `chispa.assert_df_equality` — remembering its defaults are stricter than they look:
row order, column order, and nullable flags all fail by default even on identical data.
</details>

---

**10. "What makes a batch pipeline idempotent, and why does it matter for a scheduled job?"**

<details>
<summary>Model answer</summary>

Idempotent means running it twice with the same input produces the same result as running it once
— verified concretely: a naive `append`-based load run twice DOUBLES row count, while a
`replaceWhere`-scoped overwrite (or a `MERGE`-based upsert) run twice stays correct. It matters for
scheduling specifically because a scheduler's automatic retry (Airflow's `retries`, for example)
is only a safety net if re-running the job is actually safe — retrying a non-idempotent job just
automates the double-counting bug instead of recovering from the transient failure it was meant to
handle.
</details>

---

## Advanced

**11. "Tell me about a subtle bug you'd expect to find in a Spark pipeline that wouldn't show up
in code review."**

<details>
<summary>Model answer</summary>

A strong answer picks a genuinely non-obvious, verified example rather than something a linter
would catch — e.g., the SCD Type 2 bug from Module 12: building a "which keys changed" DataFrame
lazily from a table, then reusing it AFTER a `MERGE` that mutates that same table. It silently
finds zero rows on the second use, because the underlying table already changed. Worse, `.cache()`
on that DataFrame does NOT fix it — Delta invalidates the cache once the table it reads from is
mutated. The only real fix: `.collect()` the changed keys into plain Python data before the
`MERGE` runs. This kind of bug passes code review because the code *looks* reasonable — the
failure only shows up at runtime, and only for keys that actually change.
</details>

---

**12. "How does Structured Streaming actually achieve exactly-once semantics with a file sink?"**

<details>
<summary>Model answer</summary>

Through the checkpoint — durably tracking which source offsets (file names, for a file source)
have already been committed. Verified the strong way: two genuinely separate Python
processes/JVMs sharing a `checkpointLocation`, where the second process (simulating a crash/restart)
never reprocessed files the first process had already committed, and correctly picked up only
genuinely new ones — zero duplicate output. "Exactly-once" here specifically means the sink's
output reflects each source record's effect exactly once, not that the source is read exactly once
internally (Spark may re-read/reprocess internally on retry, but the checkpoint's offset tracking
prevents that from producing duplicate *output*).
</details>

---

**13. "Explain Delta Lake's `OPTIMIZE` and `VACUUM` — why are they two separate operations
instead of one?"**

<details>
<summary>Model answer</summary>

`OPTIMIZE` compacts small files into larger ones by writing NEW files and updating the transaction
log so the current version reads only those — verified NOT to delete the old files (count went
16→17, up not down), because an older version (time travel) might still need them. `VACUUM` is the
separate, genuinely destructive step that physically deletes files nothing needs anymore (verified
17→1), which is exactly why it has its own retention safety check (refuses below a threshold
without an explicit override) — conflating the two into one operation would make routine compaction
accidentally destroy time-travel history.
</details>

---

**14. "Two processes are both trying to MERGE into the same Delta table concurrently. What
actually happens?"**

<details>
<summary>Model answer</summary>

Delta uses optimistic concurrency control — verified live with two real threads racing a `MERGE`
against the same row: both read the same starting version, the first to commit succeeds, and the
second's commit — based on now-stale information about which files exist — is rejected outright
with a `ConcurrentAppendException` rather than silently racing and corrupting the result.
Production code should catch this exception (and its siblings —
`ConcurrentDeleteReadException`, etc.) and retry; this is the normal, expected way concurrent Delta
writes resolve, not an edge case to special-case away.
</details>

---

**15. "Design the layers of a lakehouse pipeline for a dataset you know will have occasional
duplicate/malformed records. Where does each responsibility belong?"**

<details>
<summary>Model answer</summary>

Bronze: ingest raw, unconditionally, including duplicates and malformed rows — this is the only
permanent record of what upstream actually sent (verified in Module 11's medallion capstone: a
deliberate duplicate order genuinely shows up in bronze's row count). Silver: deduplicate (a
`row_number()`-based dedup, Module 07) and apply data quality gates that RAISE before write and
check volume, not just per-row validity (Module 12/13 — an empty batch trivially passes a
per-row-only gate). Gold: business-ready aggregates built ONLY on top of the deduplicated,
gated silver layer — never straight from bronze, since a leaked duplicate at gold silently
inflates a business number with no obvious symptom.
</details>

---

## System-Design-Style

**16. "A daily batch job needs to be safely retryable, auditable, and resistant to a bad load
corrupting the table. What would you actually build?"**

<details>
<summary>Model answer</summary>

Delta Lake as the storage layer (ACID writes, transaction log). Idempotent loads via
`replaceWhere` scoped to the load's partition key, or `MERGE` for upserts (Module 12) — verified
both stay correct across a repeated run, unlike naive `append`. A quality gate that raises before
write and checks both per-row validity and volume (Module 12/13). `RESTORE` as the safe, auditable
undo if something does slip through — verified to add a new version rather than erase history, so
even the undo itself is reversible. Orchestrated with retries + exponential backoff (this course's
Module 14), which is only safe BECAUSE the load is already idempotent.
</details>

---

**17. "How would you design a slowly changing customer dimension that's correctly joinable against
historical fact data?"**

<details>
<summary>Model answer</summary>

SCD Type 2 (Module 12): `effective_start`/`effective_end`/`is_current` columns, a `MERGE`-based
expire-then-insert pattern for detected changes (verified: only compare against the CURRENT row
when detecting changes, and materialize the changed-keys list to Python data BEFORE the `MERGE`
mutates the table — the verified trap otherwise). Critically, join fact rows to the dimension using
a point-in-time range condition (`fact.date >= dim.effective_start AND (dim.effective_end IS NULL
OR fact.date < dim.effective_end)`), NOT `is_current = true` — verified in this course's Capstone 3
that an `is_current`-only join incorrectly shows a customer's CURRENT tier for historical orders
placed before a tier change, silently rewriting history.
</details>

---

**18. "Your streaming job's throughput is degrading over time, and you're not sure why. Walk me
through how you'd debug it."**

<details>
<summary>Model answer</summary>

Start with `query.recentProgress`/`lastProgress` (Module 10) — specifically comparing
`inputRowsPerSecond` against `processedRowsPerSecond`; if input consistently outpaces processed,
the stream is falling behind its source, which eventually shows up as unbounded checkpoint/state
growth rather than an obvious crash. Check whether a `foreachBatch` callback is calling more than
one action against the batch DataFrame without caching it first — verified to double real work per
trigger (a very common, easy-to-introduce bug: logging a count, then writing, both against an
uncached batch_df). For a windowed aggregation, verify the watermark isn't too loose (keeping
excess state in memory) or too tight (silently dropping late data with no error, Module 10 Lesson
3). Cross-reference with the Spark UI's Structured Streaming tab for the same metrics as live
graphs.
</details>

---

**19. "How do you decide between Databricks Jobs, EMR, and a self-managed cluster for a new
pipeline?"**

<details>
<summary>Model answer</summary>

All three run the same Spark — the difference is entirely who manages infrastructure underneath
(Module 14). Databricks Jobs fits teams wanting the fastest notebook-to-production path with
minimal infra ownership. EMR fits teams already deep in the AWS ecosystem wanting tight
S3/Glue/IAM integration. Self-managed standalone/YARN fits specific compliance/on-prem/full-control
requirements or an existing cluster investment. Critically: none of the three test your
transformation logic, make your job idempotent, or reason about cluster sizing FOR you — those
remain your responsibility regardless of platform, just with a friendlier UI in front of the
infrastructure pieces.
</details>

---

**20. "What would you actually check before telling your team a new pipeline is 'production
ready'?"**

<details>
<summary>Model answer</summary>

The Module 14 checklist, in the interviewer's own words back at them if possible: is the
transformation logic tested (pure functions, edge cases including an empty batch)? Is it
idempotent (verified against a real repeated run, not assumed)? Is there a quality gate checking
both validity and volume? Is it fault-tolerant against its own restart (a real checkpoint, verified
across a genuine process restart, not just in-memory reuse)? Is it retryable with a sane retry
policy? Is it monitored in a way that would actually surface silently-falling-behind, not just
"is the process alive"? Is cluster sizing reasoned about rather than copy-pasted from an unrelated
job? A strong answer treats this as a deliberate review with real trade-offs per pipeline, not a
box-ticking exercise applied identically everywhere.
</details>

---

Good luck. Everything on this page was actually run and verified while building this course —
that discipline, more than any specific fact, is what will serve you best in a real Spark role.
