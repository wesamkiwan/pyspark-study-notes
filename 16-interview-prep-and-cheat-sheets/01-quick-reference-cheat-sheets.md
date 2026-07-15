# Quick Reference Cheat Sheets

Scannable tables, one section per topic area. Every entry traces back to a specific lesson —
follow the module reference if you need the full explanation or verified example.

## Architecture (Module 01)

| Concept | Key fact |
|---|---|
| Driver | Runs your program, builds/optimizes the plan, schedules work — doesn't process data itself |
| Executor | JVM process (or thread, in `local[*]`) that runs tasks against partitions, holds cached data |
| Transformation | Lazy — recorded in the logical plan, nothing executes (`.filter()`, `.select()`, `.join()`) |
| Action | Triggers execution (`.collect()`, `.count()`, `.show()`, `.write.*()`) |
| Stage boundary | Created by a **shuffle** — a wide transformation (`groupBy`, `join`, `orderBy`, `repartition`) |
| Why DataFrame > RDD | Catalyst can inspect/optimize a declarative, schema'd plan; RDD ops are opaque closures |
| `.collect()` on huge data | Pulls everything into the driver's single-machine memory — real OOM risk |
| `PushedFilters` in `.explain()` | Evidence Catalyst moved a filter into the scan itself (predicate pushdown) |

## Reading, Writing & Schemas (Module 02)

| Concept | Key fact |
|---|---|
| `inferSchema=True` | Extra full pass over the data before the real read — costs a double scan |
| Verified danger | A ZIP code `"00501"` silently becomes int `501` under inference — leading zeros gone, no warning |
| CSV vs Parquet column pruning | CSV must read/parse whole rows; Parquet (columnar) can skip unread columns entirely |
| Multi-line JSON | Needs `.option("multiLine", "true")` or every line fails to parse (`_corrupt_record`) |
| `PERMISSIVE` mode | A type mismatch becomes `null`, no error — indistinguishable from a legitimately missing value |
| `.count()` after a corrupt-record read | **Unreliable**, verified — even with a `.filter()` first. Force `.cache()`/`.localCheckpoint()` before counting |
| Multi-file schema drift, no `mergeSchema` | Silently picks ONE file's schema for the whole read — verified, drops a column from every row, no error |
| `mergeSchema=true` cost | Extra metadata scan of every file's schema — not default because most reads don't need it |
| `coalesce(1)` vs `repartition(1)` | `coalesce` avoids a full shuffle where possible (cheaper) but can't fix already-uneven partitions |

## Core Transformations & SQL (Modules 03-04)

| Concept | Key fact |
|---|---|
| `withColumn(name, value)` | `value` must be a `Column` — wrap constants with `lit(...)` |
| `&`/`\|`/`~` in `.filter()` | Python operator precedence trap — every condition needs its own parens, always |
| Comparison against `NULL` | Evaluates to `NULL`, not `False` — silently excluded by `filter`, falls to `.otherwise()` in `when()` chains |
| `.union()` | Matches columns by **position**, not name — verified silent data mislabeling if column order differs. Use `unionByName()` |
| SQL `UNION` vs `UNION ALL` | `UNION` dedups by default (verified); `UNION ALL` matches DataFrame `.union()`'s keep-everything behavior |
| `NOT IN (subquery with NULL)` | Collapses to zero rows for everyone, verified — `NULL` poisons the whole `AND` chain. Use `NOT EXISTS` instead |
| `CAST('bad' AS DOUBLE)` | Returns `NULL`, no error — same "silent null" pattern as bad-type CSV/JSON fields |
| Temp view scope | Scoped to the creating `SparkSession` — a new session can't see it. `createOrReplaceGlobalTempView` for cross-session |
| `DROP TABLE` | Deletes files too for `MANAGED` tables (verified); leaves files alone for `EXTERNAL` tables (verified) |
| DataFrame API vs SQL text | Same Catalyst plan either way — verified, purely a readability choice, never a performance one |
| SQL injection | `spark.sql(f"...{user_input}...")` is injectable exactly like any other string-built query — use `args={}` named params |

## Joins (Module 05)

| Concept | Key fact |
|---|---|
| `LEFT` join row count | Keeps every left row at least once — unmatched rows get `NULL` on the right, doesn't collapse matches |
| `LEFT SEMI` vs `INNER` | `LEFT SEMI` = one row per matching LEFT row (membership check); `INNER` = one row per match (row-multiplying) |
| Ambiguous column after join | Join on `on="col"` (string) coalesces automatically; a join **condition** keeps both sides' columns — alias both sides |
| `autoBroadcastJoinThreshold` | Default 10MB — Spark auto-broadcasts a side under this with NO hint needed, verified |
| Broadcasting risk | Broadcasts the WHOLE side to EVERY executor — bad if the size estimate is stale/wrong → executor OOM |
| Skew symptom | One (or a few) tasks in an otherwise-finished stage stay running far longer than the rest |
| `NULL == NULL` in a join key | Evaluates to `NULL` → treated as non-matching — verified, two null keys never join to each other |
| Missing join condition | Silently runs as a full cartesian product (`spark.sql.crossJoin.enabled` defaults `true`) — verified, no error |

## Partitioning & Shuffling (Module 06)

| Concept | Key fact |
|---|---|
| Input partition count | Governed by `spark.sql.files.maxPartitionBytes` (128 MiB default) — file size ÷ that threshold |
| `defaultParallelism` | Max SIMULTANEOUS tasks (core count in `local[*]`), not a cap on total partition count |
| `repartition(n)` vs `repartition(n, col)` | Plain = round-robin, no data relationship; keyed = hash-partitioned, same key → same partition, verified |
| Over-partitioning cost | Every partition = a task with fixed scheduling overhead — many tiny partitions can be net SLOWER |
| `spark.sql.shuffle.partitions` default | 200, fixed — verified to produce 200 near-empty partitions on tiny data |
| AQE coalescing | Shuffle still WRITES the configured partition count (verified), then MERGES small ones on read — reduces downstream cost, not upstream |
| AQE signal | `AQEShuffleRead coalesced` in `== Final Plan ==`, alongside `isFinalPlan=true` |
| Skew detection | `.rdd.glom().map(len).collect()` gives exact per-partition row counts — more reliable than wall-clock timing |
| Salting | Split a hot key into N salted sub-keys BEFORE the shuffle — needs a two-stage aggregation (by salt, then by real key) to get correct totals back |
| Salting + output partitions | Salt count alone isn't enough — output partition count must exceed salt-bucket count to actually reduce collisions |

## Window Functions (Module 07)

| Concept | Key fact |
|---|---|
| `rank()` vs `dense_rank()` | `rank()` skips ahead by the tie count after ties (verified: 3-way tie at rank 1 → next row is rank 4); `dense_rank()` always +1 |
| `row_number()` on tied `orderBy` | Non-deterministic tie-break without a unique tiebreaker column added to `orderBy` |
| `lag`/`lead` at partition edges | `NULL` at the first/last row respectively — expected, not a bug; third arg sets a custom default |
| Default frame (no explicit `rowsBetween`) | `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` — groups by VALUE not row position. Verified: tied rows all get the SAME cumulative total, a silent running-total bug |
| Safe running total | Always specify `.rowsBetween(Window.unboundedPreceding, Window.currentRow)` explicitly |
| No `partitionBy` | `Exchange SinglePartition` — every row funnels into ONE partition, a structural bottleneck |
| Top-N per group | Use `row_number()` + `filter(rn <= N)`, not `rank()`/`dense_rank()` (which can return MORE than N on a boundary tie) |

## UDFs & Performance (Modules 08-09)

| Concept | Key fact |
|---|---|
| `@udf(returnType=...)` | Never checked against the function's actual return value — wrong type silently becomes `NULL` in every row, verified |
| `.explain()` UDF signature | Plain `@udf` → `BatchEvalPython` (no codegen `*`); `@pandas_udf` → `ArrowEvalPython` |
| Pandas UDF speedup | Verified modest (~5-15%) for numeric work locally, negligible for branching/string logic — not the "10-100x" folklore figure |
| `NaN` from a pandas UDF | Lands as a genuine Spark `NULL` in a `DoubleType` column, not literal float NaN — check `is None`, not a NaN comparison |
| `.explain()` before vs after an action | With AQE, shows `isFinalPlan=false` before, `=true` (+ `AQEShuffleRead coalesced`) after — verified, genuinely different output |
| `.cache()` default storage level | `MEMORY_AND_DISK_DESER`, NOT `MEMORY_ONLY` (that's the RDD API's default) — verified |
| `spark.conf.get("spark.default.parallelism")` | Throws `SparkNoSuchElementException` — it's a core/RDD property, not a SQL config. Use `spark.sparkContext.defaultParallelism` |
| `spark.sql.files.maxPartitionBytes` | Controls INPUT read partitions — distinct from `shuffle.partitions` (shuffle OUTPUT partitions) |

## Structured Streaming (Module 10)

| Concept | Key fact |
|---|---|
| Streaming DataFrame actions | `.count()`/`.show()`/`.collect()` all raise `AnalysisException` — must `.writeStream...start()` |
| Streaming CSV read | Still needs `.option("header", "true")` — easy to forget alongside new streaming-only options |
| Default trigger | Back-to-back micro-batches, as fast as new data arrives — no fixed cadence |
| `Trigger.ProcessingTime` | First batch fires immediately regardless of interval; only SUBSEQUENT batches wait the full interval |
| `Trigger.AvailableNow` | Processes the current backlog then **stops itself** — `isActive` becomes `False`, no `.stop()` needed |
| Watermark + late data | Within tolerance → merges into the still-open window (verified); past tolerance → silently dropped, but still counted in `numInputRows` |
| Checkpoint restart | Verified across two SEPARATE processes/JVMs — no reprocessing/duplication of already-committed files |
| `foreachBatch` + multiple uncached actions | Doubles real work AND `numInputRows`, verified exactly 2x for 2 actions — `.cache()` the batch DataFrame first |

## Delta Lake (Module 11)

| Concept | Key fact |
|---|---|
| What a Delta table IS | Parquet files + `_delta_log/` — the log is what makes ACID/versioning/`MERGE` possible |
| Time travel | `option("versionAsOf", N)` reads exactly that version's file manifest, verified |
| `append` + `mergeSchema=true` on an existing key | Creates a DUPLICATE row, NOT an update — verified. `MERGE` is the real upsert tool |
| `OPTIMIZE` | Compacts into new files but does NOT delete old ones — verified file count went UP (16→17) |
| `VACUUM` | The separate, destructive step that deletes old files (verified 17→1) — refuses below the retention safety check unless explicitly disabled |
| `VACUUM` + time travel | Breaks `versionAsOf` for any version whose files got deleted — verified `Py4JJavaError` |
| Concurrent `MERGE` | Verified live: two threads racing the same row → one succeeds, one raises `ConcurrentAppendException` (optimistic concurrency control) |
| `RESTORE` | Adds a NEW version, doesn't erase history — verified, old version still queryable after |

## Testing, Production & Data Engineering Patterns (Modules 12-14)

| Concept | Key fact |
|---|---|
| Idempotent loads | Naive `append` run twice DOUBLES row count (verified 2→4); `replaceWhere` overwrite-by-partition stays correct (2→2) |
| SCD Type 2 trap | Changed-keys DataFrame reused AFTER a `MERGE` silently finds nothing — verified even `.cache()` doesn't fix it (Delta invalidates on mutation). Only `.collect()` to Python data before the `MERGE` works |
| Quality gate design | Must RAISE before write, and check VOLUME (row count) not just per-row validity — verified an empty batch trivially passes a per-row-only gate |
| Dead-letter, corrupt JSON | Broken syntax nulls the WHOLE row; a wrong-typed single field nulls only that field — but `_corrupt_record` is populated either way |
| `SparkSession` fixture scope | `session`-scoped amortizes the ~2.8s JVM startup once; `function`-scoped pays setup/teardown every test |
| `chispa.assert_df_equality` defaults | Fails on row order, column order, AND nullable-flag differences — even with identical data. Use `ignore_row_order`/`ignore_nullable` |
| Float columns in tests | `0.1 + 0.2 != 0.3` exactly — use `assert_approx_df_equality(precision=...)` |
| `--conf` / `--py-files` | Both verified to genuinely take effect — `--conf` sets configs before your code runs; `--py-files` makes a separate module importable |
| Cluster sizing rule of thumb | ~5 cores per executor — balances GC pauses (too many cores) against per-JVM overhead (too few) |
| Retry safety | Airflow `retries` is only safe if the job is idempotent — a retry of a non-idempotent job automates the double-counting bug |
