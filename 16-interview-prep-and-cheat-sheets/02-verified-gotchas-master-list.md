# Verified Gotchas — The Master List

Every entry below was actually hit and verified while building this course — not copied from a
blog post, not "commonly said to happen." Organized by theme, since interviewers (and real
incidents) tend to cluster around these same shapes: silent wrong answers, laziness/recompute
traps, and surprising defaults.

## Theme 1: Silent Wrong Answers (no error, but the result is incorrect)

These are the most dangerous class — a job "succeeds" and produces a plausible-looking, wrong
result, with nothing telling you to look closer.

1. **ZIP code `"00501"` under `inferSchema=True`** silently becomes int `501` — leading zeros
   gone, no warning. (Module 02)
2. **Multi-file Parquet reads with schema drift, no `mergeSchema`**: Spark picks ONE file's
   schema for the whole read, silently dropping a column from every row — including rows from
   files that genuinely have it on disk. (Module 02)
3. **`CAST('bad' AS DOUBLE)`** returns `NULL`, no error — indistinguishable from a genuinely
   missing value. (Module 04)
4. **`.union()` matches columns by position, not name** — mismatched column order silently
   mislabels data with no error, since the types happen to be compatible. (Module 03)
5. **`WHERE x NOT IN (subquery containing a NULL)`** silently returns zero rows for every input,
   with no error — three-valued logic poisons the whole `AND` chain. (Module 04)
6. **The default window frame (no explicit `rowsBetween`)** is `RANGE`, not `ROWS` — tied
   `orderBy` values all get the SAME cumulative running total, a silent, data-dependent bug that
   only appears once your ordering column happens to have duplicates. (Module 07)
7. **A `@udf`'s declared `returnType` is never checked** against what the function actually
   returns — a type mismatch silently becomes `NULL` in every row. (Module 08)
8. **A streaming CSV source without `.option("header", "true")`** parses every file's header row
   as a literal data row — garbage/NULL values, no error. (Module 10)
9. **A late-arriving row past a watermark's tolerance** is silently dropped from every window
   aggregate, with zero error — but it's still counted in that trigger's `numInputRows`, which
   makes the metric itself misleading about correctness. (Module 10)
10. **`append` + `mergeSchema=true` against an already-existing key** creates a DUPLICATE row, not
    an update — `mergeSchema` only ever solves the schema problem, never the row-identity one.
    (Module 11)
11. **A naive `append`-based load run twice** (a retry) silently DOUBLES the row count — no error,
    just quietly wrong totals downstream. (Module 12)
12. **A quality gate built only from per-row checks** trivially PASSES on a completely empty
    batch — every check is "count of bad rows is zero," which is vacuously true with zero rows
    total. (Module 12/13)

## Theme 2: Laziness, Recompute, and Caching Traps

Spark's lazy evaluation model is powerful but has sharp edges specifically around *when* a
DataFrame's definition actually gets re-evaluated.

13. **`.count()` on a DataFrame descended from a corrupt-record read is unreliable**, even after
    an explicit `.filter()`, even swapping `.count()` for `.agg(count("*"))` — Spark's optimizer
    has a fast path that can skip the filter entirely for certain text-source counts. Only
    `.cache()`/`.localCheckpoint()` before counting genuinely fixes it. (Module 02)
14. **Calling `.explain()` before vs. after an action, on the SAME DataFrame object**, shows
    different plans under AQE — `isFinalPlan=false` before, `=true` (with `AQEShuffleRead
    coalesced`) after. Habitually calling `.explain()` right after building a query, rather than
    after triggering it, shows you a stale, non-final plan. (Module 09)
15. **Calling more than one uncached action on a `foreachBatch` batch DataFrame** re-executes the
    source scan once per action — verified to exactly double both real work and the reported
    `numInputRows` metric for 2 actions. (Module 10)
16. **A classic SCD2 bug**: building a "which keys changed" DataFrame lazily from the table, then
    reusing it AFTER a `MERGE` that mutates that same table, silently finds zero rows on the
    second use — the underlying table has already changed. **Verified that even `.cache()` does
    NOT fix this** — Delta invalidates a cached DataFrame once the table it reads from is
    mutated. The only fix that actually works: `.collect()` the changed keys to plain Python data
    BEFORE the `MERGE` runs. (Module 12)

## Theme 3: Defaults That Surprise

Configuration and API defaults that are commonly stated wrong, or just aren't what intuition
suggests.

17. **`DataFrame.cache()` does NOT default to `MEMORY_ONLY`** — verified
    `MEMORY_AND_DISK_DESER`. `MEMORY_ONLY` is the RDD API's default; Spark 3.x's DataFrame API
    default is safer (spills instead of dropping partitions). (Module 09)
18. **`spark.sql.shuffle.partitions` defaults to 200**, completely unrelated to your data's actual
    size — verified to produce 200 partitions grouping 15 rows into 4 groups. (Module 06)
19. **`spark.conf.get("spark.default.parallelism")` throws `SparkNoSuchElementException`** —
    it's a core/RDD-level property, not a SQL config; the real access path is
    `spark.sparkContext.defaultParallelism`. (Module 09)
20. **`spark.sql.crossJoin.enabled` defaults to `true`** — a join with no condition at all
    silently runs as a full cartesian product rather than erroring. (Module 05)
21. **`autoBroadcastJoinThreshold` defaults to 10MB** and auto-broadcasts a small join side with
    zero hint required — a `BroadcastHashJoin` in `.explain()` you never asked for. (Module 05)
22. **`Trigger.ProcessingTime`'s first micro-batch fires immediately**, regardless of the
    configured interval — only subsequent batches wait the full interval. (Module 10)
23. **`Trigger.AvailableNow` queries stop THEMSELVES** once the backlog is drained — `isActive`
    becomes `False` with no `.stop()` call, unlike every other trigger. (Module 10)
24. **`OPTIMIZE` does not delete old files** — verified file count went UP (16→17), not down;
    `VACUUM` is the separate step that actually deletes, and refuses to run below its retention
    safety threshold without an explicit override. (Module 11)
25. **`chispa.assert_df_equality` fails by default** on row order, column order, AND
    nullable-flag differences — even with byte-identical data. (Module 13)

## Theme 4: Concurrency and Fault Tolerance

26. **Checkpoint-based streaming fault tolerance was verified the strong way**: two genuinely
    separate Python processes/JVMs sharing a `checkpointLocation` — a restarted query never
    reprocesses already-committed files, with zero duplicate output. (Module 10)
27. **Two threads racing a Delta `MERGE` against the same row** reproduced a real
    `io.delta.exceptions.ConcurrentAppendException` live — Delta's optimistic concurrency
    control rejects the loser's stale-based commit outright rather than letting it race silently.
    (Module 11)
28. **`RESTORE` adds a new version rather than erasing history** — verified, the version being
    restored FROM is still present and queryable in `history()` afterward. (Module 11)

## Theme 5: Testing-Specific Surprises

29. **`0.1 + 0.2 != 0.3`** in IEEE 754 floating point — not a Spark quirk, but it means any test
    comparing computed float columns with exact equality is fragile by construction.
    (Module 13)
30. **An empty DataFrame still has a real, non-zero partition count** (`local[*]`'s default
    parallelism) — every partition just happens to be empty. (Module 13)
31. **`function`-scoped vs `session`-scoped `SparkSession` pytest fixtures**: the expensive part
    (JVM + Py4J gateway startup, verified ~2.8s) only needs to happen once per test session, not
    once per test — session scope amortizes it, function scope pays it repeatedly.
    (Module 13)

## How to actually use this list

Notice the shape almost every entry shares: **something succeeds without an error, and the result
is either subtly wrong or a real production risk.** This is the single most valuable pattern to
internalize from this entire course — Spark, like most large distributed systems, tends to fail
by silently doing something plausible-but-wrong rather than raising a loud, easy-to-notice error.
The discipline this course modeled throughout — actually running code and reading real output
instead of trusting a mental model — is the actual skill being tested when an interviewer asks
"tell me about a subtle bug you found in a Spark pipeline."
