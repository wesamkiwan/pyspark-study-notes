# Module 09 Quiz

Answer each yourself before expanding the answer.

---

**1. `.explain()` is called on a DataFrame with a `groupBy(...).sum(...)` before any action has run
against it, and shows `AdaptiveSparkPlan isFinalPlan=false` with no codegen stars. The same
`.explain()` call, on the same DataFrame object, after `.collect()` has run, shows
`isFinalPlan=true`, codegen stars, and an `AQEShuffleRead coalesced` node. Why do these differ?**

<details>
<summary>Answer</summary>

With Adaptive Query Execution enabled (the Spark 3.5.3 default), Spark plans a query in two
phases: an initial plan built from static estimates, shown before any action runs, and a final
plan AQE rewrites after seeing real runtime statistics from the first shuffle. Verified: calling
`.explain()` before an action can only show the initial, non-final plan; calling it again after an
action shows both, labeled `== Final Plan ==` and `== Initial Plan ==`, with the final plan
reflecting what Spark actually decided at runtime (coalesced shuffle partitions, codegen
boundaries, etc).
</details>

---

**2. What's the practical difference between `.explain()`'s `"simple"`, `"formatted"`, and
`"cost"` modes?**

<details>
<summary>Answer</summary>

`"simple"` (the default) is one compact indented tree of physical operators. `"formatted"` numbers
each node and gives a separate detail block per node below the tree — easier to navigate once a
plan has several operators. `"cost"` shows the *logical* plan annotated with
`Statistics(sizeInBytes=...)` at each node — the size estimate the optimizer actually uses to decide
whether a join qualifies for auto-broadcast (Module 05).
</details>

---

**3. An accumulator-based counter inside a UDF shows 40 total calls across two actions against an
uncached DataFrame (20 rows x 2 passes), but only 20 total calls across the same two actions once
`.cache()` is added before them. What does this prove, precisely?**

<details>
<summary>Answer</summary>

It proves, with an actual measurement rather than an assumption, that (a) an uncached DataFrame is
genuinely recomputed from scratch by every action that needs it, and (b) `.cache()` causes the
*first* action after it to compute and materialize the result, after which every subsequent action
against that same DataFrame reuses the materialized result instead of recomputing — verified: the
second action added zero new calls to the counter.
</details>

---

**4. Right after calling `df.cache()`, before any action runs, an accumulator-based call counter
still reads `0`. What does that tell you about `.cache()` itself?**

<details>
<summary>Answer</summary>

`.cache()` is lazy, exactly like every other Spark transformation — calling it doesn't trigger any
computation on its own. It only marks the DataFrame to be materialized and retained the next time
an action actually runs against it.
</details>

---

**5. After `.unpersist()` is called on a previously-cached DataFrame, the same accumulator-based
call counter jumps back up on the next action. What does this mean for how you should treat
`.unpersist()`?**

<details>
<summary>Answer</summary>

`.unpersist()` immediately releases the cached result — the very next action against that
DataFrame recomputes it from scratch again, verified. Caching isn't a permanent optimization; it
lasts only as long as the cached data is retained. Call `.unpersist()` once you're genuinely done
reusing a DataFrame so Spark can reclaim the space, but not before you're actually finished with it.
</details>

---

**6. Most tutorials describe `.cache()` as shorthand for `.persist(StorageLevel.MEMORY_ONLY)`. Is
that accurate for a DataFrame in PySpark 3.5.3?**

<details>
<summary>Answer</summary>

No — verified false. A plain `df.cache()` on a DataFrame actually applies
`StorageLevel.MEMORY_AND_DISK_DESER`, not `MEMORY_ONLY`. `MEMORY_ONLY` is genuinely the RDD API's
`.cache()` default, but Spark 3.x's Dataset/DataFrame API default is different — it spills to disk
rather than dropping partitions that don't fit in memory, which is a meaningfully safer default for
production use.
</details>

---

**7. `StorageLevel.MEMORY_ONLY`, `.MEMORY_AND_DISK`, and `.DISK_ONLY` all print
`deserialized=False` when inspected on a DataFrame. Does that mean these three levels behave
identically?**

<details>
<summary>Answer</summary>

No — `deserialized` being `False` for all three just reflects that Spark SQL's DataFrame cache
always uses its own internal columnar/serialized format, regardless of which named level you pick
(unlike the RDD API, where `deserialized=True` is possible and means live JVM objects). The three
levels still differ meaningfully on `useMemory`/`useDisk`: `MEMORY_ONLY` keeps data in memory only
and drops (and later recomputes) partitions that don't fit; `MEMORY_AND_DISK` spills to local disk
instead of dropping; `DISK_ONLY` never attempts memory at all.
</details>

---

**8. Reading a small CSV file with `spark.sql.files.maxPartitionBytes` set to a value *smaller*
than the file's actual byte size (verified: `500` against an 804-byte file) produces 2 read
partitions instead of 1. What does this config actually control, and how is that different from
`spark.sql.shuffle.partitions`?**

<details>
<summary>Answer</summary>

`spark.sql.files.maxPartitionBytes` caps how many bytes of an **input file** each partition gets
when Spark first reads it — it controls the number of partitions a DataFrame *starts* with, before
any shuffle. `spark.sql.shuffle.partitions` (Module 06) is a completely separate knob: it controls
the number of **output** partitions produced by a shuffle (a `groupBy`, join, or `orderBy`), and has
no effect on how a file gets split on initial read.
</details>

---

**9. `spark.conf.get("spark.default.parallelism")` raises a `SparkNoSuchElementException`
(`SQL_CONF_NOT_FOUND`), even though the property genuinely exists and has a real value. Why does it
fail, and how do you actually read this value?**

<details>
<summary>Answer</summary>

`spark.conf.get(...)` only searches Spark SQL configs, and `spark.default.parallelism` is a core
Spark (RDD-level) property, not a SQL config — verified, it simply isn't in the namespace
`spark.conf.get` looks at. The correct access path is `spark.sparkContext.defaultParallelism`,
verified to return `16` on a 16-core machine under `local[*]` — matching `os.cpu_count()` exactly,
since default parallelism under local mode is the available core count.
</details>

---

**10. In the Spark UI, which tab gives the clearest live signal of data skew, and what specifically
should you look for on it?**

<details>
<summary>Answer</summary>

The **Stages** tab — look at the min/median/max task duration for a given stage. A large gap
between the median and the max duration (e.g. one task taking far longer than the rest in the same
stage) is the clearest direct evidence of data skew (Module 06), independent of and often faster to
spot than reading `.explain()` output for an `AQEShuffleRead` skew-split node.
</details>

---

Check the boxes in [`PROGRESS.md`](../PROGRESS.md) and move on to Module 10 when it's built.
