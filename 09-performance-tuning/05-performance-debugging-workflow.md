# Lesson 5 — A Performance Debugging Workflow

Every tool from Lessons 1-4 (and Modules 05-08) is diagnostic on its own. This lesson is about
combining them into one repeatable workflow, anchored on the Spark UI — the one tool in this module
you can't fully capture in a static code example, since it's a live web page your driver serves
while a job runs.

## The Spark UI: what to click, not a screenshot

Any script that creates a `SparkSession` opens a web UI on your **driver**, by default at
`http://localhost:4040` (if that port's busy, Spark tries `4041`, `4042`, ... — check your driver
log's startup lines for the actual port it bound). Keep a script running (or add a
`input("press enter to exit")` at the end, or just run something slow enough to look at while it's
executing) and open that URL in a browser. Four tabs matter most for performance work:

- **Jobs**: one entry per Spark job (roughly, one per action). Click into a slow one to see its
  stages and how long each took — this is where you'd notice, for example, that one job has far
  more tasks than the others (Module 06's partition-count story) or an unusually long single stage.
- **Stages**: per-stage task counts, and — critically for skew (Module 06) — the **min/median/max
  task duration** for that stage. A huge gap between median and max duration for the same stage is
  the single clearest live signal of data skew you'll find anywhere; `.explain()` can show you
  `AQEShuffleRead` handled a skew split, but the Stages tab shows you the actual task-time damage
  before or independent of that.
- **Storage**: every DataFrame currently cached, its storage level (Lesson 3), and how much of it
  is actually in memory vs. spilled to disk. This is the direct, ground-truth way to confirm
  `.cache()` did what you expect — no accumulator trick needed once you have a live UI to check
  against.
- **SQL / DataFrame**: the same physical plan `.explain()` prints, rendered as a diagram, with
  actual runtime metrics (rows produced, peak memory, spill size) attached to each node — the live
  version of Lesson 1's `.explain("formatted")`, but annotated with what actually happened rather
  than what the plan says should happen.

## Putting it together: a worked case study

Say a pipeline runs an expensive per-row UDF-based transformation and reuses the result across two
separate downstream aggregations — a common real shape (compute a derived column once, report on
it multiple ways). Lesson 2 verified the cost of getting this wrong:

```python
transformed = employees.withColumn("some_derived_col", expensive_udf(col("salary")))

report_a = transformed.groupBy("department").avg("some_derived_col")
report_b = transformed.filter(col("some_derived_col") > 100000).count()

report_a.collect()
report_b.collect()
```

Without caching `transformed`, **both `report_a` and `report_b` recompute the UDF from scratch** —
verified in Lesson 2 as a full doubling of UDF calls (`40` vs `20` for a trivial example; on a
genuinely expensive UDF over real data volume, this is the difference between a job finishing once
versus finishing twice). The fix, also verified in Lesson 2:

```python
transformed = employees.withColumn("some_derived_col", expensive_udf(col("salary"))).cache()

report_a = transformed.groupBy("department").avg("some_derived_col")
report_b = transformed.filter(col("some_derived_col") > 100000).count()

report_a.collect()   # first action: computes and materializes transformed
report_b.collect()   # second action: reuses the cached result, no UDF recompute
```

## The checklist

When a pipeline is slower than expected, work through these in order:

1. **Is the same expensive DataFrame computed more than once with no cache in between?** (Lesson 2)
   Check the Storage tab — if the DataFrame you'd expect to be cached isn't listed there, that's
   your answer.
2. **Does `.explain()`, called *after* an action, show anything unexpected?** (Lesson 1) A missing
   broadcast you expected, an `Exchange` you didn't expect, a `SinglePartition` shuffle (Module 07)
   on something that should have a `partitionBy`.
3. **Does the Stages tab show a skewed task-duration spread?** (Module 06, this lesson) If one task
   in a stage takes 10x the median, that's data skew, not a config problem — the fix is Module 06's
   salting technique or trusting AQE's skew-join handling, not touching `shuffle.partitions`.
4. **Are the relevant configs (Lesson 4) actually set to what you think they are?** — a config
   changed in one notebook cell/script and expected to persist elsewhere is a common, entirely
   silent way to be debugging the wrong assumption.

---
This is the last lesson in Module 09. Continue to [`exercises/`](exercises/), then
[`solutions/`](solutions/), then [`quiz.md`](quiz.md).
