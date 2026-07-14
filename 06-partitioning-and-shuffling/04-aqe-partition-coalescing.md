# Lesson 4 — Adaptive Query Execution: Automatic Partition Coalescing

Module 05 Lesson 4 introduced Adaptive Query Execution's skew-join handling. AQE does something
more foundational too, and it directly answers Lesson 3's 200-partition problem: it can **look at
actual partition sizes after a shuffle runs and coalesce the tiny ones back together**, without you
telling it the right number in advance.

## Verified: AQE off vs on, same query, same config

```python
employees.groupBy("department").count()
```

With `spark.sql.adaptive.enabled` set to `"false"`:

```python
grouped.rdd.getNumPartitions()   # 200 -- Lesson 3, the raw configured default
```

With `spark.sql.adaptive.enabled` set back to `"true"` (the actual default in this course's
PySpark 3.5.3 — every session so far has had this on the whole time):

```python
grouped.rdd.getNumPartitions()   # 1
```

Verified: the *exact same query*, same `spark.sql.shuffle.partitions=200`, produces 200 partitions
with AQE off and 1 partition with it on. AQE ran the shuffle, looked at how much data actually
landed in each of the 200 buckets (almost nothing, for 4 result rows), and merged them back down to
a sensible number automatically.

## Seeing it directly in `.explain()`

```python
grouped.collect()   # force execution so AQE's real final plan is available
grouped.explain()
```

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   HashAggregate(keys=[department], functions=[count(1)])
   +- AQEShuffleRead coalesced
      +- ShuffleQueryStage 0
         +- Exchange hashpartitioning(department, 200), ENSURE_REQUIREMENTS, [plan_id=...]
            +- HashAggregate(keys=[department], functions=[partial_count(1)])
               +- FileScan csv [department] ...
+- == Initial Plan ==
   HashAggregate(keys=[department], functions=[count(1)])
   +- Exchange hashpartitioning(department, 200), ENSURE_REQUIREMENTS, [plan_id=...]
      +- HashAggregate(keys=[department], functions=[partial_count(1)])
         +- FileScan csv [department] ...
```

Verified, near-verbatim. Both the `Initial Plan` and `Final Plan` show `Exchange
hashpartitioning(department, 200)` — the shuffle itself still writes 200 partitions' worth of
output, exactly as configured. The difference is entirely in `AQEShuffleRead coalesced`, which only
appears in the `Final Plan`: AQE re-optimizes the plan **after** the shuffle stage completes, once
real partition sizes are known, and reads those 200 tiny physical partitions back as 1 logical
partition for everything downstream. `isFinalPlan=true` (vs `=false`, which you've seen in earlier
modules' `.explain()` output before an action forced execution) tells you whether you're looking at
the original plan or the one AQE actually settled on.

## What this changes about Lesson 3's advice

AQE's `spark.sql.adaptive.coalescePartitions.enabled` (on by default alongside
`spark.sql.adaptive.enabled`) means the "too many tiny partitions" failure mode from Lesson 3 is
now substantially self-healing on the read side — you don't have to predict the perfect
`shuffle.partitions` value for every job size anymore, AQE will trim the excess automatically after
the fact. This doesn't make Lesson 3's guidance obsolete:

- The shuffle **write** itself still happens at whatever `shuffle.partitions` says — 200 tiny files
  written to disk/shuffle-service and then coalesced on read is still more I/O overhead than
  writing a sensible number to begin with. AQE reduces the downstream cost of a bad setting; it
  doesn't eliminate the upstream cost of producing it.
- AQE coalesces **small** partitions together. It does not fix the opposite problem — a single
  partition that's too *large* relative to its peers — with this particular feature. That's data
  skew, and it's `spark.sql.adaptive.skewJoin.enabled` (introduced in Module 05, and the subject of
  Lesson 5's hands-on walkthrough) that specifically targets oversized partitions, by splitting
  them, not merging.

Set `spark.sql.shuffle.partitions` deliberately for your data size regardless — AQE is a safety
net for when that estimate is imperfect, not a reason to stop estimating.

---
**Next:** [Lesson 5 — Data Skew in Practice: Detecting It and Fixing It With Salting](05-skew-and-salting.md)
