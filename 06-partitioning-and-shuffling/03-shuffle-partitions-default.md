# Lesson 3 — spark.sql.shuffle.partitions: The 200 Default Trap

Every `SparkSession` built in this course so far has included
`.config("spark.sql.shuffle.partitions", "8")`, without explanation. Here's the explanation.

## The default is 200 — regardless of your data's size

```python
spark = SparkSession.builder.appName("no-override").master("local[*]").getOrCreate()
spark.conf.get("spark.sql.shuffle.partitions")
```

```
'200'
```

Verified. This single number decides how many partitions **every** wide transformation
(`groupBy`, `join`, `orderBy`/`sort`, `distinct`) produces on its output side — completely
unrelated to how many rows or partitions the input actually had.

## Verified: 15 rows become 200 partitions

```python
employees = spark.read.csv("data/employees.csv", header=True, schema=emp_schema)
employees.rdd.getNumPartitions()   # 1 -- Lesson 1
```

```python
grouped = employees.groupBy("department").count()
grouped.rdd.getNumPartitions()
```

With `spark.sql.adaptive.enabled` set to `false` so the raw configured number is visible (Lesson 4
covers why this matters):

```
200
```

**A 15-row `groupBy` that could only ever produce 4 result rows (one per department) gets spread
across 200 partitions.** 196 of them are completely empty; the other 4 hold one row each. Every one
of those 200 partitions still becomes a task with the fixed per-task scheduling overhead from
Lesson 2 — on a local machine with 16 cores, that's 200 tasks worth of overhead to compute an
aggregation that has 4 possible answers.

## Why 200 exists, and why it's wrong for most workloads

200 was chosen years ago as a reasonable one-size-fits-all default for the shuffle-heavy, large
cluster workloads Spark was originally built around — it is not tuned to *your* data or *your*
cluster, and it's the same number whether your dataset is 15 rows or 15 billion. Two failure
directions, both real:

- **Too many partitions for small/medium data** (this course's entire dataset, and a large share
  of real ad-hoc/analytics jobs): hundreds of nearly-empty partitions, each paying full per-task
  overhead for almost no work — verified above.
- **Too few partitions for genuinely large data**: 200 partitions across a dataset that's actually
  hundreds of GB means each partition is enormous, each task takes a long time, and you're
  under-using a cluster that has far more than 200 cores available to help.

## What this course has been doing about it, and the two real fixes

This course's `.config("spark.sql.shuffle.partitions", "8")` picks a number that actually fits
15-row CSVs on a local machine — a manual, upfront choice matched to known, small data. That's
fix #1: **set `spark.sql.shuffle.partitions` deliberately, based on your actual data size and
cluster core count**, rather than leaving the one-size-fits-all default in place. A common rule of
thumb for batch jobs is roughly 2–4x your total available cores, adjusted from there based on
actual partition sizes you observe.

Fix #2 is the one that's made manual tuning matter less over time, and it's the subject of the
next lesson: **Adaptive Query Execution**, which can look at real post-shuffle partition sizes
while the job is running and coalesce the tiny ones back down automatically — without you having
to predict the right number in advance at all.

---
**Next:** [Lesson 4 — Adaptive Query Execution: Automatic Partition Coalescing](04-aqe-partition-coalescing.md)
