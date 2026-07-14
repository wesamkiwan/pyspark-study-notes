# Lesson 2 — repartition(n, col): Hash Partitioning and Key Colocation

Module 02 Lesson 4 covered `repartition(n)` and `coalesce(n)` at a mechanical level — full shuffle
vs cheap merge, controlling output file count. This lesson covers the version that takes a
**column**, `repartition(n, col)`, why it behaves fundamentally differently from plain
`repartition(n)`, and the "too many partitions" cost Module 02 deferred to here.

## `repartition(n)`: round-robin, no key guarantee

`spark_partition_id()` is a handy diagnostic function purely for this kind of inspection — it
returns which physical partition each row currently lives in:

```python
from pyspark.sql.functions import spark_partition_id

employees.repartition(4).withColumn("p", spark_partition_id()) \
    .groupBy("department", "p").count().orderBy("department").show()
```

```
+-----------+---+-----+
| department|  p|count|
+-----------+---+-----+
|Engineering|  0|    1|
|Engineering|  1|    2|
|Engineering|  2|    2|
|Engineering|  3|    1|
|    Finance|  2|    1|
|    Finance|  3|    1|
|  Marketing|  0|    1|
|  Marketing|  1|    1|
|  Marketing|  3|    1|
|      Sales|  0|    1|
|      Sales|  1|    1|
|      Sales|  2|    1|
|      Sales|  3|    1|
+-----------+---+-----+
```

Verified — `Engineering`'s 6 rows are scattered across **all four** partitions. Plain
`repartition(n)` distributes rows round-robin (or by a random-ish scheme), with no relationship to
any column's values. It's the right tool when your only goal is "spread this out into `n` roughly
equal pieces," e.g. before writing (Module 02 Lesson 4).

## `repartition(n, col)`: hash partitioning — every value of `col` lands in exactly one partition

```python
employees.repartition(4, "department").withColumn("p", spark_partition_id()) \
    .select("department", "p").distinct().orderBy("department").show()
```

```
+-----------+---+
| department|  p|
+-----------+---+
|Engineering|  2|
|    Finance|  1|
|  Marketing|  1|
|      Sales|  0|
+-----------+---+
```

Verified — every one of `Engineering`'s rows lands in partition 2, and only partition 2. This is a
**hash partitioning**: Spark computes `hash(department) % n` and sends every row with the same
`department` value to the same output partition, deterministically. (`Finance` and `Marketing`
landing in the same partition, 1, is expected and fine — hash partitioning guarantees "same value →
same partition," not "one partition per distinct value.")

## Why key colocation actually matters

A `groupBy("department")` or a `join(..., on="department")` needs every row sharing a
`department` value physically together on one partition before it can aggregate/match them — that
requirement *is* the shuffle (Module 01's wide-transformation discussion). If you already
`repartition(n, "department")` earlier in the pipeline and a later operation groups or joins on
that *exact same column with a compatible partition count*, Spark can potentially skip re-shuffling
that data, because it's already colocated correctly. This matters most when you're about to do
**multiple** operations keyed on the same column (several joins against the same key, or a
join immediately followed by a `groupBy` on that join key) — pre-partitioning once can save
redoing the equivalent shuffle work repeatedly. It's a real optimization, but a situational one:
don't reach for it reflexively on every pipeline, only when you can see the same key being used to
combine data more than once downstream.

## The cost Module 02 deferred here: over-partitioning

Module 02 Lesson 4 flagged too *few*, too-large partitions (the tiny-files write problem) and
promised the opposite trade-off here. Every partition becomes at least one **task**, and every
task carries fixed scheduling overhead — the driver has to serialize it, send it to an executor,
track its status, and collect its result, regardless of how much actual data is inside it. Spreading
a genuinely small dataset across thousands of partitions (or output files) means paying that fixed
per-task overhead thousands of times over for almost no work each time — often *slower* overall
than fewer, larger partitions would have been, despite "more partitions" sounding like "more
parallelism." The right number of partitions is the smallest number that still keeps every
available core busy with a reasonably-sized chunk of work — not the largest number you could
technically create.

---
**Next:** [Lesson 3 — spark.sql.shuffle.partitions: The 200 Default Trap](03-shuffle-partitions-default.md)
