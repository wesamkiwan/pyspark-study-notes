# Lesson 5 — Data Skew in Practice: Detecting It and Fixing It With Salting

Module 05 Lesson 4 explained skew conceptually and deferred a hands-on demonstration here, since it
needs more rows and a genuinely lopsided key distribution than the course's 15-row CSVs can
provide. This lesson builds a synthetic skewed dataset in-script, **measures** the imbalance
directly (not just infers it from a slow task), fixes it with salting, and verifies the fix
produces the identical correct result.

## Building a deliberately skewed dataset

```python
N = 100_000
df = spark.range(N).withColumn(
    "key",
    when(col("id") < N * 0.9, "HOT")
    .otherwise(concat_ws("_", lit("key"), (col("id") % 999).cast("string")))
).withColumn("value", lit(1))
```

90,000 of the 100,000 rows (90%) share the single key `"HOT"`; the remaining 10,000 spread evenly
across 999 other keys — a deliberately extreme version of a very real pattern (a dominant
customer/product/default-bucket value that legitimately accounts for most of the traffic).

## Measuring the skew directly: partition sizes, not guesses

`repartition(n, "key")` hash-partitions by key (Lesson 2) — every row with the same key lands in
the same output partition, which means the imbalance in the *key distribution* becomes visible as
an imbalance in *partition sizes*:

```python
sizes_before = df.repartition(8, "key").rdd.glom().map(len).collect()
sorted(sizes_before, reverse=True)
```

```
[91282, 1413, 1411, 1373, 1290, 1120, 1071, 1040]
```

Verified — with 8 output partitions, one of them holds **91,282 rows** (all of `"HOT"`, plus
whatever other keys happened to hash into the same bucket); the other seven share the remaining
~8,700 rows between them. `.rdd.glom()` collects each partition's rows into a list — `.map(len)`
then gives you the row count per partition directly, which is exactly what a wide transformation
keyed on this column would have to process per task. One task here does roughly 65x the work of
its least-loaded neighbor — this is precisely the "199-of-200-tasks-done, one still grinding"
symptom Module 05 Lesson 4 described, now with an actual number behind it instead of a guess.

## The fix: salting

Salting artificially splits a hot key into several sub-keys by appending a random suffix, so the
hot key's rows spread across multiple partitions instead of piling into one:

```python
NUM_SALTS = 8
salted = df.withColumn("salt", floor(rand(seed=42) * NUM_SALTS)) \
           .withColumn("salted_key", concat_ws("_", col("key"), col("salt")))
```

`"HOT"` is now effectively 8 different keys (`"HOT_0"` through `"HOT_7"`), each getting roughly
90,000 / 8 ≈ 11,250 rows — a normal-sized share instead of one giant one.

## Verified: partition sizes after salting, with enough output partitions

```python
sizes_after = salted.repartition(16, "salted_key").rdd.glom().map(len).collect()
sorted(sizes_after, reverse=True)
```

```
[12074, 11960, 11957, 11890, 11845, 11795, 11781, 11773, 711, 648, 644, 644, 594, 567, 561, 556]
```

Verified — the largest partition dropped from **91,282 rows to 12,074**, roughly a 7.5x reduction,
and the top 8 partitions (the former `"HOT"` traffic, now split 8 ways) are all close to the
expected ~11,250-row share. **Note the output partition count matters here too:** with only 8
output partitions (matching `NUM_SALTS`), hash collisions between the 8 salted-hot-key buckets and
the 999 other genuinely-distinct keys still produced meaningful imbalance in testing — going to 16
output partitions (more buckets than salt values, reducing collision odds) is what actually
produced the clean, even result above. **Salting alone isn't sufficient — pair it with enough
output partitions that your salted keys aren't immediately colliding back into an uneven layout.**

## Correctness: salting must not change the answer

Splitting `"HOT"` into 8 sub-keys means a naive `groupBy("salted_key")` would produce 8 partial
results for what should be one logical group. The fix is a **two-stage aggregation**: aggregate by
`(key, salt)` first, then aggregate those partial results by `key` alone:

```python
stage1 = salted.groupBy("key", "salt").agg(spark_sum("value").alias("partial_total"))
stage2 = stage1.groupBy("key").agg(spark_sum("partial_total").alias("total"))
```

Verified against the plain (unsalted) aggregation:

```python
plain_total = df.groupBy("key").agg(spark_sum("value").alias("total")) \
    .filter(col("key") == "HOT").collect()[0]["total"]          # 90000

salted_total = stage2.filter(col("key") == "HOT").collect()[0]["total"]   # 90000
plain_total == salted_total   # True
```

Both the skewed `"HOT"` key and an ordinary cold key (verified separately) produce identical totals
through the salted, two-stage path as through the plain one — salting changed the *physical
distribution* of the work, not the *logical result*. This is the general shape of the technique for
any associative aggregation (`sum`, `count`, `min`, `max`); it extends to a skewed **join** the same
way in spirit — salt the large/skewed side's key, and "explode" the small side into one row per
salt value so every salted row on the large side still finds a match — but the aggregation version
here is the clearest place to see the mechanics and verify correctness directly.

## When to actually reach for this

Salting is manual work and adds a second aggregation stage — don't reach for it as a first
response to "my join/groupBy is slow." Check AQE's automatic skew-join handling first (Module 05
Lesson 4) — for many real skew cases in Spark 3.x, it's already handling the worst of this for you.
Salting earns its complexity when you've confirmed (via partition-size inspection like this lesson,
or the Spark UI in Module 09) that a specific key's imbalance is severe enough that AQE's automatic
splitting isn't enough on its own.

---
This closes out Module 06. Next: [`exercises/`](exercises/), then [`quiz.md`](quiz.md).
