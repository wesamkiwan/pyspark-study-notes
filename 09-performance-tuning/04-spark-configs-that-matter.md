# Lesson 4 — Spark Configs That Matter

Spark has hundreds of configuration properties; a working data engineer needs to genuinely
understand a small handful. This lesson verifies what each one actually controls — including one
you'd expect to read like the others but that fails outright if you try.

## Verified defaults on this course's PySpark 3.5.3 install

```python
for c in ["spark.sql.shuffle.partitions", "spark.sql.autoBroadcastJoinThreshold",
          "spark.sql.adaptive.enabled", "spark.sql.adaptive.coalescePartitions.enabled",
          "spark.sql.adaptive.skewJoin.enabled", "spark.sql.adaptive.advisoryPartitionSizeInBytes",
          "spark.sql.files.maxPartitionBytes", "spark.sql.execution.arrow.maxRecordsPerBatch"]:
    print(c, "=", spark.conf.get(c))
```

Verified output:

```
spark.sql.shuffle.partitions = 200
spark.sql.autoBroadcastJoinThreshold = 10485760b
spark.sql.adaptive.enabled = true
spark.sql.adaptive.coalescePartitions.enabled = true
spark.sql.adaptive.skewJoin.enabled = true
spark.sql.adaptive.advisoryPartitionSizeInBytes = 67108864b
spark.sql.files.maxPartitionBytes = 134217728b
spark.sql.execution.arrow.maxRecordsPerBatch = 10000
```

You've already met most of these, verified in context, in earlier modules — this table is the
single reference point going forward:

| Config | Verified default | What it controls | Covered |
|---|---|---|---|
| `spark.sql.shuffle.partitions` | `200` | Output partition count for any shuffle (`groupBy`, joins, `orderBy`) | Module 06 |
| `spark.sql.autoBroadcastJoinThreshold` | `10485760` (10 MiB) | Max estimated table size Spark will auto-broadcast in a join | Module 05 |
| `spark.sql.adaptive.enabled` | `true` | Master switch for AQE (runtime plan rewriting) | Module 06, Lesson 1 |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | AQE merges small post-shuffle partitions together | Module 06 |
| `spark.sql.adaptive.skewJoin.enabled` | `true` | AQE splits detected skewed join partitions | Module 06 |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | `67108864` (64 MiB) | AQE's target size when coalescing/splitting partitions | New here |
| `spark.sql.files.maxPartitionBytes` | `134217728` (128 MiB) | Max bytes per **input** split when reading files | New here |
| `spark.sql.execution.arrow.maxRecordsPerBatch` | `10000` | Rows per Arrow batch for `pandas_udf` | Module 08 |

## New this lesson: `maxPartitionBytes` controls *input* partitioning, not shuffle output

Everything in Module 06 was about *shuffle* partition counts. `spark.sql.files.maxPartitionBytes`
is a different knob entirely — it caps how many bytes of an input file each partition gets when
Spark first reads it, before any shuffle happens.

```python
size = os.path.getsize("data/orders.csv")   # 804 bytes
for max_bytes in ["134217728", "2000", "500"]:
    spark.conf.set("spark.sql.files.maxPartitionBytes", max_bytes)
    df = spark.read.csv("data/orders.csv", header=True, inferSchema=True)
    print(max_bytes, "->", df.rdd.getNumPartitions())
```

Verified output:

```
134217728 -> 1
2000      -> 1
500       -> 2
```

At the 128 MiB default, an 804-byte file obviously fits in one split — one partition. Lowering the
cap below the file's actual size (`500` bytes, smaller than the 804-byte file) forces Spark to
split it into 2 partitions on read — verified directly, not inferred. On real-sized data, this is
the setting that controls how many partitions your job *starts* with, before Module 06's shuffle
mechanics ever come into play; too few input partitions on a huge file means too little initial
read parallelism, too many means excessive small-task overhead.

## The trap: `spark.default.parallelism` isn't readable the way SQL configs are

Every config above is read with `spark.conf.get(...)`. Try that same pattern on
`spark.default.parallelism` and it fails:

```python
spark.conf.get("spark.default.parallelism")
```

Verified error:

```
SparkNoSuchElementException: [SQL_CONF_NOT_FOUND] The SQL config "spark.default.parallelism"
cannot be found. Please verify that the config exists.
```

**`spark.default.parallelism` is a core Spark (RDD-level) property, not a SQL config** —
`spark.conf.get()` only searches SQL configs, and this one isn't one. The correct way to read it:

```python
spark.sparkContext.defaultParallelism
```

Verified: `16` on this machine, exactly matching `os.cpu_count()` — under `local[*]`, default
parallelism is the number of available cores. This value is what RDD operations (and DataFrame
operations that fall back to RDD-level parallelism, like `spark.range(...)` with no explicit
partition count) use when no shuffle or file-read partitioning applies — a distinct knob from both
`shuffle.partitions` and `maxPartitionBytes`, easy to conflate with either if you haven't verified
which config family each one actually belongs to.

---
**Next:** [Lesson 5 — A Performance Debugging Workflow](05-performance-debugging-workflow.md)
