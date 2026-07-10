# Lesson 4 — Writing Data Like a Pro

Reading gets the attention, but writing is where you decide the shape of data every downstream
consumer (including future-you) has to live with. Getting layout and write behavior right here
saves real pain later.

## Write modes

```python
df.write.mode("overwrite").parquet(path)   # replace whatever is at `path` entirely
df.write.mode("append").parquet(path)       # add to what's already there
df.write.mode("errorifexists").parquet(path) # default if you omit .mode(...) at all — fails safe
df.write.mode("ignore").parquet(path)        # silently do nothing if `path` already exists
```

Verified: writing to a path that already has data, using the default/`errorifexists` mode:

```
AnalysisException: [PATH_ALREADY_EXISTS] Path ... already exists.
Set mode as "overwrite" to overwrite the existing path.
```

**This default is deliberate and good** — Spark refuses to silently clobber or silently no-op
unless you explicitly say so. In a real pipeline, be equally deliberate: `overwrite` for a
full-refresh table, `append` for incremental loads (paired with logic to avoid double-counting
reprocessed data — a preview of idempotency, covered fully in Module 12), and treat `ignore` with
suspicion — it can hide the fact that a write you expected to happen didn't.

## Partitioning your output with `partitionBy`

```python
employees.write.mode("overwrite").partitionBy("department").parquet("output/by_dept")
```

Verified — this creates one subdirectory per distinct value of `department`, using Hive-style
partition directory naming:

```
by_dept/department=Engineering/
by_dept/department=Finance/
by_dept/department=Marketing/
by_dept/department=Sales/
```

**Why this matters for performance, not just organization:** when you later read this data and
filter on the partition column, Spark can skip entire directories without opening a single file
inside them — **partition pruning**. Verified via `.explain()` after filtering on the partition
column:

```
FileScan parquet [...] PartitionFilters: [isnotnull(department#48), (department#48 = Engineering)]
```

`PartitionFilters` (as opposed to the `PushedFilters` you saw in Module 01 for row-level
predicate pushdown) means Spark decided which *directories* to read from the partition layout
itself, before opening any file — for a large dataset this can turn "scan everything" into "scan
1/N of everything," for free, just from choosing a sensible partition column.

**Choosing a partition column well matters.** Good candidates: a column with a moderate number
of distinct values that you frequently filter on (`department`, `region`, or very commonly a date
column like `order_date` truncated to day/month). Bad candidates: high-cardinality columns like
`emp_id` or a raw timestamp with second precision — you'd end up with thousands/millions of
tiny partition directories, which hurts far more (metadata overhead, tiny files) than it helps.
We go deeper on this trade-off in Module 06.

## Controlling output file count: `repartition` vs `coalesce`

The number of files Spark writes is determined by the DataFrame's number of **partitions in
memory** at write time — not something you set directly on the writer. Verified:

```python
employees = spark.read.csv("data/employees.csv", header=True, inferSchema=True)
print(employees.rdd.getNumPartitions())   # -> 1 (small file, read in local mode)

spread = employees.repartition(4)
print(spread.rdd.getNumPartitions())       # -> 4

spread.write.mode("overwrite").parquet("output/spread")     # writes 4 files
spread.coalesce(1).write.mode("overwrite").parquet("output/collapsed")  # writes 1 file
```

Verified file counts: `spread` (4 in-memory partitions) wrote **4** Parquet files;
`spread.coalesce(1)` wrote **1**.

- **`repartition(n)`** does a full shuffle to redistribute data into exactly `n` partitions —
  can increase *or* decrease the count, and is relatively expensive (it's a wide transformation,
  Module 01's shuffle discussion applies directly).
- **`coalesce(n)`** only *combines* existing partitions to reduce the count — cheaper than
  `repartition` because it avoids a full shuffle where possible, but it can **only reduce**
  partition count, never increase it, and can produce uneven partition sizes if the source
  partitions were already uneven.

**The "too many tiny files" problem this solves:** if an upstream job left you with, say, 500
partitions for a dataset that's actually small, writing it as-is produces 500 tiny files — every
one of which costs real overhead for any system that later has to list/open them (this is a
notorious real-world performance killer, especially on cloud object storage like S3, where each
file listed/opened is a network round trip). `coalesce()` before writing is the standard fix.
Going the other direction — spreading data too thin across many small partitions — has its own
cost, covered in depth in Module 06.

---
**Next:** [Lesson 5 — Schema Evolution and Mismatches](05-schema-evolution-and-mismatches.md)
