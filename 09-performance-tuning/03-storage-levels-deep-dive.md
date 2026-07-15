# Lesson 3 — Storage Levels Deep Dive

`.cache()` is really `.persist()` with a default storage level baked in. Every `StorageLevel` is
just four independent flags — `useMemory`, `useDisk`, `deserialized`, and a replication factor —
and reading those flags directly, rather than trying to remember what each named constant means,
is the fastest way to reason about a storage level you haven't used before.

## The flags, verified

```python
from pyspark import StorageLevel

for lvl in [StorageLevel.MEMORY_ONLY, StorageLevel.MEMORY_AND_DISK,
            StorageLevel.DISK_ONLY, StorageLevel.OFF_HEAP]:
    print(lvl, "| useMemory=", lvl.useMemory, "useDisk=", lvl.useDisk,
          "deserialized=", lvl.deserialized)
```

Verified output:

```
MEMORY_ONLY      -> useMemory=True  useDisk=False deserialized=False
MEMORY_AND_DISK  -> useMemory=True  useDisk=True  deserialized=False
DISK_ONLY        -> useMemory=False useDisk=True  deserialized=False
OFF_HEAP         -> useMemory=True  useDisk=True  deserialized=False
```

- **`useMemory`**: will this level try to keep data in the JVM heap at all?
- **`useDisk`**: if it doesn't fit in memory (or isn't requested there), does it spill to local
  disk instead of being dropped?
- **`deserialized`**: is the cached data kept as live JVM objects (`True`, faster to read, more
  memory) or as a serialized byte format (`False`, slower to read, less memory)? Note all four
  DataFrame-facing levels above show `deserialized=False` — Spark SQL's DataFrame cache always
  uses its own internal columnar/serialized in-memory format regardless of which named constant you
  pick, unlike the older RDD API where `MEMORY_ONLY` genuinely means live deserialized Java objects.
- **No flag = data is simply dropped** if it doesn't fit and the level doesn't allow disk —
  `MEMORY_ONLY` with insufficient memory silently evicts partitions (LRU) rather than erroring;
  they're just recomputed from the original lineage the next time they're needed.

## The trap: `.cache()`'s actual default level is not what most tutorials say

Plenty of material describes `.cache()` as "shorthand for `.persist(StorageLevel.MEMORY_ONLY)`" —
true for the RDD API, but verified **false for DataFrames**:

```python
df = spark.range(5).cache()
df.count()
print(df.storageLevel)
print(df.storageLevel == StorageLevel.MEMORY_AND_DISK_DESER)
```

Verified output:

```
Disk Memory Deserialized 1x Replicated
True
```

**A plain `df.cache()` on a DataFrame actually applies `MEMORY_AND_DISK_DESER`** — Spark 3.x's
Dataset/DataFrame API default, not `MEMORY_ONLY`. This matters in practice: unlike `MEMORY_ONLY`,
which would silently drop partitions that don't fit in memory (forcing an expensive recompute the
next time they're touched), the DataFrame default spills to local disk instead — slower to read
back than memory, but it won't force a full recomputation just because the data didn't fit.

## Choosing deliberately

- **`MEMORY_AND_DISK_DESER` (the DataFrame `.cache()` default):** a safe general default — spills
  rather than losing data, deserialized for faster reads once cached. Good enough for most course
  and small/medium production workloads; leave it as-is unless you have a specific reason not to.
- **`MEMORY_ONLY`:** only when you're confident the data comfortably fits in available executor
  memory and you'd genuinely prefer a partition to be dropped-and-recomputed over ever touching
  disk (rare — usually you'd rather have the disk fallback).
- **`DISK_ONLY`:** when the cached data is too large for memory to hold at all, but recomputing it
  from scratch (a slow join, an expensive read) is still costlier than a disk read. Slower than any
  memory-based level, but still faster than not caching.
- **`OFF_HEAP`:** keeps cached data outside the JVM heap (needs `spark.memory.offHeap.enabled` and a
  size set) — reduces JVM garbage-collection pressure for very large cached datasets, at the cost of
  extra configuration. A specialized choice, not a default.

Explicit choice: `df.persist(StorageLevel.DISK_ONLY)` instead of `.cache()`. Always call
`.unpersist()` when done, regardless of which level you chose (Lesson 2).

**Windows-specific note, verified on this course's setup:** stopping a `SparkSession` shortly after
using a disk-backed storage level occasionally logs a harmless
`ERROR DiskBlockManager: Exception while deleting local spark dir` /
`Failed to delete: ...\blockmgr-.../rdd_...` on shutdown — a benign Windows file-handle race during
temp-directory cleanup, not a data-correctness problem. It didn't appear on every run in testing
(non-deterministic), and every assertion about the cached data itself still passed when it did
appear. If you see it, it's this — not a sign your `persist()` call failed.

---
**Next:** [Lesson 4 — Spark Configs That Matter](04-spark-configs-that-matter.md)
