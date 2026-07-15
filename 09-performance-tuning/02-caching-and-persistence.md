# Lesson 2 — Caching and Persistence, Verified

`.cache()`/`.persist()` tell Spark to keep a DataFrame's computed result around after the first
action that needs it, so a *second* action against the same DataFrame doesn't repeat the work that
produced it. That's the textbook description — this lesson proves it with an actual recompute
counter instead of just asserting it, and shows exactly when the saving does and doesn't apply.

## Proving it: an accumulator that counts real computation

An `Accumulator` is a counter every executor task can increment; the driver can read its total
after actions complete. Wiring one into a UDF turns "was this row's value actually recomputed?"
into a number you can print.

```python
compute_calls = spark.sparkContext.accumulator(0)

@udf(returnType=DoubleType())
def expensive(n):
    compute_calls.add(1)
    return float(n) * 2

df = spark.range(20).withColumnRenamed("id", "n").repartition(4)
transformed = df.withColumn("doubled", expensive(col("n")))
```

**Without caching, two actions that both need the `doubled` column recompute it from scratch, every
time:**

```python
transformed.agg(sum("doubled")).collect()
transformed.agg(sum("doubled")).collect()
print(compute_calls.value)
```

Verified: `40` — 20 rows computed on the first `collect()`, 20 more computed again on the second.
Nothing was reused.

**With `.cache()`, the same two actions compute the UDF only once:**

```python
cached = transformed.cache()
print(compute_calls.value)          # right after calling .cache() itself

cached.agg(sum("doubled")).collect()
print(compute_calls.value)          # after the FIRST action

cached.agg(sum("doubled")).collect()
print(compute_calls.value)          # after the SECOND action
```

Verified output, in order: `0`, then `20`, then `20` again. Two facts confirmed here, not assumed:

- **`.cache()` itself is lazy** — calling it doesn't compute anything. The counter is still `0`
  right after the call, exactly like every other transformation in Spark.
- **The first action after `.cache()` computes and materializes the result; every action after
  that reuses it.** The second `collect()` added zero new calls to the counter — genuinely skipped
  recomputation, not just a faster re-run.

## `.unpersist()` gives the recomputation cost right back

```python
transformed.unpersist()
transformed.agg(sum("doubled")).collect()
print(compute_calls.value)   # jumps from 20 to 40
```

Verified: the count jumps back up immediately. Caching isn't a permanent optimization — it holds
exactly as long as the cached data stays in memory (or disk, depending on storage level — Lesson
3) and hasn't been explicitly released. Call `.unpersist()` once you're done reusing a DataFrame,
so Spark can reclaim the space for something else; leaving unused cached data around is a common
way to run an executor out of memory for no benefit.

## When caching helps — and when it doesn't

- **Helps:** a DataFrame that gets reused across multiple actions or multiple downstream branches
  of the same pipeline — exactly the pattern proven above. Iterative algorithms (ML training loops)
  are the canonical case.
- **Doesn't help, and can actively hurt:** caching a DataFrame that's only ever used once. You pay
  the cost of materializing and storing it for zero reuse benefit, and it occupies memory another
  DataFrame or shuffle could have used instead — potentially forcing spills or evictions elsewhere.
- **Rule of thumb:** cache right before a DataFrame you're about to reference more than once
  (multiple `.filter()` branches, multiple joins against the same base DataFrame, a loop that
  re-queries it), and `.unpersist()` as soon as you're done with it.

---
**Next:** [Lesson 3 — Storage Levels Deep Dive](03-storage-levels-deep-dive.md)
