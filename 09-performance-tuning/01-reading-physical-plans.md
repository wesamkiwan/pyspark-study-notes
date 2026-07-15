# Lesson 1 — Reading Physical Plans Like a Pro

Every module since 05 has called `.explain()` to prove a specific point. This lesson treats
`.explain()` itself as the subject: its three useful modes, and a genuinely surprising verified
behavior — the plan can look different depending on *when* you call it, because Adaptive Query
Execution (AQE, covered in Module 06) rewrites the plan after seeing real runtime statistics.

## The three modes worth knowing

```python
orders = spark.read.csv("data/orders.csv", header=True, inferSchema=True)
filtered = orders.filter(col("amount") > 200).groupBy("category").sum("amount")

filtered.explain()             # "simple" -- the default, one indented tree
filtered.explain("formatted")  # numbered nodes + a detail section per node
filtered.explain("cost")       # adds Statistics(sizeInBytes=...) to the logical plan
```

- **`simple`** (the default, used throughout this course so far): one tree, physical operators
  only. Good for a quick check of Exchange/Filter/Aggregate shape.
- **`formatted`**: numbers each node (`(1)`, `(2)`, ...) and then gives a detail block per node
  below the tree — verified genuinely easier to read once a plan has more than 4-5 operators,
  since the tree itself stays compact and the detail (input columns, conditions, arguments) is
  looked up by number rather than crammed inline.
- **`cost`**: shows the *logical* plan (pre-physical-planning) annotated with
  `Statistics(sizeInBytes=...)` at every node — this is exactly the number the optimizer uses to
  decide whether a join qualifies for a broadcast (Module 05's `autoBroadcastJoinThreshold`).
  Verified on `orders.csv`: `Statistics(sizeInBytes=804.0 B)` for the base relation — reading this
  is how you'd confirm *why* Spark did or didn't broadcast a particular join, instead of guessing.

## The trap: `.explain()` before an action can show a stale, non-final plan

With AQE enabled (the default since Spark 3.2), Spark plans a query in two phases: an **initial
plan**, built from static estimates, and a **final plan**, which AQE rewrites after the first
shuffle's real output sizes are known. Calling `.explain()` on a DataFrame **before running any
action against it** can only show you the initial plan — verified:

```python
filtered = orders.filter(col("amount") > 200).groupBy("category").sum("amount")
filtered.explain()   # BEFORE any action
```

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[category#20], functions=[sum(amount#21)])
   +- Exchange hashpartitioning(category#20, 8), ENSURE_REQUIREMENTS, [plan_id=36]
      +- HashAggregate(keys=[category#20], functions=[partial_sum(amount#21)])
         +- Filter (isnotnull(amount#21) AND (amount#21 > 200.0))
            +- FileScan csv [category#20,amount#21] ...
```

Now run an action on the **same DataFrame object**, then call `.explain()` again:

```python
filtered.collect()
filtered.explain()   # AFTER an action
```

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   *(2) HashAggregate(keys=[category#20], functions=[sum(amount#21)])
   +- AQEShuffleRead coalesced
      +- ShuffleQueryStage 0
         +- Exchange hashpartitioning(category#20, 8), ENSURE_REQUIREMENTS, [plan_id=49]
            +- *(1) HashAggregate(keys=[category#20], functions=[partial_sum(amount#21)])
               +- *(1) Filter (isnotnull(amount#21) AND (amount#21 > 200.0))
                  +- FileScan csv [category#20,amount#21] ...
+- == Initial Plan ==
   HashAggregate(keys=[category#20], functions=[sum(amount#21)])
   +- Exchange hashpartitioning(category#20, 8), ENSURE_REQUIREMENTS, [plan_id=36]
      +- HashAggregate(keys=[category#20], functions=[partial_sum(amount#21)])
         +- Filter (isnotnull(amount#21) AND (amount#21 > 200.0))
            +- FileScan csv [category#20,amount#21] ...
```

Verified, and genuinely easy to be misled by: **`isFinalPlan` flips from `false` to `true`, codegen
stars (`*(1)`, `*(2)`) appear for the first time, and an `AQEShuffleRead coalesced` node shows up** —
none of that was visible in the pre-action plan. The two plans describe the same query, but the
first one is Spark's *guess* before it has run anything, and the second is what actually executed,
shown side-by-side as `== Final Plan ==` and `== Initial Plan ==`.

## The rule

**If you want to know what a query actually did, call `.explain()` (or check the Spark UI's SQL
tab, Lesson 5) after triggering an action, not before.** A pre-action `.explain()` is still useful
for sanity-checking a query's shape early, but don't use it to answer questions AQE can only answer
at runtime — "did this get coalesced?", "is this reading from a broadcast?", "how many partitions
did the shuffle actually produce?" All of those require the *final* plan.

---
**Next:** [Lesson 2 — Caching and Persistence, Verified](02-caching-and-persistence.md)
