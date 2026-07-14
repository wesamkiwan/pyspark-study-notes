# Module 06 Quiz

Answer each yourself before expanding the answer.

---

**1. `employees.rdd.getNumPartitions()` returns 1 right after reading `employees.csv`. Where does
that number actually come from?**

<details>
<summary>Answer</summary>

Spark's file-based readers split input by size, governed by `spark.sql.files.maxPartitionBytes`
(128 MiB, verified default). `employees.csv` is a single file far smaller than that threshold, so
it becomes exactly one input partition. A file larger than the threshold would be split into
multiple input partitions automatically; many small files would each become their own tiny
partition.
</details>

---

**2. `spark.sparkContext.defaultParallelism` returns 16 on this course's machine. Does that mean
Spark can only ever have 16 partitions?**

<details>
<summary>Answer</summary>

No — it's the number of tasks that can run *simultaneously* (tied to available cores in `local[*]`
mode, or total executor cores on a cluster), not a cap on partition count. You can have far more
partitions than cores; Spark just processes them in waves, `defaultParallelism`-many at a time.
</details>

---

**3. `employees.repartition(4).groupBy("department", spark_partition_id()).count()` shows
`Engineering` spread across all 4 partitions. `employees.repartition(4, "department")` puts every
`Engineering` row in exactly one partition. What's the mechanical difference between the two?**

<details>
<summary>Answer</summary>

Plain `repartition(n)` distributes rows round-robin with no relationship to any column's values.
`repartition(n, col)` hash-partitions instead — Spark computes `hash(col) % n` per row, so every
row sharing the same value of `col` deterministically lands in the same output partition. Verified:
`Engineering` spanned all 4 partitions under the first, exactly 1 under the second.
</details>

---

**4. Why does spreading a genuinely small dataset across many more partitions than necessary
actually cost you something, beyond "it's a bit wasteful"?**

<details>
<summary>Answer</summary>

Every partition becomes at least one task, and every task carries fixed scheduling overhead
(serializing it, dispatching it to an executor, tracking status, collecting the result) regardless
of how little data is inside it. Thousands of near-empty partitions means paying that fixed
overhead thousands of times for almost no actual work — often net *slower* than fewer, larger
partitions, despite "more partitions" sounding like more parallelism.
</details>

---

**5. `spark.sql.shuffle.partitions` defaults to 200. `employees.groupBy("department").count()` on
15 rows verifiably produces 200 partitions with AQE off, even though there are only 4 possible
result rows. What's actually wrong with the default here, and is it wrong in the opposite way for
some workloads too?**

<details>
<summary>Answer</summary>

200 is a fixed, one-size-fits-all number completely unrelated to your data's actual size — for
small/medium data (like this course's, or a lot of real ad-hoc jobs) it produces hundreds of
nearly-empty partitions, each paying full per-task overhead for near-zero work. For genuinely huge
datasets (hundreds of GB+), the same fixed 200 can be too *few* — each partition becomes enormous
and you under-use a cluster with far more than 200 cores available.
</details>

---

**6. Same query, same `spark.sql.shuffle.partitions=200`, produces 200 partitions with
`spark.sql.adaptive.enabled=false` and 1 partition with it `=true`. What did AQE actually do, and
what's the exact plan-text signal that shows it happened?**

<details>
<summary>Answer</summary>

AQE ran the shuffle (which still physically writes 200 partitions' worth of output — verified, the
`Exchange hashpartitioning(department, 200)` step appears in both the Initial and Final plan), then
looked at the *actual* size of each of those 200 partitions after the fact and merged the tiny ones
back down to a sensible number for everything downstream. The signal is `AQEShuffleRead coalesced`
appearing in the `== Final Plan ==` section of `.explain()`, alongside `isFinalPlan=true` instead of
`=false`.
</details>

---

**7. Does AQE's partition coalescing mean you no longer need to think about
`spark.sql.shuffle.partitions` at all?**

<details>
<summary>Answer</summary>

No. The shuffle *write* itself still happens at whatever `shuffle.partitions` says — writing 200
tiny partitions and coalescing them on read afterward is still more I/O overhead than writing a
sensible number in the first place. AQE reduces the downstream cost of a badly-chosen value; it
doesn't eliminate the upstream cost of choosing badly. It also only merges small partitions
together — it doesn't fix an oversized one (that's the separate skew-join AQE feature).
</details>

---

**8. A synthetic dataset has 90,000 of its 100,000 rows sharing one key, `"HOT"`.
`df.repartition(8, "key").rdd.glom().map(len).collect()` shows one partition with 91,282 rows and
the rest under 1,500. What does `.glom()` actually do here, and why is this a more reliable way to
detect skew than watching wall-clock time?**

<details>
<summary>Answer</summary>

`.rdd.glom()` collects each partition's rows into its own list, so `.map(len)` gives you the exact
row count per partition directly — a hard number, not an inference. It's more reliable than timing
because local-mode wall-clock time is noisy and doesn't reproducibly demonstrate the *cause*;
partition-size inspection shows the actual data imbalance driving any slowness directly, verified
here at 91,282 vs ~1,000-1,400 across the other 7 partitions.
</details>

---

**9. Salting splits `"HOT"` into `"HOT_0"` through `"HOT_7"` and repartitions on the salted key.
Why does a single `groupBy("salted_key")` on the salted data give the WRONG total for `"HOT"`, and
what's the fix?**

<details>
<summary>Answer</summary>

Splitting one logical key into 8 salted sub-keys means `"HOT"`'s rows are now spread across 8
different groups — grouping directly on `salted_key` would produce 8 separate partial totals
instead of the one correct total for `"HOT"`. The fix is a two-stage aggregation: aggregate by
`(key, salt)` first (producing correct partial totals per salt bucket), then aggregate those
partial results by `key` alone to combine them back into the one correct logical total. Verified:
this two-stage total for `"HOT"` (90,000) exactly matches the plain, unsalted aggregation.
</details>

---

**10. In testing, salting with `NUM_SALTS=8` but repartitioning into only 8 output partitions left
meaningful imbalance (max partition 23,917); repartitioning into 16 output partitions with the same
salting brought the max down to 12,074, close to the theoretical ~11,250-per-bucket minimum. Why
does the output partition count matter here, separately from the salt count?**

<details>
<summary>Answer</summary>

With only 8 output partitions and roughly 1,000 total distinct keys (8 salted-hot-key buckets plus
~999 other genuinely distinct keys) all hashing into just 8 buckets, hash collisions between the
salted-hot pieces and unrelated cold keys still produce uneven partitions. Increasing the output
partition count (to more than the number of salt buckets) reduces those collisions, letting each
salted-hot sub-key land in its own partition close to its true ~11,250-row share. Salting alone
isn't sufficient — it needs to be paired with enough output partitions to actually realize the
spread.
</details>

---

Check the boxes in [`PROGRESS.md`](../PROGRESS.md) and move on to Module 07 when it's built.
