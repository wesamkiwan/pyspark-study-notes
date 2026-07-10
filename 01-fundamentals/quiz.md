# Module 01 Quiz

Answer each question yourself (out loud or in writing) before expanding the answer. That
struggle is what makes it stick — reading the answer first teaches you nothing.

---

**1. What's the difference between the driver and an executor?**

<details>
<summary>Answer</summary>

The driver runs your main program, builds and optimizes the execution plan, and schedules
work — it coordinates but doesn't do the heavy data processing itself. Executors are JVM
processes on worker nodes that actually run tasks against partitions of data and hold cached
data in memory.
</details>

---

**2. Why does `df.filter(...)` not print anything or take any noticeable time, even on a huge
file?**

<details>
<summary>Answer</summary>

Because `.filter()` is a transformation, and transformations are lazy — Spark just records it
as a step in a logical plan without touching any data. Nothing executes until an action (like
`.show()` or `.count()`) is called.
</details>

---

**3. What creates a new Stage boundary in a Spark job, and why?**

<details>
<summary>Answer</summary>

A shuffle — redistributing data across partitions/machines by key, required by wide
transformations like `groupBy`, `join`, `orderBy`, and `repartition`. Data must be written out
and read back in on the other side, which is a natural boundary between one stage of work and
the next.
</details>

---

**4. Why are DataFrames generally faster than the equivalent logic written with RDDs?**

<details>
<summary>Answer</summary>

DataFrame operations are declarative and structured (they have a schema), which lets Spark's
Catalyst optimizer inspect and rewrite the whole plan — pushing filters down, pruning unused
columns, picking better join strategies — before anything runs. RDD operations are opaque
function calls Spark can't see inside or optimize.
</details>

---

**5. You need every row of a 2-billion-row DataFrame pulled into a Python list in your driver
program. What's wrong with this request, and what would you push back with?**

<details>
<summary>Answer</summary>

`.collect()` would pull all 2 billion rows into the single driver process's memory — this will
almost certainly crash it (OutOfMemory), since the driver is one machine, not the cluster.
Push back on the *need* for all rows on the driver at all — usually the real requirement is
better served by writing the result to storage (`.write.*()`), aggregating it down first, or
processing it in a distributed way rather than materializing it in Python.
</details>

---

**6. In local mode (`local[*]`), is the driver/executor separation still real, or is it just one
process?**

<details>
<summary>Answer</summary>

It's conceptually still there but collapsed into a single JVM process, which spawns
executor-*threads* rather than separate processes on separate machines. This makes local mode
great for learning/testing logic, but it hides real distributed-systems behavior (network
shuffle cost, executor failures, uneven data distribution across machines) that only shows up
on an actual cluster.
</details>

---

**7. What does `PushedFilters` in a `.explain()` physical plan tell you, and why does it matter?**

<details>
<summary>Answer</summary>

It shows that Catalyst moved your `.filter()` condition as close to the data source as
possible (predicate pushdown) — e.g. into the file scan itself — so fewer rows are read and
processed in the first place. It matters because it's evidence the optimizer is doing real
work on your behalf, and because for some data sources (e.g. Parquet, JDBC) pushdown can skip
reading whole files/blocks entirely, which is a major performance factor.
</details>

---

**8. Name one legitimate reason to still use an RDD instead of a DataFrame today.**

<details>
<summary>Answer</summary>

Genuinely unstructured data that doesn't fit a row/column schema, needing very fine-grained
custom partitioning logic, or working with legacy code/older MLlib APIs that still expose RDDs
directly. (Not a legitimate reason: "it feels more like normal Python" — that's exactly the
instinct to resist, since it throws away Catalyst optimization.)
</details>

---

Scored yourself honestly? Check the corresponding box in [`PROGRESS.md`](../PROGRESS.md) and
move on to Module 02 when it's built.
