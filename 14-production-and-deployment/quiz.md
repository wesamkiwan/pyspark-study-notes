# Module 14 Quiz

Answer each yourself before expanding the answer.

---

**1. `spark.conf.get("spark.executor.memory")` returned a real, readable value when a job was
submitted with `--conf "spark.executor.memory=2g"` under `--master "local[*]"`. Does this config
actually change anything about how the job runs, in local mode specifically?**

<details>
<summary>Answer</summary>

No — verified, it's purely informational in `local[*]` mode. Local mode has no separate executor
JVMs to size at all; everything runs as threads inside one process. `spark.executor.memory` only
becomes operationally meaningful the moment a job is submitted to a real cluster manager (YARN,
standalone, Kubernetes) that actually launches distinct executor processes sized by that config.
</details>

---

**2. `--py-files helper_module.py` let `submit_job.py` successfully `from helper_module import
shout`, verified, even though `helper_module.py` was never copied next to `submit_job.py` on disk.
What's this mechanism actually for, and what should you reach for once you have more than a
couple of supporting files?**

<details>
<summary>Answer</summary>

`--py-files` ships additional Python modules/packages to wherever the driver (and executors, on a
real cluster) run, making them importable without bundling everything into one giant script file —
this is how a pipeline's supporting code (Module 13-style `pipeline_logic.py` files) gets deployed
alongside the entry-point script. For more than a couple of files, package them into a `.zip` or a
wheel/`.egg` and pass that instead of listing individual `.py` files.
</details>

---

**3. `--deploy-mode client` (the default) versus `--deploy-mode cluster` — what specifically
differs, and why does this distinction matter far more for a production job than for interactive
development?**

<details>
<summary>Answer</summary>

They differ in where the **driver** process (the one running your script's main logic) lives:
`client` mode runs it on the machine you submitted from; `cluster` mode runs it on a node inside the
cluster itself. This matters for production because a `client`-mode job submitted from a laptop or
a temporary machine dies the instant that machine disconnects — `cluster` mode is what lets an
unattended job survive independent of whatever submitted it.
</details>

---

**4. The standard cluster-sizing methodology recommends roughly 5 cores per executor as a "sweet
spot," rather than either 1-2 cores or 15+ cores per executor. What specific cost grows with MORE
cores per executor, and what's lost by using FEWER?**

<details>
<summary>Answer</summary>

More cores per executor means a bigger shared heap and more concurrent threads per JVM — GC pauses
get worse as heap size grows, and past roughly 5 concurrent threads, throughput to a shared
HDFS/network client connection degrades. Fewer cores per executor (many tiny executors) wastes
overhead on redundant per-JVM bookkeeping (each JVM has its own fixed costs) without adding real
parallelism benefit — 5 is the empirically-observed balance point, not a hard technical limit.
</details>

---

**5. In the worked cluster-sizing example (8 nodes, 16 cores, 64GB RAM each, 5 cores/executor
target), each executor ended up with 21GB requested memory but only ~18.9GB of actual usable heap.
Where did the other ~2.1GB go, and why is it necessary?**

<details>
<summary>Answer</summary>

To `spark.executor.memoryOverhead` — Spark reserves `max(384MB, 0.1 × executorMemory)` per executor
for JVM overhead: thread stacks, native memory, and non-JVM process memory that isn't part of the
JVM heap itself but is still real memory the executor process needs. Skipping this reservation
risks the executor process (not just the JVM heap) running out of memory and being killed by the
cluster manager, even though `spark.executor.memory` itself looks fine.
</details>

---

**6. Databricks Jobs and AWS EMR both handle cluster provisioning and Spark installation for you.
What real responsibilities from this course (idempotency, testing, cluster sizing) do you STILL
own even on a fully managed platform?**

<details>
<summary>Answer</summary>

All of them. Managed provisioning doesn't test your transformation logic (Module 13), doesn't make
your job idempotent (Module 12) — a managed scheduler retrying a non-idempotent job just automates
the double-counting bug instead of preventing it — and still requires you to choose instance
types/counts or an autoscaling range (Lesson 2's reasoning, just behind a friendlier UI). Managed
platforms handle *infrastructure*, not your pipeline's correctness.
</details>

---

**7. An Airflow DAG's `default_args` include `retries=3`, `retry_delay=timedelta(minutes=5)`, and
`retry_exponential_backoff=True`. A standalone simulation of this pattern was verified to produce
gaps of `[1.0, 2.0]` seconds between three attempts. What specific problem does the "exponential"
part of exponential backoff solve that a FIXED retry delay wouldn't?**

<details>
<summary>Answer</summary>

It avoids a "thundering herd" effect — if the underlying cause is real load or contention on the
cluster, retrying at a fixed short interval repeatedly hits the same overloaded resource and can
make the problem worse. A growing delay between attempts gives the underlying issue actual room to
resolve before the next attempt, rather than piling more load on top of an already-struggling
system.
</details>

---

**8. Why does Module 12's idempotency lesson matter specifically as a PREREQUISITE for safely using
Airflow's `retries` argument, rather than the two being independent concerns?**

<details>
<summary>Answer</summary>

A retry only helps if re-running the job produces the correct result whether it ran once or
multiple times. If the job isn't idempotent (a naive `append`-based load, verified in Module 12 to
double row count on a second run), an automatic retry after a transient failure — or worse, a retry
after a job that actually *partially succeeded* — silently corrupts data instead of safely
recovering from the failure. Retries are a safety net that only works if the job underneath it
has already earned the right to be retried safely.
</details>

---

**9. `catchup=False` is set on the example DAG's `default_args`. What would `catchup=True` (or
omitting it, since it defaults to `True`) actually cause to happen the moment a new DAG with a
`start_date` in the past is turned on?**

<details>
<summary>Answer</summary>

Airflow would attempt to run every scheduled interval between `start_date` and the current time
that hasn't run yet, all at once — potentially dozens or hundreds of backfill runs firing
immediately. `catchup=False` is the right default for a newly-deployed pipeline that should only
run going forward; `catchup=True` is the right choice specifically when a deliberate historical
backfill is actually wanted.
</details>

---

**10. The production-readiness checklist in Lesson 5 is described as "a review, not a single gate"
— unlike Module 12's quality gate, which is code that automatically raises before a bad write. Why
does that distinction matter for how a team should actually use this checklist?**

<details>
<summary>Answer</summary>

A quality gate is enforced automatically, every run, with no judgment call involved — either the
data passes or it doesn't. The readiness checklist is a set of deliberate decisions a team makes
once (or revisits after an incident), and different pipelines legitimately warrant different levels
of rigor — a low-stakes internal reporting job doesn't need the same checkpointing/alerting rigor
as a customer-facing pipeline. Treating it as a review means each item gets a conscious yes/no
decision, rather than either blindly applying every item everywhere or skipping the review
entirely because "it's not enforced by code."
</details>

---

Check the boxes in [`PROGRESS.md`](../PROGRESS.md) and move on to Module 15 when it's built.
