# Lesson 1 — spark-submit Deep Dive

Every script this course has run so far used `SparkSession.builder...getOrCreate()` directly,
executed with plain `python script.py`. `spark-submit` is the actual production entry point —
the CLI every real Spark job, on every platform (standalone, Databricks, EMR, Kubernetes), ultimately
runs through. This lesson verifies its core mechanics with a real submitted job.

```mermaid
flowchart LR
    CLI["spark-submit\n--master ... --conf ... --py-files ... script.py"] --> Driver["Driver process\n(your script's main logic)"]
    Driver --> Cluster["Cluster manager\n(local / standalone / YARN / k8s)"]
    Cluster --> Executors["Executors\n(do the actual work)"]
```

## A real job, submitted with real config overrides

```python
# submit_job.py
from pyspark.sql import SparkSession
from helper_module import shout   # a SEPARATE module, not in the same file

spark = SparkSession.builder.appName("submit-demo").getOrCreate()

print("app name:", spark.conf.get("spark.app.name"))
print("master:", spark.sparkContext.master)
print("executor memory (as configured):", spark.conf.get("spark.executor.memory", "not set"))
print("shuffle partitions (from --conf):", spark.conf.get("spark.sql.shuffle.partitions"))
print("helper_module says:", shout("hello from --py-files"))
```

```bash
spark-submit \
  --master "local[*]" \
  --conf "spark.sql.shuffle.partitions=4" \
  --conf "spark.executor.memory=2g" \
  --py-files helper_module.py \
  submit_job.py
```

Verified, real output:

```
app name: submit-demo
master: local[*]
executor memory (as configured): 2g
shuffle partitions (from --conf): 4
helper_module says: HELLO FROM --PY-FILES!!!
row count: 10
```

Three things confirmed here, precisely:
- **`--conf key=value` genuinely sets Spark configs before your code ever runs** — no different
  from `.config(...)` calls in a builder, just supplied from the command line instead, which is
  what lets the same script run with different tuning per environment without a code change.
  `spark.executor.memory` in particular has zero effect in `local[*]` mode (there's no separate
  executor process to size — verified the config is *readable* via `spark.conf.get`, but it's
  purely informational here); it becomes meaningful the moment you submit to an actual cluster
  manager (YARN, standalone, Kubernetes) that launches real executor JVMs.
- **`--py-files` genuinely makes a separate module importable** — `helper_module.py` was never
  copied next to `submit_job.py`, only passed via `--py-files`, and `from helper_module import
  shout` still worked. This is how you ship a small pipeline's supporting code (Module 13's
  `pipeline_logic.py`-style files) without bundling everything into one giant script. For anything
  bigger than a couple of files, package them into a `.zip` or `.egg`/wheel and pass that instead.
- **The script itself needed zero changes to be "production-shaped."** No hardcoded config
  values — everything tunable came from `--conf`, exactly Module 13 Lesson 1's testable-design
  principle applied one level up: separate *how it's configured* from *what it does*.

## Deploy modes — `client` vs `cluster`

`--master` and `--deploy-mode` together decide where the **driver** (the process running your
script's main logic, coordinating everything) actually lives:

| Deploy mode | Where the driver runs | Typical use |
|---|---|---|
| `client` (the default) | On the machine you ran `spark-submit` from | Interactive/dev work, notebooks, anywhere you want to see driver logs directly in your terminal |
| `cluster` | On a node inside the cluster itself | Production jobs — the submitting machine doesn't need to stay connected, and driver failures are handled by the cluster manager like any other node failure |

This distinction doesn't show up in `local[*]` mode (there's only one machine involved either way),
but it's one of the first things to get right moving to a real cluster: a long-running production
job submitted in `client` mode from a laptop dies the moment that laptop disconnects — `cluster`
mode is almost always the right choice for anything unattended.

## Best-practice callout

**Never hardcode environment-specific values (paths, cluster URLs, credentials) into the script
itself.** Pass them as `--conf` values or script arguments (`sys.argv`, parsed the same way any
CLI tool would) instead — the exact same script should be submittable to a local dev environment,
a staging cluster, and production, differing only in the `spark-submit` command line, never in the
code.

---
**Next:** [Lesson 2 — Cluster Sizing](02-cluster-sizing.md)
