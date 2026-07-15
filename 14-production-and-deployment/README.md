# Module 14 — Production & Deployment

Every prior module ran on `local[*]` inside a single Python process. This module is about
everything that changes once a job needs to run unattended, on a schedule, against a real cluster,
in a platform someone else operates. It's structured a bit differently from earlier modules: parts
of it (`spark-submit` itself, its `--conf`/`--py-files` mechanics) are verified exactly the way this
whole course has been — actually run, real output captured. Other parts (multi-node cluster sizing,
Databricks Jobs, EMR, Airflow) describe real, standard methodology and patterns, but honestly
labeled as **not executed against a live multi-node cluster or managed platform in this course's
environment** — the same honest line Module 00 drew around Docker and Databricks Community Edition.

## Learning objectives

By the end of this module you can:
- Use `spark-submit` correctly — deploy modes, `--conf` overrides, `--py-files` for extra modules —
  verified end to end with a real submitted job reading its own passed-in config
- Reason about cluster sizing (executors, cores per executor, memory per executor, off-heap
  overhead) using the standard methodology, and explain why "more executors" and "more cores per
  executor" trade off against each other rather than both being free wins
- Compare running Spark as a standalone cluster, on Databricks Jobs, and on EMR — what each
  platform actually manages for you versus what you still own
- Read a realistic Airflow DAG that orchestrates a `spark-submit` job with retries and backoff, and
  explain what problem each DAG argument (retries, `retry_delay`, `depends_on_past`) solves
- Assemble a production-readiness checklist from tools this course already covered — the Spark UI
  (Module 09), checkpointing (Module 10), idempotency (Module 12), and automated tests (Module 13)
  — as one coherent "is this job actually ready to run unattended" review

## Lessons

1. [spark-submit Deep Dive](01-spark-submit-deep-dive.md)
2. [Cluster Sizing](02-cluster-sizing.md)
3. [Databricks Jobs, EMR, and Standalone Clusters](03-databricks-emr-and-standalone.md)
4. [Orchestration with Airflow](04-orchestration-with-airflow.md)
5. [A Production Readiness Checklist](05-production-readiness-checklist.md)

Then: [`exercises/`](exercises/) before [`solutions/`](solutions/), then [`quiz.md`](quiz.md).

Lesson 1's `spark-submit` example was actually run against the local venv, with real captured
output. Lessons 2-4 describe standard, widely-used methodology and real code patterns, but — same
as Module 00's treatment of Docker/Databricks — this course's environment doesn't include a live
multi-node cluster, a Databricks workspace, an EMR account, or an Airflow install, so those
sections are conceptual guidance rather than verified execution. Where that's the case, it's called
out explicitly rather than dressed up as something it isn't.

---
**Next:** [Module 15 — Capstone Projects](../15-capstone-projects/)
