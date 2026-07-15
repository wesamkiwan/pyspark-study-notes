# PySpark: Zero to Hero 🚀

A complete, hands-on PySpark curriculum for going from "comfortable with Python/SQL" to
"can walk into a data engineering job and handle production Spark workloads."

This is **not** a copy of the official docs. Every module has:
- 📖 Plain-English explanations of *why*, not just *how*
- 🖼️ Diagrams (Mermaid, rendered natively by GitHub) for every architectural concept
- 💻 Runnable code examples you execute yourself, not just read
- 🏋️ Exercises with hidden solutions (click to reveal) so you're forced to think first
- ✅ Best-practice call-outs and "here's how this bites you in production" warnings
- 🧪 A working local PySpark environment, verified end-to-end

> **A note on screenshots:** you asked for screenshots from the internet. I've deliberately used
> Mermaid diagrams and ASCII art instead — they render directly in GitHub and in your editor,
> never rot into dead links, and I can guarantee their accuracy. Where a real UI screenshot would
> help (e.g. Spark UI, Databricks), I'll tell you exactly what to click so you generate your own —
> that's also better practice, since you'll recognize the real thing.

---

## How to use this repo

1. Work through modules **in order** — each builds on the last.
2. Do the exercises before peeking at the solutions. Struggling productively is the point.
3. Check off items in [`PROGRESS.md`](PROGRESS.md) as you go — that file is your tracker.
4. Every module's code assumes the local venv from [`00-setup`](00-setup/README.md) is active.
5. Ask questions as you go — tell me which module/lesson and what's unclear, and we'll dig in
   together before moving on.

## Environment

You're learning on a **local install** (pip + venv), which is the fastest inner loop for learning.
[`00-setup`](00-setup/README.md) also documents **Docker** and **Databricks Community Edition**
so you can recognize and use those setups too — production teams use all three depending on context.

## Course Roadmap

| # | Module | You'll be able to... |
|---|--------|----------------------|
| 00 | [Setup & Environment](00-setup/) | Run PySpark locally, in Docker, or on Databricks |
| 01 | [Fundamentals & Architecture](01-fundamentals/) | Explain driver/executor model, DAGs, lazy evaluation, RDD vs DataFrame |
| 02 | [Reading, Writing & Schemas](02-io-and-schemas/) | Handle CSV/JSON/Parquet/ORC/Avro, enforce schemas, avoid schema inference traps, handle malformed data and schema drift deliberately |
| 03 | [Core DataFrame Transformations](03-core-transformations/) | select/filter/withColumn/groupBy/agg fluently, know when to use SQL instead |
| 04 | [Spark SQL](04-spark-sql/) | Mix SQL and DataFrame API, use the Catalog, write maintainable SQL pipelines |
| 05 | [Joins Deep Dive](05-joins-deep-dive/) | Pick the right join strategy, diagnose and fix data skew, broadcast correctly |
| 06 | [Partitioning & Shuffling](06-partitioning-and-shuffling/) | Control data layout, avoid shuffle disasters, use AQE |
| 07 | [Window Functions](07-window-functions/) | Solve ranking/running-total/gap-analysis problems the "Spark way" |
| 08 | [UDFs & Pandas UDFs](08-udfs-and-pandas-udfs/) | Know when a UDF is the wrong answer, write vectorized Pandas UDFs when it's right |
| 09 | [Performance Tuning](09-performance-tuning/) | Read a query plan, use `.explain()`, caching/persistence, Spark configs that matter |
| 10 | [Structured Streaming](10-structured-streaming/) | Build a streaming pipeline with watermarks, triggers, checkpointing |
| 11 | [Delta Lake / Lakehouse](11-delta-lake/) | ACID tables, time travel, MERGE, OPTIMIZE/VACUUM, medallion architecture |
| 12 | [Data Engineering Patterns](12-data-engineering-patterns/) | Idempotent pipelines, SCD types, data quality gates, error handling at scale |
| 13 | [Testing PySpark Code](13-testing-pyspark-code/) | Unit-test transformations with pytest + chispa, structure testable pipelines |
| 14 | [Production & Deployment](14-production-and-deployment/) | spark-submit, cluster sizing, Databricks Jobs/EMR, orchestration with Airflow |
| 15 | [Capstone Projects](15-capstone-projects/) | Build 3 progressively harder real-world pipelines end-to-end |
| 16 | Interview Prep & Cheat Sheets | Answer the questions that actually get asked, fast reference sheets |

Modules 00–15 are built now. We'll build the rest together, module by module — tell me when
you're ready to continue and we'll keep going (or jump ahead if you already know a topic).

## Repo Structure

```
pyspark-study-notes/
├── README.md                 <- you are here
├── PROGRESS.md                <- your tracker, check boxes as you complete lessons
├── requirements.txt            <- pinned deps for the local venv
├── data/                       <- shared sample datasets used across modules
├── 00-setup/                   <- environment setup (local/Docker/Databricks)
├── 01-fundamentals/            <- architecture, RDD vs DataFrame, SparkSession
│   ├── 01-...md .. 05-...md    <- lessons, read in order
│   ├── exercises/               <- do these yourself first
│   └── solutions/                <- check after you've tried
├── 02.../ ... 09.../             <- same pattern
└── 10.../ ...                    <- future modules, same pattern
```

## Quick Start

```powershell
# from repo root, PowerShell
. .\00-setup\set_env.ps1              # sets JAVA_HOME / HADOOP_HOME for this session
C:\venvs\pyspark-course\Scripts\Activate.ps1   # or wherever you created your venv — see 00-setup/README.md
python 00-setup\verify_install.py
```

If that prints a Spark version and a small DataFrame, you're ready for Module 01. If anything
fails, [`00-setup/README.md`](00-setup/README.md) documents every gotcha we actually hit
(and fixed) setting this up on Windows — read the "Windows gotchas" table before searching
elsewhere.
