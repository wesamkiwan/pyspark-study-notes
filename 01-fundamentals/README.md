# Module 01 — Fundamentals & Architecture

Before touching another line of DataFrame code, you need a correct mental model of what
actually happens when you run PySpark. Skip this and you'll be able to copy-paste code that
works — but you won't be able to explain *why* a job is slow, why it fails on a cluster but
not locally, or answer basic interview questions about how Spark distributes work. This module
fixes that.

## Learning objectives

By the end of this module you can:
- Explain, from memory, what a driver, executor, and cluster manager each do
- Explain why Spark computes nothing until you call an *action*, and why that matters
- Choose correctly between the DataFrame API and raw RDDs (spoiler: almost always DataFrame)
- Read a `.explain()` plan well enough to tell what Spark is actually going to do
- Write and run a correct, idiomatic first PySpark program

## Lessons

1. [What Spark Is and Why It Exists](01-what-is-spark-and-why.md)
2. [Architecture Deep Dive](02-architecture-deep-dive.md)
3. [RDD vs DataFrame vs Dataset](03-rdd-vs-dataframe-vs-dataset.md)
4. [SparkSession and Your First Program](04-sparksession-and-first-program.md)
5. [Lazy Evaluation and Execution Plans](05-lazy-evaluation-and-execution-plans.md)

Then: [`exercises/`](exercises/) (attempt before opening [`solutions/`](solutions/)), and
[`quiz.md`](quiz.md) to check retention.

All exercises use the shared sample data in [`/data`](../data/) (`employees.csv`, `orders.csv`)
so you're working with the same dataset you'll reuse in later modules.
