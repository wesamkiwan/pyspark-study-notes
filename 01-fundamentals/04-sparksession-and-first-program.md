# Lesson 4 — SparkSession and Your First Program

## SparkSession: your one entry point

Historically Spark had separate `SparkContext` (core engine), `SQLContext` (SQL/DataFrames), and
`HiveContext` (Hive integration) objects you had to juggle. Since Spark 2.0, **`SparkSession`**
unifies all of them into a single entry point. You'll still see `SparkContext` referenced (e.g.
`spark.sparkContext.setLogLevel(...)`) because `SparkSession` wraps one internally — but you
create exactly one `SparkSession` per application, and everything else hangs off of it.

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("my-first-pyspark-app")   # shows up in the Spark UI / logs
    .master("local[*]")                 # "*" = use all available CPU cores as workers
    .config("spark.sql.shuffle.partitions", "8")  # see note below
    .getOrCreate()                       # reuses an existing session if one exists
)
```

- **`.master(...)`**: `local[*]` for local dev (this course). On a real cluster this would be
  `yarn`, `k8s://...`, or omitted entirely (set by `spark-submit --master` / the platform,
  e.g. Databricks sets this for you — never hardcode it in production code).
- **`.config("spark.sql.shuffle.partitions", "8")`**: the default is **200**, which is tuned for
  large clusters, not your laptop with a few CPU cores. Leaving it at 200 for small local data
  means Spark creates 200 tiny partitions and wastes time on scheduling overhead for each one.
  We're setting it low here for a snappier learning experience — full tuning guidance is in
  Module 09.
- **`.getOrCreate()`**: idempotent — if a session already exists (e.g. in a notebook, or
  Databricks, where `spark` is already provided), this returns the existing one instead of
  creating a conflicting second one.

## Creating your first DataFrame three ways

```python
# 1. From in-memory Python data — useful for tests and small examples
df = spark.createDataFrame(
    [("Alice", 29), ("Bob", 31)],
    schema=["name", "age"],
)

# 2. From a file — what you'll do almost all the time in real pipelines
df = spark.read.csv("data/employees.csv", header=True, inferSchema=True)

# 3. From SQL directly against a registered view (more in Module 04 — Spark SQL)
df.createOrReplaceTempView("employees")
df2 = spark.sql("SELECT name, department FROM employees WHERE department = 'Engineering'")
```

> **Best practice flag, revisited in Module 02:** `inferSchema=True` is convenient for learning
> but risky in production — Spark scans the data to *guess* types, which is slow on large files
> and can guess wrong (e.g. a numeric-looking ID column with leading zeros gets silently read as
> an integer, losing the zeros). Production pipelines define an explicit `StructType` schema.
> We're using `inferSchema=True` in this module purely to keep the code short while you're still
> learning the API surface.

## Inspecting a DataFrame

```python
df.show(5)          # print first 5 rows (this IS an action — triggers a real job)
df.printSchema()     # print column names + types — this is metadata only, no job runs
df.columns           # -> ['emp_id', 'name', 'department', ...] — Python list, no job runs
df.dtypes             # -> [('emp_id', 'int'), ('name', 'string'), ...]
df.count()            # -> number of rows — an action, triggers a real job
```

Notice the split: some things (`.columns`, `.printSchema()`) are answered from the DataFrame's
**schema metadata** alone — no distributed computation needed. Others (`.show()`, `.count()`)
require Spark to actually go read/compute data across partitions. This distinction — actions
that require computation vs. metadata that doesn't — is the seed of the next lesson (lazy
evaluation).

## A complete first program

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

spark = (
    SparkSession.builder
    .appName("first-program")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

employees = spark.read.csv("data/employees.csv", header=True, inferSchema=True)

avg_salary_by_dept = (
    employees
    .filter(col("salary").isNotNull())
    .groupBy("department")
    .agg(avg("salary").alias("avg_salary"))
    .orderBy(col("avg_salary").desc())
)

avg_salary_by_dept.show()

spark.stop()  # good practice: release cluster resources when your app is done
```

Run it yourself: save as a script and run it, or paste into a Python REPL with your venv active.
This exact pattern — filter nulls, `groupBy`, `agg`, `orderBy` — is worth typing out by hand
rather than copy-pasting, since you'll write variations of it constantly in real pipelines.

---
**Next:** [Lesson 5 — Lazy Evaluation and Execution Plans](05-lazy-evaluation-and-execution-plans.md)
