# Lesson 3 — Grouping and Aggregating

`groupBy().agg(...)` is the DataFrame-API equivalent of SQL's `GROUP BY`. The mechanics are
simple; the sharp edges are about column naming and about *when* you're allowed to filter on an
aggregated value.

## Basic aggregation, and why the default column names are ugly

```python
from pyspark.sql.functions import avg, count

employees.groupBy("department").agg(avg("salary"), count("*"))
```

```
+-----------+------------------+--------+
| department|       avg(salary)|count(1)|
+-----------+------------------+--------+
|  Marketing|61333.333333333336|       3|
|    Finance|           86500.0|       2|
|      Sales|           71250.0|       4|
|Engineering|          109200.0|       6|
+-----------+------------------+--------+
```

`.columns` on that result is `['department', 'avg(salary)', 'count(1)']` — Spark names an
un-aliased aggregate column after the expression that produced it. That works, but it's awkward to
reference downstream (you'd need backticks: `` col("`avg(salary)`") ``), unreadable in a wider
pipeline, and the full floating-point average is rarely what you actually want to display. **Always
alias your aggregates:**

```python
from pyspark.sql.functions import count, avg, sum as spark_sum, round as spark_round

dept_stats = (
    employees.groupBy("department")
    .agg(
        count("*").alias("headcount"),
        spark_round(avg("salary"), 2).alias("avg_salary"),
        spark_sum("salary").alias("total_salary"),
    )
    .orderBy(col("avg_salary").desc())
)
```

```
+-----------+---------+----------+------------+
| department|headcount|avg_salary|total_salary|
+-----------+---------+----------+------------+
|Engineering|        6|  109200.0|    546000.0|
|    Finance|        2|   86500.0|    173000.0|
|      Sales|        4|   71250.0|    285000.0|
|  Marketing|        3|  61333.33|    184000.0|
+-----------+---------+----------+------------+
```

> **Note on the import alias:** `sum` and `round` are Python built-ins. Importing
> `pyspark.sql.functions.sum`/`round` without renaming them silently shadows the built-ins for the
> rest of the file — usually harmless if you only ever use the Spark versions, but a real trap if
> any other code in the same file expects the plain Python `sum()`/`round()`. Aliasing on import
> (`sum as spark_sum`) or importing the module and using `F.sum`/`F.round` avoids the ambiguity
> entirely — most production PySpark code favors `from pyspark.sql import functions as F`.

## Filtering an aggregated result: no `HAVING`, just `.filter()` after `.agg()`

SQL needs a separate `HAVING` clause because `WHERE` runs before grouping. The DataFrame API has
no such split — a plain `.filter()` **after** `.agg()` operates on the aggregated result, which is
exactly what `HAVING` does in SQL:

```python
dept_stats.filter(col("headcount") >= 4)
```

```
+-----------+---------+----------+------------+
| department|headcount|avg_salary|total_salary|
+-----------+---------+----------+------------+
|Engineering|        6|  109200.0|    546000.0|
|      Sales|        4|   71250.0|    285000.0|
+-----------+---------+----------+------------+
```

There's no separate "having" method to look for — the position of `.filter()` in the chain (before
vs. after `.groupBy().agg()`) is what determines whether it behaves like `WHERE` or `HAVING`.

## `countDistinct` vs `count`

```python
from pyspark.sql.functions import countDistinct, count

orders.select(countDistinct("emp_id").alias("distinct_buyers"), count("*").alias("total_orders"))
```

```
+---------------+------------+
|distinct_buyers|total_orders|
+---------------+------------+
|              5|          15|
+---------------+------------+
```

`countDistinct` needs to track every unique value it's seen, which for a huge, high-cardinality
column can mean real memory pressure on executors. When you need an approximate distinct count
over very large data and can tolerate a small margin of error, `approx_count_distinct` uses a
fixed, bounded amount of memory (HyperLogLog under the hood) regardless of cardinality — a
worthwhile trade in that specific situation, not a general replacement for `countDistinct` on
data where you actually need the exact number.

---
**Next:** [Lesson 4 — Sorting, Deduplication, and Combining DataFrames](04-sort-dedup-union.md)
