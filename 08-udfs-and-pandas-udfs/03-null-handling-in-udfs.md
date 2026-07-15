# Lesson 3 — NULL Handling Inside UDFs

Spark's `NULL` becomes Python's `None` inside a UDF — and unlike most Spark built-in functions,
which handle `NULL` inputs gracefully by design (e.g. `col("x") + 1` on a `NULL` `x` just produces
`NULL`, no error), **your Python function gets no such protection for free.** If your function's
logic doesn't explicitly account for `None`, ordinary Python arithmetic on `None` throws, and that
exception kills the whole task.

`data/employees.csv` has a genuine `NULL` salary — row 13, Mona Farouk, Engineering — which is
exactly the kind of real-world data quality gap that exposes this.

## The crash, verified against real data

```python
@udf(returnType=DoubleType())
def apply_raise_broken(salary):
    return salary * 1.1  # no None check

employees.withColumn("new_salary", apply_raise_broken(col("salary"))).show()
```

Verified — this raises immediately, from inside the Python worker, and the driver reports:

```
PythonException:
  An exception was thrown from the Python worker. Please see the stack trace below.
Traceback (most recent call last):
  ...
  File "...", line 24, in apply_raise_broken
    return salary * 1.1  # Mona Farouk has NULL salary in the data
           ~~~~~~~^~~~~
TypeError: unsupported operand type(s) for *: 'NoneType' and 'float'
```

The whole job fails — Spark doesn't skip the bad row and continue. One `NULL` in one row, in a
column you may not have even been thinking about, takes down the entire task.

## The fix: guard `None` explicitly

```python
@udf(returnType=DoubleType())
def apply_raise_safe(salary):
    if salary is None:
        return None
    return salary * 1.1

employees.withColumn("new_salary", apply_raise_safe(col("salary"))).show()
```

Verified output — every row succeeds, and Mona Farouk's row correctly propagates `NULL` rather
than crashing:

```
+------+--------------+-----------+--------+----------+----------+------------------+
|emp_id|          name| department|  salary| hire_date|manager_id|        new_salary|
+------+--------------+-----------+--------+----------+----------+------------------+
|     1|    Alice Chen|Engineering|125000.0|2019-03-14|      NULL|          137500.0|
...
|    13|   Mona Farouk|Engineering|    NULL|2023-11-01|         1|              NULL|
...
+------+--------------+-----------+--------+----------+----------+------------------+
```

## The rule

**Never assume a column feeding a UDF has no `NULL`s, even if you don't expect any** — real data has
gaps, and a UDF is the one place in your pipeline where a stray `NULL` becomes a hard crash instead
of a graceful propagation. Every UDF argument needs an explicit `is None` check (or equivalent) at
the top of the function unless you've verified, for this specific run, that the column is
guaranteed non-null (e.g. it's a join key you already filtered on).

This is also a strong argument for the built-in-function-first rule from Lesson 1: `col("salary") *
1.1` handles a `NULL` salary correctly with zero extra code, because null-propagation is built into
how Spark's native expressions work. A UDF gets none of that for free — you have to write it
yourself, every time.

---
**Next:** [Lesson 4 — Pandas UDFs and Arrow: Vectorized Execution](04-pandas-udfs-and-arrow.md)
