# Lesson 5 — The Catalog API: Temp Views vs Managed vs External Tables

Module 03 Lesson 5 covered temp views: `createOrReplaceTempView(...)`, scoped to the current
`SparkSession`, gone once that session ends. This lesson covers the rest of what `spark.catalog`
manages — **persistent** tables that survive across sessions and restarts — and the one
distinction inside that (managed vs external) that decides whether `DROP TABLE` deletes your data
or just forgets about it.

## `spark.catalog`: what's registered right now

```python
employees.createOrReplaceTempView("employees")
orders.createOrReplaceTempView("orders")

spark.catalog.listTables()
```

```
[Table(name='employees', catalog=None, namespace=[], description=None,
       tableType='TEMPORARY', isTemporary=True),
 Table(name='orders', catalog=None, namespace=[], description=None,
       tableType='TEMPORARY', isTemporary=True)]
```

`listColumns("employees")` gives you the resolved schema of anything registered, temp view or real
table alike — useful for programmatically checking a table's shape before writing a query against
it, without needing a separate DataFrame reference lying around:

```python
spark.catalog.listColumns("employees")
```

```
[Column(name='emp_id', ..., dataType='int', ...),
 Column(name='name', ..., dataType='string', ...),
 Column(name='department', ..., dataType='string', ...),
 Column(name='salary', ..., dataType='double', ...),
 Column(name='hire_date', ..., dataType='date', ...),
 Column(name='manager_id', ..., dataType='int', ...)]
```

## Managed tables: `saveAsTable` persists *and* stores the data for you

```python
spark.sql("SELECT * FROM employees WHERE department = 'Finance'") \
    .write.mode("overwrite").saveAsTable("finance_employees")

spark.catalog.listTables()
```

```
[Table(name='finance_employees', catalog='spark_catalog', namespace=['default'],
       description=None, tableType='MANAGED', isTemporary=False),
 Table(name='employees', ..., tableType='TEMPORARY', isTemporary=True),
 Table(name='orders', ..., tableType='TEMPORARY', isTemporary=True)]
```

Unlike a temp view, `finance_employees` is `isTemporary=False` — it's registered in the actual
metastore (a local `spark-warehouse/` directory and `metastore_db/` in this course's local setup;
a real Hive/Unity/Glue metastore in production) and is visible to **any** session that connects to
that same metastore, not just the one that created it. `DESCRIBE EXTENDED` shows where Spark
actually put the data:

```python
spark.sql("DESCRIBE EXTENDED finance_employees").show(truncate=False)
```

```
...
|Type                        |MANAGED                                                    |
|Provider                    |parquet                                                    |
|Location                    |file:/.../pyspark-study-notes/spark-warehouse/finance_employees |
```

`Type: MANAGED` means Spark owns the data file's location and lifecycle — it picked the path
(inside `spark-warehouse/`), and it will manage deleting it too.

## The trap: `DROP TABLE` on a MANAGED table deletes the data files

```python
print(os.path.exists(location))   # True
spark.sql("DROP TABLE finance_employees")
print(os.path.exists(location))   # False
```

Verified: the Parquet files backing `finance_employees` are gone from disk after the `DROP TABLE`,
not just the metastore entry. For a table you created with `saveAsTable`, that's usually what you
want — it's Spark's data, not something borrowed from elsewhere.

## External tables: `DROP TABLE` only forgets, never deletes

If instead you point Spark at data that already exists somewhere (files another job wrote, a
shared location other tools also read), register it as an **external** table with `LOCATION`:

```python
employees.filter(col("department") == "Finance") \
    .write.mode("overwrite").parquet("/some/shared/path/finance")

spark.sql("CREATE TABLE ext_finance USING parquet LOCATION '/some/shared/path/finance'")
spark.sql("DESCRIBE EXTENDED ext_finance").show()
# Type: EXTERNAL
```

```python
print(os.path.exists("/some/shared/path/finance"))   # True
spark.sql("DROP TABLE ext_finance")
print(os.path.exists("/some/shared/path/finance"))   # True -- files untouched
```

Verified: dropping `ext_finance` only removes the metastore's *reference* to it — the actual
Parquet files at that location are completely untouched, because Spark never owned them; it was
only pointed at a location someone else's data already lived in.

**The rule to internalize:** before running `DROP TABLE` on anything, check whether it's `MANAGED`
or `EXTERNAL` (`DESCRIBE EXTENDED` or `spark.catalog.listTables()` tells you). Dropping a `MANAGED`
table is a real, irreversible delete of the underlying data — treat it with the same caution you'd
give `rm -rf` on that path, not the same casualness as dropping a temp view.

---
This closes out Module 04. Next: [`exercises/`](exercises/), then [`quiz.md`](quiz.md).
