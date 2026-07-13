# Module 03 — Core DataFrame Transformations

This is the module where you start actually *shaping* data: picking columns, computing new ones,
filtering rows, grouping and aggregating, sorting, deduplicating, and combining DataFrames. These
five operations — `select`, `filter`, `withColumn`, `groupBy`/`agg`, and choosing DataFrame API vs
SQL — cover the large majority of what a real transformation pipeline actually does.

## Learning objectives

By the end of this module you can:
- Build and rename columns fluently with `select`, `withColumn`, and column expressions
- Filter correctly with `filter`/`where`, avoiding the classic Python-operator-precedence trap
- Handle conditional logic and nulls with `when`/`otherwise` and `coalesce`
- Aggregate with `groupBy().agg()`, alias results cleanly, and filter aggregated results
- Sort, deduplicate, and safely combine DataFrames — including the `union` column-order trap
- Choose between the DataFrame API and Spark SQL deliberately, and mix them safely

## Lessons

1. [Selecting and Creating Columns](01-select-and-withcolumn.md)
2. [Filtering and Conditional Logic](02-filter-and-conditionals.md)
3. [Grouping and Aggregating](03-groupby-and-agg.md)
4. [Sorting, Deduplication, and Combining DataFrames](04-sort-dedup-union.md)
5. [DataFrame API vs Spark SQL](05-dataframe-api-vs-sql.md)

Then: [`exercises/`](exercises/) before [`solutions/`](solutions/), then [`quiz.md`](quiz.md).

Uses `/data/employees.csv` and `/data/orders.csv`. Every code example and output in this module
was run against these exact files with PySpark 3.5.3 — nothing here is hypothetical.
