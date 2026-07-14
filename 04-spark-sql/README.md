# Module 04 — Spark SQL

Module 03's last lesson showed that `spark.sql(...)` and the DataFrame API compile to the same
plan. This module goes deeper into the SQL side specifically: writing dynamic SQL without an
injection risk, CTEs and subqueries, a naming trap where SQL's `UNION` keyword does *not* mean
what the DataFrame `.union()` method means, SQL's three-valued NULL logic, and the Catalog API
that manages both temporary views and persistent tables.

## Learning objectives

By the end of this module you can:
- Write parameterized `spark.sql(...)` queries that are safe against injection, instead of
  building query strings by concatenation
- Use CTEs (`WITH`) and correlated subqueries (`EXISTS`), and know why SQL's `UNION` silently
  deduplicates while `UNION ALL` (and the DataFrame's `.union()`) doesn't
- Reason correctly about `NULL` in `WHERE`/`NOT`/`IN` — SQL's three-valued logic, not Python's
  two-valued logic
- Bridge SQL text and the DataFrame API with `expr()`/`selectExpr()`, and know what silently
  happens when a `CAST` fails
- Use the Catalog API to tell temp views apart from persistent managed/external tables, and know
  which one loses your data when you `DROP TABLE`

## Lessons

1. [Parameterized SQL: The Injection-Safe Way to Build Dynamic Queries](01-parameterized-sql.md)
2. [CTEs, Subqueries, and the UNION vs UNION ALL Trap](02-ctes-subqueries-union.md)
3. [NULL and Three-Valued Logic in SQL](03-null-three-valued-logic.md)
4. [Bridging SQL and the DataFrame API: expr(), selectExpr(), and CAST](04-expr-selectexpr-cast.md)
5. [The Catalog API: Temp Views vs Managed vs External Tables](05-catalog-managed-external.md)

Then: [`exercises/`](exercises/) before [`solutions/`](solutions/), then [`quiz.md`](quiz.md).

Uses `/data/employees.csv` and `/data/orders.csv`. Every code example and output in this module
was run against these exact files with PySpark 3.5.3 — nothing here is hypothetical.
