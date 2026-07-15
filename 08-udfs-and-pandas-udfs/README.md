# Module 08 — UDFs & Pandas UDFs

A User-Defined Function (UDF) lets you drop arbitrary Python into a Spark pipeline — but every
Python UDF is a black box to Catalyst, the query optimizer, and it comes with real, verifiable
costs: no codegen, a JVM↔Python serialization hop per row, and return-type mismatches that fail
silently instead of erroring. This module is about knowing when a UDF is genuinely the wrong tool
(almost always — reach for a built-in function first), and how to write one safely and efficiently
when it truly is the right tool: `pandas_udf` for vectorized Series-to-Series work, and
`applyInPandas` for whole-group transformations.

## Learning objectives

By the end of this module you can:
- Explain *why* a Python UDF blocks Catalyst's whole-stage codegen — verified by comparing
  `.explain()` output for a native column expression against an equivalent UDF
- Register and use a basic UDF, and recognize the silent-`NULL` trap when a UDF's actual return
  value doesn't match its declared `returnType`
- Handle `NULL`/`None` explicitly inside a UDF, having seen the real crash that happens when you don't
- Write a vectorized `pandas_udf`, understand Arrow-based columnar batching (`ArrowEvalPython` vs
  `BatchEvalPython` in the physical plan), and read genuine, measured timing numbers instead of
  assuming a UDF is always 10-100x slower
- Use `applyInPandas` for whole-group transformations that need an entire group's data as one
  pandas DataFrame, and know the memory/skew risk that comes with that
- Apply a clear decision order: built-in function > Pandas UDF > row-at-a-time Python UDF

## Lessons

1. [Why UDFs Are a Last Resort](01-why-udfs-are-a-last-resort.md)
2. [Registering UDFs and the Return-Type Trap](02-registering-udfs-and-the-return-type-trap.md)
3. [NULL Handling Inside UDFs](03-null-handling-in-udfs.md)
4. [Pandas UDFs and Arrow: Vectorized Execution](04-pandas-udfs-and-arrow.md)
5. [applyInPandas and the Decision Tree](05-applyinpandas-and-the-decision-tree.md)

Then: [`exercises/`](exercises/) before [`solutions/`](solutions/), then [`quiz.md`](quiz.md).

Uses `/data/employees.csv`, including its genuine `NULL` salary row (Mona Farouk, Engineering) to
demonstrate real UDF null-handling failures and z-score edge cases — not synthetic nulls. This
module needs `pandas` and `pyarrow` in your venv (added to `requirements.txt`) in addition to the
packages from Module 00 — see the "Environment" note below if you're picking this module up fresh.
Every code example and output in this module was run against PySpark 3.5.3, pandas 3.0.3, and
pyarrow 25.0.0 — nothing here is hypothetical.

## Environment note

If your venv was set up before this module, install the two new dependencies:

```powershell
pip install pandas pyarrow
```

(or `pip install -r requirements.txt` again — they're now pinned there).

---
**Next:** [Module 09 — Performance Tuning](../09-performance-tuning/) *(not yet built)*
