# Lesson 1 — Why Explicit Schemas

Module 01 used `inferSchema=True` to keep the code short while you were learning the API. Now
that the API is familiar, it's time to unlearn that habit — production pipelines almost always
define an explicit schema. This lesson shows exactly why, with a real, reproducible example of
`inferSchema` silently corrupting data.

## What `inferSchema=True` actually does

For schema-less formats like CSV and JSON, Spark can't know column types just by looking at the
file layout the way it can with Parquet (which stores its schema in the file itself). So
`inferSchema=True` makes Spark do an **extra full pass over the data first**, sampling values in
each column to guess a type, *before* the real read even begins. Two costs follow directly:

1. **Performance**: you pay to scan the data twice — once to guess types, once to actually read
   it. For a large file this is a real, avoidable cost.
2. **Correctness**: "guessing" means Spark can guess *wrong*, and it fails silently — no warning,
   no error, just quietly wrong data.

## Seeing the correctness problem yourself

Consider a CSV with US ZIP codes, some of which have a leading zero (real ZIP codes in the
northeastern US, like Holtsville, NY's `00501`, do):

```csv
zip_code,city
00501,Holtsville
10001,New York
```

```python
df = spark.read.csv("zip_test.csv", header=True, inferSchema=True)
df.printSchema()
df.show()
```

Output (verified — this is real, not a hypothetical):

```
root
 |-- zip_code: integer (nullable = true)
 |-- city: string (nullable = true)

+--------+----------+
|zip_code|      city|
+--------+----------+
|     501|Holtsville|
|   10001|  New York|
+--------+----------+
```

**`00501` silently became `501`.** Spark saw a column that looked numeric, inferred `IntegerType`,
and integers don't have leading zeros — so they're gone, permanently, with zero indication
anything went wrong. This is exactly the kind of bug that survives code review, passes tests
that don't check this specific value, and shows up three months later as "why are some ZIP
codes five digits and some are four?" in a downstream report.

This isn't a contrived example — it's one of the most common real-world `inferSchema` failures,
alongside similar issues with IDs that look numeric but aren't meant to be treated as numbers,
and dates/timestamps in ambiguous formats Spark guesses wrong.

## The fix: define the schema explicitly

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

employee_schema = StructType([
    StructField("emp_id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("department", StringType(), nullable=True),
    StructField("salary", DoubleType(), nullable=True),
    StructField("hire_date", DateType(), nullable=True),
    StructField("manager_id", IntegerType(), nullable=True),
])

employees = spark.read.csv("data/employees.csv", header=True, schema=employee_schema)
```

Note `zip_code` in our example would be declared `StringType()` — the fix isn't "pick a smarter
type," it's "a human decides the type based on what the data *means*, not what it superficially
looks like." A ZIP code is an identifier, not a number you'd ever do arithmetic on — it should
never have been inferred as numeric in the first place.

## Why this matters beyond correctness

An explicit schema is also **documentation and a contract**:
- Anyone reading the code immediately sees every column and its type, without running anything.
- If the actual file doesn't match (missing column, wrong type, extra column), you find out
  immediately via a clear error — instead of a silent surprise discovered downstream.
- `nullable=False` lets you assert an expectation about the data (though note: Spark does not
  strictly *enforce* this for most sources — see the callout below).

> **Nuance:** for CSV/JSON, setting `nullable=False` in your schema is closer to *documentation
> of intent* than a hard guarantee — Spark will still let a null through from these sources in
> many cases rather than throwing. Don't rely on `nullable=False` alone as a data-quality gate;
> pair it with an explicit `.filter()`/validation step if nulls are truly unacceptable. This
> matters much more (and is enforced much more strictly) for typed formats like Parquet.

## When `inferSchema=True` is genuinely fine

Being dogmatic isn't the goal — judgment is. `inferSchema=True` is reasonable for:
- Ad hoc, one-off exploration of a new dataset you don't yet understand.
- Small, low-stakes local scripts where a wrong guess is immediately obvious and cheap to fix.

It should not be in code that runs on a schedule, feeds a system other people depend on, or
processes data you haven't personally eyeballed.

---
**Next:** [Lesson 2 — File Formats Deep Dive](02-file-formats-deep-dive.md)
