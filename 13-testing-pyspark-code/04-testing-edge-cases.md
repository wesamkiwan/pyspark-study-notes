# Lesson 4 — Testing Edge Cases

Tests written only against "normal-looking" data miss an entire class of bugs. This lesson
verifies how Spark actually behaves on the edge cases every real pipeline eventually hits — empty
input above all — and a genuinely important consequence for how you test quality gates
specifically (Module 12).

## Empty DataFrames don't error — verified, and that's not always good news

```python
schema = StructType([StructField("order_id", IntegerType()), StructField("customer", StringType()), StructField("amount", DoubleType())])
empty_df = spark.createDataFrame([], schema=schema)

empty_df.count()                                    # 0
empty_df.filter(col("amount") > 0).count()           # 0 -- no error
empty_df.groupBy("customer").agg(spark_sum("amount")).count()   # 0 -- no error, correct schema either way
```

Verified: every ordinary transformation handles an empty DataFrame gracefully — filtering,
grouping, aggregating all just produce empty results with the correct schema, no special-casing
needed in your transformation code. **Verified oddity worth knowing:** `empty_df.rdd.getNumPartitions()`
still returned `16` (matching `local[*]`'s default parallelism) even with zero rows — an empty
DataFrame still has a real partition count, it's just that every partition happens to be empty.

## The real trap: an empty batch trivially PASSES a naive quality gate

```python
def run_quality_gate(df):
    null_customer = df.filter(col("customer").isNull()).count()
    non_positive = df.filter(col("amount") <= 0).count()
    return null_customer == 0 and non_positive == 0

run_quality_gate(empty_df)   # True -- verified
```

Verified: Module 12's quality gate pattern, run against a completely empty batch, **passes**. Every
check is some variant of "count of bad rows is zero" — and on zero rows, that's trivially true. If
an upstream source silently sent nothing (a broken cron job, a truncated file, a network partition
that dropped every record), a quality gate built only from per-row checks won't catch it at all —
it'll happily wave through an empty batch as "clean."

**This is a real, common production incident shape**: "the pipeline ran successfully, quality gate
passed, but the table has zero new rows today" — caught by no per-row check, because there were no
rows to check. The fix is a **volume check**, a different class of test entirely:

```python
def run_quality_gate(df, min_expected_rows=1):
    row_count = df.count()
    if row_count < min_expected_rows:
        raise DataQualityError(f"Expected at least {min_expected_rows} row(s), got {row_count}")
    # ... the rest of the per-row checks from Module 12 ...
```

## Testing this explicitly

```python
def test_quality_gate_rejects_empty_batch(spark):
    empty = spark.createDataFrame([], schema=order_schema)
    with pytest.raises(DataQualityError):
        run_quality_gate(empty)

def test_quality_gate_passes_normal_batch(spark):
    good = spark.createDataFrame([(1, "alice", 10.0)], ["order_id", "customer", "amount"])
    run_quality_gate(good)   # should not raise
```

Writing the empty-batch test **first**, before assuming the gate handles it correctly, is exactly
the value of testing edge cases deliberately — it's easy to design a quality gate entirely around
"what if a row is bad" and never once ask "what if there are no rows at all."

## Other edge cases worth a deliberate test

- **A DataFrame where every value in a column is `NULL`** (not just some) — aggregations like
  `sum`/`avg` over an all-`NULL` column return `NULL`, not `0`, which can silently propagate into a
  downstream calculation expecting a number (Module 03's null-handling rules apply here directly).
- **A single-row DataFrame** — window functions (Module 07) and some aggregations behave
  differently at the boundary of "just one group" versus "multiple groups"; worth a dedicated test
  rather than assuming the multi-row case generalizes down.
- **Duplicate rows in input that should be deduplicated** — exactly Module 11/12's dedup and SCD2
  logic; a test with a deliberately duplicated key is the cheapest way to confirm dedup logic
  actually removes duplicates rather than just looking like it does on already-clean sample data.

## Best-practice callout

For any quality gate or validation function, always write at least one test with genuinely empty
input, and treat "does this pass when it obviously shouldn't" as the specific question that test is
asking — not just "does this crash."

---
**Next:** [Lesson 5 — Testing Real Pipeline Logic](05-testing-real-pipeline-logic.md)
