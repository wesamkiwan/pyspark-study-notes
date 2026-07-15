# Lesson 5 — Testing Real Pipeline Logic

The previous four lessons built the tools: testable function design, a shared `SparkSession`
fixture, `chispa` equality assertions, and deliberate edge-case coverage. This lesson puts them
together on real logic from Module 12 — a quality gate and an SCD2 change-detection function — and
verifies a complete, passing test suite end to end, not just individual assertions in isolation.

## The functions under test

```python
# pipeline_logic.py
class DataQualityError(Exception):
    pass

def run_quality_gate(df: DataFrame, known_emp_ids, min_expected_rows: int = 1) -> None:
    row_count = df.count()
    if row_count < min_expected_rows:
        raise DataQualityError(f"Expected at least {min_expected_rows} row(s), got {row_count}")
    # ... the per-row checks from Module 12 Lesson 3 ...

def find_changed_keys(incoming: DataFrame, current: DataFrame) -> DataFrame:
    """SCD2 step 1 (Module 12 Lesson 2): which keys have a genuinely different tier."""
    return incoming.alias("i").join(current.alias("c"), "cust_id") \
        .filter("i.tier != c.tier").select("i.cust_id", "i.name", "i.tier")
```

Both are pure functions per Lesson 1's rule — no file paths, no `SparkSession` creation, no
hardcoded table locations — which is exactly what makes the test suite below possible.

## A real test suite, verified passing end to end

```python
# test_pipeline_logic.py
import pytest
from chispa.dataframe_comparer import assert_df_equality
from pipeline_logic import DataQualityError, run_quality_gate, find_changed_keys

KNOWN_EMP_IDS = [4, 5, 6]

def test_quality_gate_passes_clean_batch(spark):
    good = spark.createDataFrame([(1, 4, "alice", 10.0), (2, 5, "bob", 20.0)], order_schema)
    run_quality_gate(good, KNOWN_EMP_IDS)  # should not raise

def test_quality_gate_rejects_empty_batch(spark):        # Lesson 4's edge case
    empty = spark.createDataFrame([], schema=order_schema)
    with pytest.raises(DataQualityError, match="Expected at least"):
        run_quality_gate(empty, KNOWN_EMP_IDS)

def test_quality_gate_rejects_null_customer(spark):
    bad = spark.createDataFrame([(1, 4, None, 10.0)], order_schema)
    with pytest.raises(DataQualityError, match="NULL customer"):
        run_quality_gate(bad, KNOWN_EMP_IDS)

def test_quality_gate_rejects_non_positive_amount(spark):
    bad = spark.createDataFrame([(1, 4, "alice", -5.0)], order_schema)
    with pytest.raises(DataQualityError, match="amount <= 0"):
        run_quality_gate(bad, KNOWN_EMP_IDS)

def test_quality_gate_rejects_unknown_emp_id(spark):
    bad = spark.createDataFrame([(1, 999, "alice", 10.0)], order_schema)
    with pytest.raises(DataQualityError, match="not in the known employee set"):
        run_quality_gate(bad, KNOWN_EMP_IDS)

def test_find_changed_keys_detects_real_change(spark):
    current = spark.createDataFrame([(1, "alice", "gold"), (2, "bob", "silver")], ["cust_id", "name", "tier"])
    incoming = spark.createDataFrame([(2, "bob", "gold")], ["cust_id", "name", "tier"])
    result = find_changed_keys(incoming, current)
    expected = spark.createDataFrame([(2, "bob", "gold")], ["cust_id", "name", "tier"])
    assert_df_equality(result, expected, ignore_row_order=True, ignore_nullable=True)

def test_find_changed_keys_ignores_unchanged_rows(spark):
    current = spark.createDataFrame([(1, "alice", "gold")], ["cust_id", "name", "tier"])
    incoming = spark.createDataFrame([(1, "alice", "gold")], ["cust_id", "name", "tier"])
    assert find_changed_keys(incoming, current).count() == 0
```

Verified: `pytest test_pipeline_logic.py -v` — **all 7 tests pass**, one assertion per distinct
behavior the underlying functions are supposed to have: the happy path, the empty-batch edge case
(Lesson 4), each individual quality-gate failure mode (each tested in isolation, not bundled), and
both the positive and negative case for SCD2 change detection.

## Reading this suite as a specification

Notice what this test file communicates, just by existing: `run_quality_gate` checks row count,
null customer, non-positive amount, and unknown `emp_id` — and rejects each independently. A new
teammate reading `test_pipeline_logic.py` learns the function's actual contract faster than reading
the implementation line by line, and — critically — if a future change to `run_quality_gate`
accidentally removes the null-customer check, `test_quality_gate_rejects_null_customer` fails
immediately, in CI, before that change ever reaches production data.

## Best-practice callout

**One test, one behavior.** Notice each quality-gate failure mode gets its own test function rather
than one giant test asserting all four failure conditions on one mixed "everything wrong" batch
(the style Module 12's manual script used, reasonably, for a quick demonstration). In an actual test
suite, splitting them means a failure tells you *immediately and specifically* which check broke —
`test_quality_gate_rejects_null_customer` failing is a much faster diagnosis than "one of the four
assertions in `test_everything` failed, go read the traceback."

---
Check the boxes in [`PROGRESS.md`](../PROGRESS.md), then: [`exercises/`](exercises/) before
[`solutions/`](solutions/), then [`quiz.md`](quiz.md).
