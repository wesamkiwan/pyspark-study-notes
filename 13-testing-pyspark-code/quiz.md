# Module 13 Quiz

Answer each yourself before expanding the answer.

---

**1. A pipeline function reads a hardcoded CSV path, transforms it, and writes to a hardcoded
output path — all in one function. Why is this specific shape hard to unit test, and what's the
minimal structural change that fixes it?**

<details>
<summary>Answer</summary>

There's no seam to hook a test into without touching the real filesystem — you'd need a real file
at that exact path to test anything. The fix is separating I/O from transformation: a pure function
that takes a DataFrame and returns a DataFrame (no `SparkSession` creation, no hardcoded paths
inside it), with only a thin wrapper function doing the actual `spark.read`/`.write` calls. The pure
function can then be tested with a small, hand-built DataFrame — no real files needed at all.
</details>

---

**2. Three trivial tests were run once with a `function`-scoped `SparkSession` fixture and once
with a `session`-scoped one. Verified timings: `8.75s` total (function-scoped) vs `7.84s` total
(session-scoped) — a modest ~1 second difference for only 3 tests. Why does the gap matter far more
than this specific number suggests?**

<details>
<summary>Answer</summary>

The function-scoped suite pays a real setup/teardown cost (verified: `~0.1-0.5s` each) on *every
single test*, while the session-scoped suite pays the expensive part (JVM + Py4J gateway startup,
verified `~2.8-2.9s`) exactly once for the whole run, with near-zero per-test overhead afterward
(verified: subsequent setups under `0.005s`). With only 3 tests the difference looks small; a real
suite with hundreds of tests would see the function-scoped version's overhead compound into minutes
while the session-scoped version's stays flat.
</details>

---

**3. `assert_df_equality(df1, df2)` raised `DataFramesNotEqualError` on two DataFrames containing
literally the same rows, just in a different order. Is this a chispa bug? What should you actually
do about it?**

<details>
<summary>Answer</summary>

Not a bug — verified, intentional default behavior. Spark makes no guarantee about row order unless
you explicitly sort, so treating order as significant by default is the safer choice for a
comparison library. The fix is `ignore_row_order=True` when your transformation doesn't itself
guarantee an order — asserting exact row order for an operation that never promised one just makes
your test brittle for a reason unrelated to a real bug.
</details>

---

**4. Two DataFrames with byte-for-byte identical data failed `assert_df_equality` because one had
`nullable=True` and the other `nullable=False` in its schema. What does this tell you about what
`assert_df_equality` actually compares, and how do you fix a test where nullability genuinely
doesn't matter?**

<details>
<summary>Answer</summary>

It compares schema equality (including the `nullable` flag) in addition to data equality — not
just the literal values. Verified: identical data still fails if nullability differs. Fix:
`ignore_nullable=True`, appropriate whenever the test's actual point is comparing data, not
asserting nullability strictness as part of the contract being tested.
</details>

---

**5. `assert_df_equality` failed comparing a column holding `0.1 + 0.2` against a column holding
`0.3`, even though these "should" be equal. What's actually going on, and what's the correct chispa
function for this case?**

<details>
<summary>Answer</summary>

`0.1 + 0.2 != 0.3` in IEEE 754 floating point — verified directly in Python, not a Spark-specific
quirk. Any test comparing computed float columns with exact equality is fragile by construction.
`assert_approx_df_equality(df1, df2, precision=...)` is the fix, verified to correctly treat values
within the given tolerance as equal.
</details>

---

**6. A quality gate built entirely from per-row checks (null checks, value-range checks) was run
against a completely empty DataFrame and verified to PASS. Why, and what real production incident
does this correspond to?**

<details>
<summary>Answer</summary>

Every check in that gate is some form of "count of bad rows is zero," and on zero total rows that's
trivially true — there's nothing to be wrong with an empty batch by that definition. This
corresponds to a real, common incident shape: an upstream source silently sends nothing (a broken
cron job, a truncated file, a dropped connection), and a quality gate with no row-count check
happily reports success on an empty result.
</details>

---

**7. What's the fix for the empty-batch gap in question 6, and why is it a genuinely different
*class* of check than the per-row checks already in the gate?**

<details>
<summary>Answer</summary>

Add a minimum-row-count check (`if df.count() < min_expected_rows: raise ...`) before the per-row
checks run. It's a different class of check because it's about *volume*, not the validity of any
individual row — a batch can have zero invalid rows and still be a complete data quality failure if
it should have had rows at all.
</details>

---

**8. `empty_df.rdd.getNumPartitions()` returned `16` (matching `local[*]`'s default parallelism)
even though the DataFrame had zero rows. What does this confirm about how Spark represents an empty
DataFrame internally?**

<details>
<summary>Answer</summary>

An empty DataFrame still has a real, non-zero partition count — it's just that every partition
happens to contain zero rows. Partitioning is a structural property of how Spark would distribute
work, independent of whether there's currently any data to distribute.
</details>

---

**9. A verified test suite for `run_quality_gate` used five separate test functions — one per
distinct failure mode (null customer, non-positive amount, unknown emp_id, empty batch, and a
passing case) — rather than one test asserting all the failure conditions against one mixed "bad in
every way" batch. What's the practical benefit of splitting them, verified by how each test's
failure message reads?**

<details>
<summary>Answer</summary>

Each test's *name* tells you immediately and specifically which behavior broke —
`test_quality_gate_rejects_null_customer` failing points straight at the null-customer check,
rather than a single `test_everything` failure requiring you to read into the traceback or the
gate's own error message to figure out which of several bundled assertions actually failed.
</details>

---

**10. `find_changed_keys(incoming, current)` is tested with two separate test functions: one
feeding it a genuinely changed row, one feeding it an unchanged row. Why is testing "nothing should
be flagged as changed" just as important as testing "a real change gets detected," for this
specific function?**

<details>
<summary>Answer</summary>

A change-detection function has two distinct ways to be wrong: missing a real change (a false
negative) or flagging something that didn't actually change (a false positive). Module 12's SCD2
lesson showed a real bug where an unchanged row could get incorrectly processed due to lazy
DataFrame re-evaluation after a `MERGE` — a test that only ever checks "did it catch the real
change" would never catch a regression where *unchanged* rows start being flagged incorrectly,
which would silently corrupt a dimension table's history just as badly as missing a real change.
</details>

---

Check the boxes in [`PROGRESS.md`](../PROGRESS.md) and move on to Module 14 when it's built.
