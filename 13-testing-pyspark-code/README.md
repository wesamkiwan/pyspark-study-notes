# Module 13 — Testing PySpark Code

Every module so far has been verified by running a script and reading its output — good enough for
learning, but not how a real codebase stays correct as it grows. This module covers how to write
actual automated tests for PySpark transformations: structuring code so it's testable at all,
reusing a `SparkSession` across a test suite instead of paying its startup cost per test, and using
`chispa` for DataFrame equality assertions — including several of its default behaviors that are
stricter than they first appear, verified directly rather than assumed from its docs.

## Learning objectives

By the end of this module you can:
- Structure a PySpark transformation as a pure function (DataFrame in, DataFrame out) so it can be
  unit tested without spinning up real files, a real cluster, or a real upstream system
- Use a `session`-scoped pytest fixture to share one `SparkSession` across an entire test suite,
  verified against a `function`-scoped fixture's real, measured startup/teardown cost
- Use `chispa.assert_df_equality` correctly, verified against its actual default sensitivities —
  row order, column order, and nullable-flag differences all fail by default, even when every
  literal data value matches
- Use `assert_approx_df_equality` for floating-point columns, verified against a genuine float
  imprecision case (`0.1 + 0.2 != 0.3`) that exact equality would incorrectly fail on
- Write tests for edge cases (nulls, empty DataFrames, schema mismatches) that are easy to miss
  when only testing against "normal" data
- Apply this to actual functions from earlier modules — Module 12's quality gate and SCD2 logic —
  as realistic testing targets, not synthetic toy examples

## Lessons

1. [Testable Pipeline Design](01-testable-pipeline-design.md)
2. [SparkSession Fixtures and Test Speed](02-sparksession-fixtures.md)
3. [DataFrame Equality with chispa, Verified](03-dataframe-equality-with-chispa.md)
4. [Testing Edge Cases](04-testing-edge-cases.md)
5. [Testing Real Pipeline Logic](05-testing-real-pipeline-logic.md)

Then: [`exercises/`](exercises/) before [`solutions/`](solutions/), then [`quiz.md`](quiz.md).

Uses `pytest==9.1.1` and `chispa==0.12.0` (already pinned in `requirements.txt` from the start of
this course). Every code example and output in this module was run against PySpark 3.5.3 on the
local Windows venv — including running the actual test suites with `pytest`, not just reading
described behavior.

---
**Next:** [Module 14 — Production & Deployment](../14-production-and-deployment/)
