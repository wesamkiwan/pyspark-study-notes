# Capstone 3 — Customer 360 Lakehouse

**Scenario:** a customer's tier changes over time (Module 12's SCD Type 2), and orders keep
arriving that reference a customer by ID. Leadership wants every order correctly attributed to the
tier that customer actually had **at the time the order was placed** — not whatever tier that
customer happens to have today. Getting this wrong is a real, subtle bug: naively joining orders
against "the current customer record" silently rewrites history every time a customer's tier
changes.

## Requirements

1. **SCD2 customer dimension** (Module 12): `cust_id`, `tier`, `effective_start`, `effective_end`,
   `is_current` — a tier change expires the old row and inserts a new one, full history kept.
2. **Idempotent fact table loads** (Module 11/12): orders loaded via `MERGE` keyed on `order_id` —
   loading the exact same order twice must not create a duplicate row.
3. **Point-in-time correct join**: join each fact row to the dimension row whose
   `[effective_start, effective_end)` range actually contains that order's date — not to
   `is_current = true`.
4. **Tested** (Module 13): `pipeline_logic.py` holds the pure functions;
   `test_solution.py` tests them in isolation, including a test that specifically guards against
   the "just joins to today's tier" bug.

## Try it yourself

Open `starter.py` and fill in every `# TODO`. `pipeline_logic.py` is provided as-is (the functions
under test) — your job is wiring them into a working pipeline, exactly like `run_job` wired
`transform.py`'s pure functions together back in Module 13 Lesson 1. Run with:

```bash
python 15-capstone-projects/capstone-3-customer-360-lakehouse/starter.py
```

Then run the test suite:

```bash
pytest 15-capstone-projects/capstone-3-customer-360-lakehouse/test_solution.py -v
```

Don't peek at `solution.py` until you've made a genuine attempt at both.

## What you should notice once it's working

- The dimension ends up with **three** rows total — alice (untouched, one row) and bob (two rows:
  an expired `silver` version and a current `gold` version) — verified.
- An order placed on `2024-01-01` (before bob's upgrade) correctly joins to `tier='silver'`; an
  order placed on `2024-01-02` (the day of/after the upgrade) correctly joins to `tier='gold'` —
  verified against the exact same dimension table, proving the join is genuinely point-in-time
  aware, not just returning whatever bob's tier happens to be right now.
- Loading the second order's batch **twice** (a simulated retry) still leaves the fact table at
  exactly 2 rows, not 3 — the `MERGE`-based load is verified idempotent.

---
Read [Module 15's README](../README.md) again for the full picture, then move on to
[Module 16 — Interview Prep & Cheat Sheets](../../16-interview-prep-and-cheat-sheets/) once it's built.
