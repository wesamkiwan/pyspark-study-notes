# Capstone 1 — Retail Sales ETL

**Scenario:** you've inherited `data/orders.csv` and `data/employees.csv` as the only two raw
sources for a retail sales reporting pipeline. Leadership wants three reports: total sales by
region, total sales by department, and a per-employee sales leaderboard. Some order records are
known to be bad (a made-up `emp_id` that doesn't exist in the employee roster) — those need to be
quarantined, not silently dropped or allowed to corrupt the reports.

## Requirements

1. **Bronze**: ingest both CSVs as Delta tables, unmodified, schema-enforced (Module 02, Module 11).
2. **Quality gate** (Module 12): any order with `amount <= 0`, or an `emp_id` that doesn't exist in
   the employee roster, must be quarantined into a separate dead-letter Delta table — not dropped
   silently, not allowed into silver.
3. **Silver**: the remaining clean orders, joined (Module 05) with employee `name`/`department`.
4. **Gold**: three aggregates, each its own Delta table — region totals, department totals, and a
   per-employee leaderboard (name + total + order count), sorted highest revenue first.

## Try it yourself

Open `starter.py` and fill in every `# TODO`. Run with:

```bash
python 15-capstone-projects/capstone-1-retail-sales-etl/starter.py
```

The self-check asserts at the bottom tell you exactly what's expected — including the real,
verified quarantine count, silver count, and gold totals from the actual shared fixture data.
Don't open `solution.py` until you've made a genuine attempt.

## What you should notice once it's working

- `orders.csv` has exactly **one** row with a made-up `emp_id` (`999`) — verified quarantined,
  leaving **14** clean orders in silver from the original **15**.
- Every single valid order in this fixture happens to trace back to the **Sales** department —
  verified, not a coincidental simplification on my part; the raw data really is that skewed.
- The regional breakdown loses **South** entirely once the bad row is quarantined — that row
  (`order_id=1013`) was South's only order.

---
**Next:** [Capstone 2 — Streaming Order Monitor](../capstone-2-streaming-order-monitor/)
