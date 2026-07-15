# Module 15 — Capstone Projects

Three progressively harder pipelines, each combining techniques from across this entire course
instead of isolating one concept per lesson. This module is structured differently from every
other one: no numbered lessons, just a brief, a starter (TODO-driven) script, and a full solution
per project — closer to how a real take-home assignment or a first real ticket at a new job is
actually shaped.

## The three projects

| # | Project | Combines | Difficulty |
|---|---|---|---|
| 1 | [Retail Sales ETL](capstone-1-retail-sales-etl/) | Medallion architecture (11), quality gates (12), joins (05) | Intermediate |
| 2 | [Streaming Order Monitor](capstone-2-streaming-order-monitor/) | Structured Streaming (10), watermarks, Delta sinks (11), dead-letter (12) | Advanced |
| 3 | [Customer 360 Lakehouse](capstone-3-customer-360-lakehouse/) | SCD Type 2 (12), MERGE upserts (11), point-in-time joins, tested with pytest (13) | Advanced |

## How to work through these

1. Read the project's `README.md` brief first — business scenario, requirements, and the exact
   assertions your solution needs to satisfy.
2. Attempt `starter.py` yourself — it has the same TODO-and-self-check-assert shape every exercise
   in this course has used, just at project scale instead of single-concept scale.
3. Compare against `solution.py` only after a genuine attempt — every solution in this module was
   actually run against the real `data/orders.csv` / `data/employees.csv` fixtures, with the exact
   verified numbers baked into its assertions, not invented expected values.

Uses `delta-spark==3.3.0` (Module 11) throughout. Capstone 3 additionally uses `pytest`/`chispa`
(Module 13). Every solution's assertions were verified against real output before being written
into this module — the specific totals, counts, and top-employee/region results you'll see are
real numbers from the shared course fixtures, not illustrative placeholders.

---
**Next:** [Module 16 — Interview Prep & Cheat Sheets](../16-interview-prep-and-cheat-sheets/)
