# Capstone 2 — Streaming Order Monitor

**Scenario:** the same order data from Capstone 1, except now it arrives as a stream (a landing
directory that files get dropped into) instead of one static CSV. The monitoring pipeline needs to
split each arriving micro-batch into clean orders and dead-lettered ones, write both durably and
idempotently, and survive being restarted mid-stream without reprocessing or duplicating anything.

## Requirements

1. **Streaming ingestion** (Module 10): read the landing directory as a `readStream` source with an
   explicit schema.
2. **Per-batch dead-letter split** (Module 10 Lesson 5, Module 12): inside `foreachBatch`, split
   each micro-batch into clean vs bad rows (same rule as Capstone 1 — bad `amount`, or an `emp_id`
   that doesn't exist in the employee roster) and write each to its own Delta table.
3. **Checkpointed, idempotent** (Module 10 Lesson 4, Module 11): use a real `checkpointLocation`.
4. **Verified fault tolerance**: the pipeline must be provably correct across a genuine
   process/JVM restart — not just re-running the same Python objects in memory.

## Try it yourself

`solution.py` takes a `stage1`/`stage2` argument plus four directory paths, meant to be invoked as
two **separate process runs** sharing the same directories — that's what actually proves checkpoint
recovery rather than just object reuse (Module 10 Lesson 4's exact standard). `run_capstone2.py` is
the driver that does both stages via `subprocess` and checks the final result — read it to see
exactly how the two-process test is wired, then try building your own version of `solution.py`
before checking against the real one.

```bash
python 15-capstone-projects/capstone-2-streaming-order-monitor/run_capstone2.py
```

## What you should notice once it's working

- Stage 1 processes the first 10 orders (all clean, since the one bad row is order `1013`, which
  falls in the second half of the file) — verified `silver=10`, `dead_letter=0`.
- Stage 2 is a **brand new Python process and JVM**, pointed at the same checkpoint. It correctly
  picks up only the remaining 5 orders (not reprocessing the first 10), correctly catches the one
  bad row, and ends at the exact same totals Capstone 1 computed from the static file — proving the
  streaming and batch versions of this pipeline agree on the final answer.

---
**Next:** [Capstone 3 — Customer 360 Lakehouse](../capstone-3-customer-360-lakehouse/)
