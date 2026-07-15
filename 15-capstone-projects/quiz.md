# Module 15 Quiz

Answer each yourself before expanding the answer.

---

**1. Capstone 1's quality gate quarantined exactly 1 order out of 15 raw orders (`order_id=1013`,
`emp_id=999`). After that row is removed, the "South" region disappears entirely from the gold
region-totals table. Why, precisely, and what would it mean if South had simply been silently
dropped instead of quarantined?**

<details>
<summary>Answer</summary>

`order_id=1013` was South's *only* order in the fixture — verified, removing it removes South's
sole row entirely, rather than reducing South's total. Silently dropping it (instead of routing it
to a dead-letter table) would leave no record that South's data was ever incomplete — anyone
looking at the gold report would see "no South orders" and have no way to know that's a data
quality artifact rather than a genuine business fact.
</details>

---

**2. Capstone 1 verified that every valid order traces back to the Sales department. Was this
designed into the fixture deliberately for a cleaner lesson, or is it a real property of
`orders.csv`/`employees.csv`?**

<details>
<summary>Answer</summary>

A real, verified property of the actual shared fixture — every `emp_id` appearing in `orders.csv`
(`4, 5, 6, 11`) maps to a Sales-department employee in `employees.csv`. It wasn't invented for the
capstone; it's an artifact of the existing course data actually being used honestly rather than
replaced with something more "interesting" but fabricated.
</details>

---

**3. Capstone 2 ran `solution.py` twice as two literally separate Python processes (via
`subprocess`), rather than calling the same function twice inside one process. What would a
single-process "restart" test have failed to actually prove?**

<details>
<summary>Answer</summary>

It would only prove that reusing the same Python objects remembers their own history — not that the
checkpoint itself durably persists offsets to disk in a way that survives a real crash or restart.
The genuine production guarantee that matters is surviving a process/JVM death (a deploy, a crash,
a scheduler restart), which only a truly separate process invocation can verify — exactly the same
standard Module 10 Lesson 4 established.
</details>

---

**4. Capstone 2's stage 2 (a brand new process) correctly processed only the 5 new orders, not all
15. What specifically, stored where, made that possible?**

<details>
<summary>Answer</summary>

The `checkpointLocation` shared between stage 1 and stage 2 — Delta/Structured Streaming's
checkpoint durably records which source files were already committed as of stage 1's exit. Stage
2's fresh query, on startup, reads that checkpoint and skips every file it already covers, only
picking up the genuinely new ones dropped after stage 1 finished.
</details>

---

**5. Capstone 2's final `silver` row count (14) and total revenue (7632.97) exactly matched
Capstone 1's numbers, computed from a completely different code path (streaming + foreachBatch vs.
a single batch read). Why is this agreement itself a meaningful verification, beyond each pipeline
individually passing its own asserts?**

<details>
<summary>Answer</summary>

It confirms the streaming and batch versions of essentially the same business logic converge on
the same correct answer, from the same raw source data — a real cross-check that neither
implementation has a hidden bug that happens to still pass its own narrower assertions. Two
independently-built pipelines agreeing is stronger evidence of correctness than either one passing
alone.
</details>

---

**6. Capstone 3's `point_in_time_join` matches fact rows to dimension rows using
`order_date >= effective_start AND (effective_end IS NULL OR order_date < effective_end)`, instead
of joining only on `is_current = true`. What specific, verified bug does the `is_current`-only
version have?**

<details>
<summary>Answer</summary>

It would show EVERY historical order under the customer's CURRENT tier, rewriting history — verified
directly: a test specifically checks that an order placed before bob's upgrade does NOT show
`tier='gold'`, which is exactly what an `is_current`-only join would incorrectly produce. The
point-in-time range join is what correctly preserves "what was true when this order actually
happened."
</details>

---

**7. Capstone 3 loaded the same `order_id=2` fact row twice (a simulated retry) and verified the
fact table stayed at exactly 2 rows, not 3. What specific `MERGE` clause made that idempotent, and
what would have happened with a plain `.write.mode("append")` instead?**

<details>
<summary>Answer</summary>

`whenNotMatchedInsertAll()` keyed on `order_id` — the second load of the same `order_id` finds a
match already present and simply does nothing (no `whenMatchedUpdate` clause was even specified),
so the retry is a no-op. A plain `append` would have inserted the row a second time regardless of
whether it already existed, producing a duplicate — exactly Module 12 Lesson 1's naive-append
non-idempotency, here at the fact-table-load level.
</details>

---

**8. `test_point_in_time_join_does_not_just_use_the_current_tier` in Capstone 3's test suite is
named for the specific bug it guards against, rather than just being called
`test_point_in_time_join_2`. Why does that naming choice matter, tying back to Module 13?**

<details>
<summary>Answer</summary>

Module 13 Lesson 5 established that a test's name should communicate exactly which behavior it's
verifying — if this specific test starts failing later (say, someone "simplifies" the join back to
an `is_current`-only join), the test name alone tells a future developer precisely what regressed,
without needing to read the test body or the original capstone's reasoning first.
</details>

---

**9. All three capstones reused Module 11's Delta Lake (`MERGE`, transaction log) rather than
plain Parquet. Name two specific capstone requirements that would have been substantially harder to
implement correctly without Delta.**

<details>
<summary>Answer</summary>

Idempotent fact-table loads (Capstone 3) rely on `MERGE`'s upsert semantics — plain Parquet has no
row-level update/insert at all, so avoiding duplicate `order_id`s would require manually rewriting
entire files. Streaming checkpoint recovery combined with a durable sink (Capstone 2) relies on
Delta's ACID transaction log to make each micro-batch's write atomic and resumable — a plain
Parquet sink has no equivalent transactional guarantee if a write is interrupted mid-batch.
</details>

---

**10. None of the three capstones invented a "toy" dataset from scratch for their core numbers —
Capstone 1 and 2 both used the real, shared `orders.csv`/`employees.csv`, and their exact totals
(`7632.97`, `14` clean orders, etc.) came from actually running the pipeline, not from working
backward from a number that "sounded right." Why does this matter for how much you should trust a
capstone's stated expected results?**

<details>
<summary>Answer</summary>

Every specific number in these capstones (region totals, quarantine counts, the top-employee name)
is a genuine, reproducible fact about the shared fixture data, verified by actually executing the
code — the same standard every module in this course held itself to. That means re-running any
capstone's solution yourself should reproduce the exact same numbers, not approximately similar
ones — a strong, checkable signal that the pipeline logic is genuinely correct rather than merely
plausible-looking.
</details>

---

Congratulations on completing the hands-on portion of this course! Move on to
[Module 16 — Interview Prep & Cheat Sheets](../16-interview-prep-and-cheat-sheets/) when it's built.
