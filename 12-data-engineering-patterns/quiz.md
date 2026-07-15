# Module 12 Quiz

Answer each yourself before expanding the answer.

---

**1. A daily load job using `.write.format("delta").mode("append")` was run twice for the same
day (simulating a retry). Row count verified going from 2 to 4. What specifically makes an
`append`-based job non-idempotent, and what's the minimal change that fixes it?**

<details>
<summary>Answer</summary>

`append` always adds new files/rows regardless of whether equivalent data already exists in the
table — it has no concept of "this load already happened." Verified fix: writing with
`.mode("overwrite").option("replaceWhere", "load_date = '...'")` instead, which atomically replaces
exactly the rows matching that condition — re-running the same day's load just reproduces the same
partition's data rather than adding a second copy.
</details>

---

**2. `replaceWhere` validates that every row being written actually satisfies the replaceWhere
condition. What real mistake does this protect against?**

<details>
<summary>Answer</summary>

Writing data for the wrong partition due to a mismatched or incorrect condition string (e.g. a
typo'd date, or genuinely mixed-partition data in the batch) — Delta refuses the write rather than
silently overwriting a scope that doesn't match what was actually intended, which would otherwise
be a silent, hard-to-notice data-loss bug.
</details>

---

**3. In the verified SCD Type 2 pipeline, after bob's tier changed from silver to gold, the table
ended up with TWO rows for bob's `cust_id`, not one. Why is this the correct outcome, and what
specific column values distinguish the two rows?**

<details>
<summary>Answer</summary>

SCD Type 2 preserves full history rather than overwriting it (unlike Type 1). The two rows are
distinguished by `is_current` (`false` on the expired silver row, `true` on the new gold row) and
`effective_end`/`effective_start` (the expired row gets `effective_end` set to the change date; the
new row's `effective_end` is `NULL`, meaning "current, no end yet"). This is what makes
"what tier was this customer on some date in the past" answerable — `is_current` alone only
answers "what's true right now."
</details>

---

**4. Step 1 of the verified SCD2 pipeline compares incoming data against only the rows where
`is_current = true`, not the entire history table. Why does this matter?**

<details>
<summary>Answer</summary>

Comparing against the full history (including already-expired rows) could incorrectly treat "no
real change" as a change if an incoming value happens to match some older, no-longer-current
version of that key — the only version that matters for detecting a genuine change is the current
one.
</details>

---

**5. A hand-rolled data quality gate collected three independent failures (null customer, a
non-positive amount, and an unrecognized `emp_id`) and raised a single exception listing all three,
verified to prevent the write from ever executing. Why collect all failures before raising, rather
than raising on the first one found?**

<details>
<summary>Answer</summary>

So whoever is fixing the upstream data gets the complete picture in one pass — raising on the
first failure would mean fixing one issue, rerunning, discovering the next issue, rerunning again,
and so on, when all three were already knowable from the very first run.
</details>

---

**6. Reading a batch of JSON lines with an explicit schema and `columnNameOfCorruptRecord`
produced two different failure shapes: a line with broken JSON syntax had EVERY column come back
`NULL` (with `_corrupt_record` populated), while a line with valid JSON but a wrong-typed field
(`amount` as a string) had only THAT field come back `NULL` — with `_corrupt_record` still
populated for the whole line either way. What's the one condition that reliably detects both
failure classes?**

<details>
<summary>Answer</summary>

`_corrupt_record IS NOT NULL` — verified populated in both cases, even when only a single field
was actually wrong. This means dead-letter routing logic doesn't need separate detection paths for
"totally broken" versus "partially broken" rows; checking this one column catches both.
</details>

---

**7. Why does a dead-letter table need to preserve the raw original record (e.g. via
`_corrupt_record`), rather than just recording which columns came back `NULL`?**

<details>
<summary>Answer</summary>

Whoever investigates a dead-lettered batch needs the actual original input to diagnose what went
wrong upstream (a malformed client payload, a schema change, an encoding issue) — "some columns
were null" doesn't tell you why, but the raw text usually does.
</details>

---

**8. In the combined pipeline (Lesson 5), a row that fails the quality gate is routed to
dead-letter instead of raising an exception that stops the whole batch — a different choice than
Lesson 3's gate, which raises. What determines which of these two responses is the right one for a
given pipeline?**

<details>
<summary>Answer</summary>

Whether the pipeline should make partial progress or not. Raising and stopping the whole batch is
right when any bad row means something is fundamentally wrong and a human should look before
anything downstream changes (Lesson 3's framing). Routing to dead-letter and continuing is right
when a few known-shaped bad rows are an expected, tolerable trickle and the rest of the batch
should still land — there's no universally correct answer, it's a deliberate call based on how
costly each failure mode is for that specific pipeline.
</details>

---

**9. In the combined pipeline, BOTH the silver table and the dead-letter table used the same
`replaceWhere` idempotency key (`load_date`), and both stayed the same size across a verified
retry. What would have happened if the dead-letter write had used plain `.mode("append")` instead,
while silver kept using `replaceWhere`?**

<details>
<summary>Answer</summary>

Silver would have stayed correctly idempotent, but the dead-letter table would have silently
duplicated its entries on every retry — the same non-idempotency bug from Lesson 1, just moved to
a different output of the same pipeline. The lesson's point is that idempotency has to be applied
consistently to *every* write a pipeline makes, not just the primary "happy path" output.
</details>

---

**10. This module's data quality gate, dead-letter split, and SCD2 logic were all written by hand,
directly in PySpark. Real teams often use dedicated frameworks (Great Expectations, Deequ, dbt
tests) for quality checks instead. What's the value of having built the hand-rolled version first?**

<details>
<summary>Answer</summary>

Understanding exactly what a quality gate or dead-letter split has to guarantee (raise-before-write,
preserve-the-raw-record, apply-idempotency-consistently) makes it possible to evaluate whether a
framework's defaults actually provide those guarantees, and to debug it correctly when they don't —
rather than treating the framework as an opaque box that "does data quality" without knowing what
that concretely means.
</details>

---

Check the boxes in [`PROGRESS.md`](../PROGRESS.md) and move on to Module 13 when it's built.
