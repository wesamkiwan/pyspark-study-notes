# Module 11 Quiz

Answer each yourself before expanding the answer.

---

**1. A Delta table is, physically, just a folder of ordinary Parquet files plus one extra thing.
What is that extra thing, and what does it actually make possible that plain Parquet can't do?**

<details>
<summary>Answer</summary>

A `_delta_log/` directory of versioned JSON commit files, recording exactly which Parquet files
belong to the table at each version. It's what makes ACID writes (a version only becomes visible
once its whole commit lands), time travel (`versionAsOf`/`timestampAsOf` read exactly the files a
past version's log entry lists), and row-level `MERGE`/`UPDATE`/`DELETE` possible — none of which
plain Parquet supports at all.
</details>

---

**2. `spark.read.format("delta").option("versionAsOf", 0).load(path)` returns exactly the version-0
rows, even though the physical folder on disk currently contains files from later versions too. How
does this actually work under the hood?**

<details>
<summary>Answer</summary>

The reader consults version 0's entry in `_delta_log/`, which lists exactly which Parquet files
were considered "live" as of that version, and only reads those files — it isn't filtering rows
by a timestamp column, it's reading a different, explicit file manifest for that version.
</details>

---

**3. A `MERGE` with `whenMatchedUpdateAll()` and `whenNotMatchedInsertAll()` against a target table
correctly updated one existing row and inserted one brand-new row, in a single atomic operation.
What real capability does this give you that plain `append`/`overwrite` writes don't?**

<details>
<summary>Answer</summary>

A genuine upsert — "update this row if the key already exists, insert it if it's new" — as one
atomic operation. Plain Parquet (or a plain Delta `append`) has no equivalent: the only options
without `MERGE` are rewriting entire files/partitions yourself or reloading the whole table.
</details>

---

**4. Appending a DataFrame with a genuinely new column, using `.option("mergeSchema", "true")`,
to a Delta table where the key column's value (e.g. `cust_id=1`) already exists produced a
**second, duplicate row** for that key rather than updating the existing one. Why, precisely?**

<details>
<summary>Answer</summary>

`mergeSchema` only solves the *schema* problem — it lets a write whose columns differ from the
table's current schema succeed instead of raising the schema-mismatch `AnalysisException`. It has
nothing to do with row-level identity: `.mode("append")` still means append, full stop. If the goal
is "update this key if it exists, insert if new, and tolerate a new column," that's `MERGE`'s job,
not something schema evolution alone provides — conflating the two silently produces duplicate
rows, not an error.
</details>

---

**5. After running `dt.optimize().executeCompaction()` on a table with 16 small Parquet files, the
file count verified as going UP to 17, not down. Row counts were unaffected. What is `OPTIMIZE`
actually doing, and what step is still required to reduce the file count on disk?**

<details>
<summary>Answer</summary>

`OPTIMIZE` writes new, compacted files and updates the transaction log so the *current version*
reads only the new compacted file(s) — but it deliberately leaves the old small files on disk,
because an older version (time travel) might still need them. `VACUUM` is the separate step that
physically deletes files nothing needs anymore — verified: it brought the file count from 17 down
to 1 in this experiment.
</details>

---

**6. `dt.vacuum(0)` raised `IllegalArgumentException: requirement failed: Are you sure you would
like to vacuum files with such a low retention period?` and refused to run. What has to change for
it to actually execute, and why does this safety check exist at all?**

<details>
<summary>Answer</summary>

Setting `spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")` disables
the check, letting `vacuum(0)` run. The check exists because `VACUUM` is genuinely destructive —
the default 7-day retention window is meant to comfortably outlast any in-flight readers, a paused
stream about to resume, or a `versionAsOf` debugging session that might still need an older
version's files; a low retention period risks deleting files something still legitimately needs.
</details>

---

**7. Reading `option("versionAsOf", 0)` on a table right after `vacuum(0)` deleted that version's
files raised a `Py4JJavaError`, not a clean, documented error. What does this confirm about the
relationship between `VACUUM` and time travel?**

<details>
<summary>Answer</summary>

`VACUUM` is genuinely destructive to history — once it deletes the files a past version's log
entry points to, time travel to that version is permanently broken, verified directly by the read
failing outright. This is exactly why the default retention window and safety check exist (Q6):
`VACUUM` isn't a reversible cleanup, it's real deletion.
</details>

---

**8. Two threads both ran a `MERGE` against the same row of the same table at the same time. One
succeeded; the other failed with `io.delta.exceptions.ConcurrentAppendException`. What does this
verify about Delta's concurrency model, and what should production code actually do when it catches
this exception?**

<details>
<summary>Answer</summary>

It verifies Delta uses **optimistic concurrency control**: both writers read the same starting
version, the first to commit wins, and the second writer's commit — based on now-stale
information about which files exist — is rejected outright rather than silently racing and
producing a corrupted or nondeterministic result. Production code should catch
`ConcurrentAppendException` (and its siblings) around any `MERGE`/`UPDATE`/`DELETE` that might race
with another writer and **retry** — this is the normal, expected way concurrent Delta writes
resolve, not a fatal error to propagate.
</details>

---

**9. After `dt.restoreToVersion(0)` ran against a table currently at version 1, `dt.history()`
showed a **new** version 2 with operation `RESTORE` — version 1 was still listed, not deleted. Why
does `RESTORE` work this way instead of just deleting version 1's log entry?**

<details>
<summary>Answer</summary>

`RESTORE` is designed to be itself a safe, auditable, undoable action — it creates a new commit
whose data matches the target version, rather than rewriting or erasing history. This means the
full audit trail ("what happened and when") is preserved, and a `RESTORE` can itself be undone by
restoring back to the version it just moved away from.
</details>

---

**10. In a verified bronze/silver/gold pipeline, bronze went from 15 to 16 rows after a deliberate
duplicate re-send of `order_id=1001`; silver (after a `row_number()`-based dedup) came back down to
15; gold's region totals summed to exactly 15 orders across 4 regions. If the dedup step in silver
had been skipped or written incorrectly, where specifically would the bug have shown up, and why is
that the *wrong* place to have caught it?**

<details>
<summary>Answer</summary>

It would show up in gold — West's `total_amount` would be inflated by the duplicate order's
amount, and `order_count` would sum to 16 instead of 15 across regions. That's the wrong place to
catch it because gold is what dashboards and business stakeholders actually query — by the time a
data quality bug is visible there, it's already been reported on. The correct place to catch it is
comparing bronze's raw count against silver's deduplicated count directly (verified: bronze=16,
silver=15, difference of exactly 1 matching the one known duplicate) as an explicit pipeline check,
before gold is ever built from silver.
</details>

---

Check the boxes in [`PROGRESS.md`](../PROGRESS.md) and move on to Module 12 when it's built.
