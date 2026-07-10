# Module 02 Quiz

Answer each yourself before expanding the answer.

---

**1. Why does `inferSchema=True` cost more than just "convenience risk" — what does it actually
cost performance-wise, and why?**

<details>
<summary>Answer</summary>

For schema-less formats (CSV, JSON), Spark can't know column types from the file layout the way
it can with Parquet. `inferSchema=True` forces an extra full pass over the data to sample values
and guess types, *before* the real read even happens — you pay to scan the data twice.
</details>

---

**2. A ZIP code column contains `00501`. With `inferSchema=True`, what does Spark likely do to it,
and why is this dangerous?**

<details>
<summary>Answer</summary>

Verified directly in Lesson 1: Spark infers it as `IntegerType`, and integers don't have leading
zeros, so `00501` silently becomes `501` — permanently, with no error or warning. It's dangerous
because nothing in the pipeline flags this; it only surfaces later as mysteriously
wrong/inconsistent downstream data.
</details>

---

**3. Why can a columnar format (Parquet) skip reading a column you didn't ask for, while CSV
cannot?**

<details>
<summary>Answer</summary>

Columnar formats physically store each column's values together on disk. A reader can seek to
and read only the bytes for the requested column(s). CSV stores full rows, with all fields
interleaved — to get at any one field, the reader has to read and parse the entire row anyway.
</details>

---

**4. You're handed a JSON file and `spark.read.json(path)` produces a DataFrame with a single
`_corrupt_record` column and one "row" per line of the file. What's actually wrong, most likely?**

<details>
<summary>Answer</summary>

The file is a pretty-printed, multi-line JSON array (or objects spanning multiple lines), but
you read it without `.option("multiLine", "true")`. Spark's default JSON reader expects one
complete JSON object per line; against a pretty-printed file, every individual line fails to
parse as JSON on its own, so everything lands in `_corrupt_record`.
</details>

---

**5. Under `PERMISSIVE` mode (the default), a row with a type mismatch doesn't raise an error —
what happens to the bad field, and why is that risky if you don't also capture `_corrupt_record`?**

<details>
<summary>Answer</summary>

The offending field becomes `null`. It's risky without `_corrupt_record` because a null caused
by bad source data becomes indistinguishable from a null that was always legitimately there —
you lose the ability to tell "this was actually missing" from "this failed to parse."
</details>

---

**6. You read data with a `_corrupt_record` column, filter to the clean rows, and call
`clean_df.count()`. Is this number trustworthy? What did testing this directly reveal?**

<details>
<summary>Answer</summary>

No — verified directly: `.count()` (and even `.agg(count("*"))`) can return the *total* row
count including rows that were filtered out, once a corrupt-record column is involved anywhere
upstream, because Spark's optimizer can take a fast-path shortcut that skips actually applying
the filter. The fix isn't changing which counting function you call — it's forcing real
materialization first, e.g. `.cache()` or `.localCheckpoint()`, before counting.
</details>

---

**7. You read two Parquet directories together — one written before a `salary` column was added
upstream, one written after — with no special options. What happens to `salary`, and why is this
worse than an error?**

<details>
<summary>Answer</summary>

Verified: nothing errors. Spark picks one file's schema (whichever it encounters) and applies it
to the whole read, silently dropping `salary` from **every** row — including rows from files
that genuinely have it on disk. It's worse than an error because the read "succeeds," so nothing
alerts you that a column quietly vanished.
</details>

---

**8. What does `mergeSchema=true` actually cost, and why isn't it simply always turned on by
default?**

<details>
<summary>Answer</summary>

It requires Spark to read the schema (metadata) of every file being unioned to compute the full
union of columns, rather than trusting one file's schema — a real, extra metadata-scanning cost
on every read. It isn't the default because most reads aren't spanning a schema change, and
paying that cost unconditionally would slow down the common case for a benefit that usually
isn't needed.
</details>

---

**9. Why does `coalesce(1)` before a write generally cost less than `repartition(1)` for the same
goal (one output file), and when could `coalesce` still produce a poor result?**

<details>
<summary>Answer</summary>

`coalesce` only combines existing partitions without a full shuffle where possible, while
`repartition` always performs a full shuffle to redistribute data evenly. `coalesce` can still
produce a poor (very uneven, or all-on-one-executor) result if the source partitions were
already very unevenly sized, since it doesn't rebalance data the way a shuffle would.
</details>

---

Check the boxes in [`PROGRESS.md`](../PROGRESS.md) and move on to Module 03 when it's built.
