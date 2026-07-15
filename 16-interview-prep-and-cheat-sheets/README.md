# Module 16 — Interview Prep & Cheat Sheets

The final module — no new mechanics, just organizing everything Modules 00-15 already verified
into a form you can scan fast: before an interview, before a real production incident, or whenever
you need the quick answer instead of re-deriving it. Every fact in this module traces back to
something actually run and verified earlier in this course, not generic interview-prep boilerplate.

## Contents

1. [Quick Reference Cheat Sheets](01-quick-reference-cheat-sheets.md) — scannable tables per topic
   area: architecture, I/O, joins, partitioning, window functions, UDFs, performance, streaming,
   Delta Lake, testing, production.
2. [Verified Gotchas — The Master List](02-verified-gotchas-master-list.md) — every "here's how
   this silently bites you" finding from the whole course, in one place, organized by theme
   (silent wrong answers, laziness/recompute traps, defaults that surprise). This is the single
   most interview-relevant page in the whole course — these are exactly the "tell me about a time
   you debugged something subtle" stories.
3. [Interview Questions](03-interview-questions.md) — organized fundamentals → intermediate →
   advanced → system-design-style, each answer grounded in a specific verified fact from this
   course rather than a textbook definition.

## How to use this module

- **Before an interview:** skim the master gotchas list end to end — it's short enough to read in
  15-20 minutes and covers the "gotcha" questions that separate someone who's read the docs from
  someone who's actually run the code.
- **During prep, spaced out over days:** work through `03-interview-questions.md` the same way you
  worked through every module's `quiz.md` — answer yourself before expanding.
- **On the job, as a working reference:** the cheat sheet tables in `01-quick-reference-cheat-sheets.md`
  are meant to be genuinely faster to scan than re-opening the relevant module's lessons.

---

You've now built a complete PySpark curriculum from zero to production-ready, verified end to end
against real code at every step — 00 through 16. Good luck.
