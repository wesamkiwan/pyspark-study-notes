# Lesson 4 — Sorting, Deduplication, and Combining DataFrames

The mechanics of `orderBy`, `distinct`, and `dropDuplicates` are simple. The one genuinely
dangerous trap in this lesson is `union` — it fails silently, and the way it fails is worse than
an error would be.

## Sorting: `orderBy`/`sort`, and where nulls land

`orderBy` and `sort` are aliases. Use `.desc()`/`.asc()` on a column for direction, and
`.asc_nulls_first()`/`.asc_nulls_last()` (or the `desc_` equivalents) when you need to control
where nulls land explicitly rather than relying on the default:

```python
employees.orderBy(col("salary").asc_nulls_first()).select("name", "salary")
```

```
+------------+-------+
|        name| salary|
+------------+-------+
| Mona Farouk|   NULL|
|Liam O'Brien|59000.0|
|  Hassan Ali|61000.0|
+------------+-------+
only showing top 3 rows
```

(Ascending order puts nulls first by default in Spark SQL, matching standard SQL behavior — but
don't rely on an unstated default when it matters; say `asc_nulls_first`/`asc_nulls_last`
explicitly if a null's position is meaningful to a downstream consumer.)

## `distinct()` and `dropDuplicates`

`distinct()` compares **entire rows**. `dropDuplicates(subset=[...])` compares only the columns
you name, keeping one arbitrary row per distinct combination of those columns:

```python
orders.select("product", "category").distinct().orderBy("product")
```

```
+--------+-----------+
| product|   category|
+--------+-----------+
|Gadget X|Electronics|
|Gadget Y|Electronics|
|Widget A|   Hardware|
|Widget B|   Hardware|
|Widget C|   Hardware|
+--------+-----------+
```

`orders.dropDuplicates(["product"])` would do something related but different: one row *per
product*, keeping all of that row's other original columns (order_id, amount, etc. from
whichever specific order Spark happens to pick) — useful when you want a representative full row,
not just the distinct combination of a couple of columns.

## `union`: matches columns by **position**, not by name

This is the one gotcha in this lesson worth internalizing completely, because it doesn't error —
it just silently scrambles your data. Imagine combining two order feeds where one arrives with its
columns in a different order (a very real scenario — a partner feed, or a schema that got
reordered upstream):

```python
west = orders.filter(col("region") == "West").select("order_id", "product", "amount")
east = orders.filter(col("region") == "East").select("amount", "order_id", "product")  # reordered!

combined = west.union(east)
```

Verified — this runs with **no error at all**:

```python
combined.columns
# ['order_id', 'product', 'amount']   <- takes column NAMES from `west` only
```

But look what happens when you try to use it — filtering for the four known East order IDs
(`1003, 1006, 1010, 1015`) against the `order_id` column of the combined result:

```python
combined.filter(col("order_id").isin(1003, 1006, 1010, 1015)).show()
```

```
+--------+-------+------+
|order_id|product|amount|
+--------+-------+------+
+--------+-------+------+
```

**Zero rows.** `union()` combined the two DataFrames purely by column *position*: `west`'s 1st
column (`order_id`) lines up with `east`'s 1st column (`amount`), `west`'s 2nd (`product`) lines up
with `east`'s 2nd (`order_id`), and so on. For every row that came from `east`, the value sitting
in the column *labeled* `order_id` is actually that row's `amount` — the real order IDs (1003, 1006,
etc.) are now mislabeled as `product`. The filter above isn't wrong; the data underneath the label
is. This is exactly the kind of bug that produces a quietly-empty or quietly-scrambled downstream
report with no exception anywhere to point at the cause.

**The fix: `unionByName`**, which aligns columns by name instead of position:

```python
combined = west.unionByName(east)
combined.filter(col("order_id").isin(1003, 1006, 1010, 1015)).show()
```

```
+--------+--------+------+
|order_id| product|amount|
+--------+--------+------+
|    1003|Widget A| 250.0|
|    1006|Gadget Y|1200.0|
|    1010|Widget C|150.75|
|    1015|Gadget X| 899.99|
+--------+--------+------+
```

**The rule: default to `unionByName` over `union` whenever the two DataFrames weren't built by the
exact same `.select(...)` call in the exact same order** — which, in practice, is most real
pipeline code combining data from more than one source. `unionByName(allowMissingColumns=True)`
will also fill in nulls for columns present on one side but missing on the other, if you need that
flexibility; the plain `union()` requires identical column counts and (positionally) compatible
types on both sides.

---
**Next:** [Lesson 5 — DataFrame API vs Spark SQL](05-dataframe-api-vs-sql.md)
