## Incorrect transformations
### 1. Malformed dates and amounts
**Issue:** The pipeline fails with `CAST_INVALID_INPUT` and produces no report when processing input that contains an invalid date or amount. \
**Root cause:** `prepare_orders()` parses `order_date` with `F.to_date()` and `amount` with `cast("double")` without handling malformed values. As Spark 4 enables ANSI mode by default, invalid casts raise an exception instead of returning NULL. \
**Fix:** Parse with `try_to_date()` and `try_cast()`, and treat NULL results as ineligible.

### 2. Floating-point Revenue
**Issue:** Using floating-point arithmetic for revenue calculations could introduce precision errors. \
**Root cause:** Monetary values `amount` was cast to `double`, which cannot precisely represent many decimal values. \
**Fix:** Use `DecimalType(12,2)` for exact decimal arithmetic

### 3. Ineligible orders counted
**Issue:** Non-completed orders, orders with a missing `customer_id` or a negative `amount` were included. \
**Root cause:** No eligibility checks against the business rules before aggregation. \
**Fix:** Include only completed orders with nonempty `order_id` and `customer_id`, a valid `order_date`, and `amount > 0`. 

### 4. Conflicting duplicates double-counted
**Issue:** `1009` appears twice with different amounts, and both rows were counted in `revenue` and `order_count`. \
**Root cause:** `dropDuplicates()` removes only exact duplicate rows, and `count("order_id")` counts non-null values in the order_id column without deduplication. \
**Fix:** Collapse exact duplicates to one row; exclude `order_id`s with conflicting details and report them separately.

### Tests
**Issue:** The original test used a single valid row and only checked column names, so it passed despite every issue above. \
**Fix:** Added tests covering:
- A parametrized test for each ineligibility condition, with a valid control row to identify over-filtering.
- Exact duplicates are counted once.
- Conflicting orders are excluded and reported.
- Revenue uses exact decimal arithmetic. 
- End-to-end report on `data/orders.csv`.

## Tooling and CI
- Added `ruff` as the linter and formatter.
- Lint rules: pycodestyle (`E`), pyflakes (`F`), import sorting (`I`), bugbear (`B`), pyupgrade (`UP`).
- The GitHub Actions workflow runs `ruff check`, `ruff format --check`, and `pytest` on every push and pull request; any failing step fails the workflow.

## Assumptions
- `order_date` must be in `yyyy-MM-dd` format. Other formats (e.g. `2026/10/01`) are invalid.
- Amounts are in a single currency with at most two decimal places. Extra digits are rounded half-up when parsed, and amounts outside `decimal(12,2)` are treated as invalid.
- Conflict handling:
  - Rows identical in all fields are treated as one order.
  - If rows sharing an `order_id` differ in any field, all of them are excluded from revenue and reported. The data has no timestamp or version or other identifiers to identify the correct row, so no row is chosen.
  - Within the same `order_id`, a NULL and a non-NULL value in the same field count as a conflict
  - Conflicts are detected before the eligibility rules are applied, so an order with one valid and one invalid row is excluded as conflicting instead of keeping the valid row.
  - Rows without an `order_id` cannot be linked to an order, so they skip conflict detection and are removed by the eligibility rules.
- Days with no eligible orders are omitted from the report rather than shown as zero. 

## Outputs
- `output/daily_revenue`: one row per date with `order_date`, `revenue`, and `order_count`.
- `output/conflicting_orders`: every row of each conflicting order, with the parsed fields, the original `raw_amount` and `raw_order_date`, and `conflicting_columns`, which lists the fields that differ.

## What I would improve with more time
- Report rows rejected by the eligibility rules, with a reason (e.g. `invalid_amount`, `missing_customer_id`), so upstream teams can trace and fix bad data.
- Building on that and the conflict report, log per-run counts (input, duplicates, conflicts, rejected, reported) and check that they reconcile, so no row disappears silently.
- Test the pipeline entry point end to end (read CSV, write Parquet, read back)

## Unfinished work
- Reporting of rejected (ineligible) rows was designed but not implemented; only conflicting orders are reported.