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