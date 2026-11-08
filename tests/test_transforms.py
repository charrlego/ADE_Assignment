from order_pipeline.transforms import prepare_orders, daily_revenue
from datetime import date
from decimal import Decimal

def test_report_has_expected_columns(spark):
    raw = spark.createDataFrame(
        [("1", "C1", "2026-10-01", "10.00", "completed")],
        ["order_id", "customer_id", "order_date", "amount", "status"],
    )
    result = daily_revenue(prepare_orders(raw))
    assert set(result.columns) == {"order_date", "revenue", "order_count"}

def make_orders(spark, rows):
    return spark.createDataFrame(rows, ["order_id", "customer_id", "order_date", "amount", "status"])


# Add tests that capture business rules and edge cases.
def test_valid_values_are_parsed(spark):
    raw = make_orders(spark, [("1", "C1", " 2026-10-01 ", " 120.50 ", "completed")])
    row = prepare_orders(raw).collect()[0]
    assert row.order_date == date(2026, 10, 1)
    assert row.amount == Decimal("120.50")


def test_malformed_amount_becomes_null(spark):
    raw = make_orders(spark, [("1", "C1", "2026-10-01", "not-a-number", "completed")])
    row = prepare_orders(raw).collect()[0]
    assert row.amount is None
    assert row.raw_amount == "not-a-number"


def test_malformed_date_becomes_null(spark):
    raw = make_orders(spark, [("1", "C1", "not-a-date", "10.00", "completed")])
    row = prepare_orders(raw).collect()[0]
    assert row.order_date is None
    assert row.raw_order_date == "not-a-date"


def test_date_with_unexpected_format_is_rejected(spark):
    raw = make_orders(spark, [("1", "C1", "2026/10/01", "10.00", "completed")])
    assert prepare_orders(raw).collect()[0].order_date is None


def test_malformed_rows_are_excluded_from_report(spark):
    raw = make_orders(spark, [
        ("1", "C1", "2026-10-01", "10.00", "completed"),
        ("2", "C2", "2026-10-01", "20.00", "completed"),
        ("3", "C3", "not-a-date", "99.00", "completed"),
        ("4", "C4", "2026-10-01", "not-a-number", "completed"),
    ])
    rows = daily_revenue(prepare_orders(raw)).collect()
    assert len(rows) == 1
    assert rows[0].order_date == date(2026, 10, 1)
    assert rows[0].revenue == Decimal("30.00")
    assert rows[0].order_count == 2