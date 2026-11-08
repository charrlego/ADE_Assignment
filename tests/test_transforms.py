from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql.types import DecimalType
from order_pipeline.transforms import (
    build_daily_report,
    filter_eligible,
    prepare_orders,
    resolve_duplicates,
)

RAW_SCHEMA = "order_id string, customer_id string, order_date string, amount string, status string"
SAMPLE_CSV = Path(__file__).resolve().parents[1] / "data" / "orders.csv"
VALID_ORDER = {
    "order_id": "1",
    "customer_id": "C1",
    "order_date": "2026-10-01",
    "amount": "10.00",
    "status": "completed",
}

def make_orders(spark, rows):
    return spark.createDataFrame(rows, RAW_SCHEMA)

def conflicting_columns_by_order(conflicts):
    return {row.order_id: row.conflicting_columns for row in conflicts.collect()}

@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("status", "cancelled"),
        ("status", None),
        ("order_id", "   "),
        ("order_id", None),
        ("customer_id", "   "),
        ("customer_id", None),
        ("order_date", None),
        ("order_date", "not-a-date"),
        ("order_date", "2026/10/01"),
        ("amount", None),
        ("amount", "not-a-number"),
        ("amount", "0"),
        ("amount", "-10.00"),
    ],
)
def test_ineligible_order_is_excluded(spark, field, value):
    invalid = {**VALID_ORDER, "order_id": "2", field: value}
    raw = make_orders(spark, [VALID_ORDER, invalid])
    eligible = filter_eligible(prepare_orders(raw)).collect()
    assert [row.order_id for row in eligible] == ["1"]

def test_exact_duplicates_counted_once(spark):
    raw = make_orders(spark, [VALID_ORDER, VALID_ORDER])
    resolved, conflicts = resolve_duplicates(prepare_orders(raw))
    assert resolved.count() == 1
    assert conflicts.count() == 0

def test_conflicting_order_is_excluded_and_reported(spark):
    raw = make_orders(spark, [
        {**VALID_ORDER, "amount": "200.00"},
        {**VALID_ORDER, "amount": "220.00"},
    ])
    resolved, conflicts = resolve_duplicates(prepare_orders(raw))
    assert resolved.count() == 0
    assert conflicts.count() == 2
    assert conflicting_columns_by_order(conflicts) == {"1": ["amount"]}

def test_missing_value_counts_as_conflict(spark):
    raw = make_orders(spark, [VALID_ORDER, {**VALID_ORDER, "customer_id": None}])
    _, conflicts = resolve_duplicates(prepare_orders(raw))
    assert conflicting_columns_by_order(conflicts) == {"1": ["customer_id"]}

def test_rows_without_order_id_are_not_identified_as_conflicts(spark):
    raw = make_orders(spark, [
        {**VALID_ORDER, "order_id": None, "customer_id": "C1"},
        {**VALID_ORDER, "order_id": None, "customer_id": "C2"},
    ])
    resolved, conflicts = resolve_duplicates(prepare_orders(raw))
    assert conflicts.count() == 0
    assert resolved.count() == 2

def test_revenue_uses_exact_decimal_arithmetic(spark):
    raw = make_orders(spark, [
        {**VALID_ORDER, "order_id": "1", "amount": "0.10"},
        {**VALID_ORDER, "order_id": "2", "amount": "0.20"},
    ])
    report, _ = build_daily_report(raw)
    assert isinstance(report.schema["revenue"].dataType, DecimalType)
    assert report.collect()[0].revenue == Decimal("0.30")

def test_sample_data_report(spark):
    raw = spark.read.option("header", True).csv(str(SAMPLE_CSV))
    report, conflicts = build_daily_report(raw)
    assert report.columns == ["order_date", "revenue", "order_count"]

    actual = [tuple(row) for row in report.orderBy("order_date").collect()]
    assert actual == [
        (date(2026, 10, 1), Decimal("205.50"), 2),
        (date(2026, 10, 2), Decimal("40.00"), 1),
    ]
    assert conflicting_columns_by_order(conflicts) == {"1009": ["amount"]}