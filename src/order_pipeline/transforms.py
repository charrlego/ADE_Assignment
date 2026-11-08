"""Order transformations. Turn raw orders into the daily revenue report."""

from pyspark.sql import Column, DataFrame, Window, WindowSpec
from pyspark.sql import functions as F

COLUMNS = ["order_id", "customer_id", "order_date", "amount", "status"]
DATE_FORMAT = "yyyy-MM-dd"
REVENUE_STATUS = "completed"
AMOUNT_TYPE = "decimal(12,2)"


def prepare_orders(orders: DataFrame) -> DataFrame:
    """Prepare raw orders for reporting."""
    return (
        orders.withColumn("raw_amount", F.col("amount"))
        .withColumn("amount", F.trim("amount").try_cast(AMOUNT_TYPE))
        .withColumn("raw_order_date", F.col("order_date"))
        .withColumn(
            "order_date", F.try_to_date(F.trim(F.col("order_date")), DATE_FORMAT)
        )
    )


def filter_eligible(orders: DataFrame) -> DataFrame:
    """Filter orders eligible for reporting."""
    return orders.where(
        (F.col("status") == REVENUE_STATUS)
        & (F.trim(F.col("order_id")) != "")
        & (F.trim(F.col("customer_id")) != "")
        & (F.col("order_date").isNotNull())
        & (F.col("amount") > 0)
    )


def flag_conflict(column: str, window: WindowSpec) -> Column:
    """Flag conflicting values including NULL, within an order."""
    distinct_values = F.size(F.collect_set(F.struct(column)).over(window))
    return F.when(distinct_values > 1, F.lit(column))


def resolve_duplicates(orders: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Resolve duplicate orders by flagging conflicts and
    separating resolved and conflicting rows."""

    # Drop exact duplicates before checking for conflicts.
    deduplicated = orders.dropDuplicates(COLUMNS)

    has_order_id = F.coalesce(F.length(F.trim("order_id")) > 0, F.lit(False))
    by_order = Window.partitionBy("order_id")
    compared_columns = [c for c in COLUMNS if c != "order_id"]
    flagged = deduplicated.where(has_order_id).withColumn(
        "conflicting_columns",
        F.array_compact(
            F.array(*[flag_conflict(c, by_order) for c in compared_columns])
        ),
    )

    conflicts = flagged.where(F.size("conflicting_columns") > 0)
    resolved = (
        flagged.where(F.size("conflicting_columns") == 0)
        .drop("conflicting_columns")
        .unionByName(deduplicated.where(~has_order_id))
    )
    return resolved, conflicts


def daily_revenue(orders: DataFrame) -> DataFrame:
    """Calculate daily revenue."""
    return (
        orders.groupBy("order_date")
        .agg(
            F.sum("amount").alias("revenue"),
            F.count("order_id").alias("order_count"),
        )
        .orderBy("order_date")
    )


def build_daily_report(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Build daily report and conflicts report from raw orders."""
    orders, conflicts = resolve_duplicates(prepare_orders(raw))
    return daily_revenue(filter_eligible(orders)), conflicts
