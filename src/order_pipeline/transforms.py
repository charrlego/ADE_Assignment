"""Order transformations. Review correctness and test coverage."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def prepare_orders(orders: DataFrame) -> DataFrame:
    """Prepare raw orders for reporting."""
    return (
        orders
        .withColumn("raw_amount", F.col("amount"))
        .withColumn("amount", F.trim("amount").try_cast("decimal(12,2)"))
        .withColumn("raw_order_date", F.col("order_date"))
        .withColumn("order_date", F.try_to_date(F.trim(F.col("order_date")), "yyyy-MM-dd"))
        .dropDuplicates()
    )

def filter_eligible(orders: DataFrame) -> DataFrame:
    return orders.where(
        (F.col("status") == "completed")
        & (F.trim(F.col("order_id")) != "")
        & (F.trim(F.col("customer_id")) != "")
        & (F.col("order_date").isNotNull())
        & (F.col("amount") > 0)
    )

def daily_revenue(orders: DataFrame) -> DataFrame:
    """Calculate daily completed-order revenue."""
    return (
        filter_eligible(orders).groupBy("order_date")
        .agg(
            F.round(F.sum("amount"), 2).alias("revenue"),
            F.count("order_id").alias("order_count"),
        )
        .orderBy("order_date")
    )
