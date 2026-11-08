"""Run with: python -m order_pipeline.pipeline"""
from pathlib import Path
from pyspark.sql import SparkSession
from order_pipeline.transforms import build_daily_report


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("associate-order-assignment")
        .getOrCreate()
    )
    try:
        raw = spark.read.option("header", True).csv(str(root / "data" / "orders.csv"))
        result, conflicts = build_daily_report(raw)
        result.show(truncate=False)
        output = root / "output"
        result.write.mode("overwrite").parquet(str(output / "daily_revenue"))

        conflicts.write.mode("overwrite").parquet(str(output / "conflicting_orders"))
        conflict_count = conflicts.select("order_id").distinct().count()
        print(f"Found {conflict_count} conflicting orders; see {output / 'conflicting_orders'}")
        
        print(f"Saved revenuereport to {output / 'daily_revenue'}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
