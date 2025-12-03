# etl/spark_advanced.py

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def run_spark_advanced_example(data_dir: str) -> None:
    """
    Advanced Spark example on the synthetic dataset.

    Reads:
      - customers_extended.csv
      - products.csv
      - orders_header.csv
      - order_lines.csv

    Joins them and computes:
      - revenue before and after returns
      - estimated cost & margin
      - return rate
      - metrics per (shipping_country, shipping_city, segment)

    Writes an aggregated CSV to: <data_dir>/spark_city_segment_metrics/
    """

    spark = (
        SparkSession.builder
        .appName("SparkAdvancedExample")
        .getOrCreate()
    )

    customers_path = os.path.join(data_dir, "customers_extended.csv")
    products_path = os.path.join(data_dir, "products.csv")
    orders_path = os.path.join(data_dir, "orders_header.csv")
    lines_path = os.path.join(data_dir, "order_lines.csv")

    print("Reading source CSVs...")
    customers_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(customers_path)
    )

    products_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(products_path)
    )

    products_df = products_df.withColumnRenamed("category", "product_category")

    orders_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(orders_path)
    )

    lines_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(lines_path)
    )

    print("customers_df schema:")
    customers_df.printSchema()
    print("products_df schema:")
    products_df.printSchema()
    print("orders_df schema:")
    orders_df.printSchema()
    print("lines_df schema:")
    lines_df.printSchema()

    # -----------------------------
    # Join order lines + orders + customers + products
    # -----------------------------
    joined_df: DataFrame = (
        lines_df.alias("l")
        .join(orders_df.alias("o"), on="order_id", how="inner")
        .join(customers_df.alias("c"), on="customer_id", how="left")
        .join(products_df.alias("p"), on="product_id", how="left")
    )

    # -----------------------------
    # Derived columns for metrics
    # -----------------------------
    # NOTE: net_amount & refund_amount come from generator.
    #      We'll define:
    #        - line_revenue_net = net_amount
    #        - line_revenue_after_returns = net_amount - refund_amount
    #        - line_cost_estimate = base_price * quantity * 0.7 (fake cost)
    #        - line_margin_estimate = revenue_after_returns - cost_estimate
    #        - is_returned_int = 1/0 for aggregations
    # -----------------------------
    joined_with_metrics = (
        joined_df
        .withColumn("line_revenue_net", F.col("net_amount"))
        .withColumn("line_refund", F.col("refund_amount"))
        .withColumn(
            "line_revenue_after_returns",
            F.col("net_amount") - F.col("refund_amount"),
        )
        .withColumn(
            "line_cost_estimate",
            F.col("base_price") * F.col("quantity") * F.lit(0.7),
        )
        .withColumn(
            "line_margin_estimate",
            F.col("line_revenue_after_returns") - F.col("line_cost_estimate"),
        )
        .withColumn(
            "is_returned_int",
            F.when(F.col("is_returned") == True, F.lit(1)).otherwise(F.lit(0)),
        )
    )

    # -----------------------------
    # Aggregate: per (shipping_country, shipping_city, segment)
    # -----------------------------
    grouped = (
        joined_with_metrics
        .groupBy("shipping_country", "shipping_city", "segment")
        .agg(
            F.round(F.sum("line_revenue_net"), 2).alias("total_net"),
            F.round(F.sum("line_refund"), 2).alias("total_refund"),
            F.round(F.sum("line_revenue_after_returns"), 2).alias("total_net_after_returns"),
            F.round(F.sum("line_cost_estimate"), 2).alias("total_cost_estimate"),
            F.round(F.sum("line_margin_estimate"), 2).alias("total_margin_estimate"),
            F.count("*").alias("line_count"),
            F.sum("is_returned_int").alias("returned_line_count"),
        )
        .withColumn("return_rate_lines", F.round(F.col("returned_line_count") / F.col("line_count"), 4))
        .withColumn("margin_pct_estimate",
                    F.round(F.col("total_margin_estimate") / F.col("total_net_after_returns"), 4))
        .orderBy(F.col("total_net_after_returns").desc())
    )

    print("City + segment metrics (top 20):")
    grouped.show(20, truncate=False)

    # -----------------------------
    # Optional: per product category & country
    # -----------------------------
    category_metrics = (
        joined_with_metrics
        .groupBy("shipping_country", "product_category")
        .agg(
            F.sum("line_revenue_after_returns").alias("total_net_after_returns"),
            F.sum("line_margin_estimate").alias("total_margin_estimate"),
            F.count("*").alias("line_count"),
            F.sum("is_returned_int").alias("returned_line_count"),
        )
        .withColumn(
            "return_rate_lines",
            F.col("returned_line_count") / F.col("line_count"),
        )
        .orderBy(F.col("total_net_after_returns").desc())
    )

    print("Category metrics by country (top 20):")
    category_metrics.show(20, truncate=False)

    # -----------------------------
    # Write aggregated metrics to CSV
    # -----------------------------
    out_dir_city_segment = os.path.join(data_dir, "spark_city_segment_metrics")
    out_dir_category_country = os.path.join(data_dir, "spark_category_country_metrics")

    print(f"Writing city+segment metrics to: {out_dir_city_segment}")
    (
        grouped
        .coalesce(1)  # single CSV file for convenience (small data)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(out_dir_city_segment)
    )

    print(f"Writing category+country metrics to: {out_dir_category_country}")
    (
        category_metrics
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(out_dir_category_country)
    )

    spark.stop()
