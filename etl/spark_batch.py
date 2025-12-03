# etl/spark_batch.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr


def run_spark_batch_example(sales_path: str, customers_path: str) -> None:
    """
    Demonstrates:
    - SparkSession & DataFrame
    - Lazy transformations (select, withColumn, filter, groupBy)
    - Join sales + customers to get country
    """

    spark = (
        SparkSession.builder
        .appName("SparkBatchExample")
        .getOrCreate()
    )

    # --- read CSVs as DataFrames ---
    sales_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(sales_path)
    )

    customers_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(customers_path)
    )

    # join on customer_id to get country
    merged_df = (
        sales_df.alias("s")
        .join(
            customers_df.select("customer_id", "country").alias("c"),
            on="customer_id",
            how="left",
        )
    )

    transformed = (
        merged_df
        .select("customer_id", "country", "price", "quantity", "product")
        .withColumn("revenue", col("price") * col("quantity"))
        .filter(col("revenue") > 0)#
    )

    # aggregate: revenue per country
    revenue_by_country = (
        transformed
        .groupBy("country")
        .agg(expr("sum(revenue) as total_revenue"))
        .orderBy(col("total_revenue").desc())
    )

    print("Revenue by country (Spark):")
    revenue_by_country.show(truncate=False)

    # maybe also revenue per customer (nice for charts)
    revenue_by_customer = (
        transformed
        .groupBy("customer_id", "country")
        .agg(expr("sum(revenue) as total_revenue"))
        .orderBy(col("total_revenue").desc())
    )

    print("Top customers (Spark):")
    revenue_by_customer.show(10, truncate=False)

    spark.stop()

