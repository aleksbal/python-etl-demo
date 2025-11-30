from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

def run_spark_batch_example(csv_path: str) -> None:
    spark = (
        SparkSession.builder
        .appName("SparkBatchExample")
        .getOrCreate()
    )

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(csv_path)
    )

    transformed = (
        df
        .select("customer_id", "price", "quantity", "country")
        .withColumn("revenue", col("price") * col("quantity"))
        .filter(col("revenue") > 0)
    )

    revenue_by_country = (
        transformed
        .groupBy("country")
        .agg(expr("sum(revenue) as total_revenue"))
        .orderBy(col("total_revenue").desc())
    )

    revenue_by_country.show()
    spark.stop()

