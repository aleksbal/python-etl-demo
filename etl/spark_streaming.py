# conceptual example only, not executed normally
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window

def run_spark_streaming_example():
    spark = (
        SparkSession.builder
        .appName("SparkStreamingExample")
        .getOrCreate()
    )

    stream_df = (
        spark.readStream
        .option("header", True)
        .option("inferSchema", True)
        .csv("data/stream_input")
    )

    aggregated = (
        stream_df
        .withWatermark("timestamp", "10 minutes")
        .groupBy(
            window(col("timestamp"), "5 minutes"),
            col("country"),
        )
        .sum("value")
        .withColumnRenamed("sum(value)", "value_sum")
    )

    query = (
        aggregated.writeStream
        .outputMode("update")
        .format("console")
        .start()
    )

    query.awaitTermination()

