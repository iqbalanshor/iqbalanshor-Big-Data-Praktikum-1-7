# module_7_spark_consumer.py
# Big Data Practicum - Module 7: Spark Structured Streaming Consumer
# Environment: Google Colab (Requires Kafka & Spark Setup)

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)

# Ensure the SparkSession includes the Kafka package
# Note: In Colab, you might need to download the jar manually or use --packages if using spark-submit.
# Since we are likely in a notebook, we configure it in the builder.
# Make sure to match the Scala version (usually 2.12) and Spark version (3.5.0)

spark_version = "3.5.0"
scala_version = "2.12"
kafka_package = f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}"

print(f"Initializing SparkSession with package: {kafka_package}")

spark = (
    SparkSession.builder.appName("Module7_KafkaConsumer")
    .config("spark.jars.packages", kafka_package)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Schema for the JSON data coming from Kafka
# {"transaction_id": 1, "product": "Mouse", "price": 20, "quantity": 3, "timestamp": "..."}
schema = StructType(
    [
        StructField("transaction_id", IntegerType(), True),
        StructField("product", StringType(), True),
        StructField("price", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField(
            "timestamp", StringType(), True
        ),  # We will cast this later if needed
    ]
)

# 1. Read Stream from Kafka
print("Reading stream from Kafka topic 'transaksi-toko'...")
df_kafka = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "transaksi-toko")
    .option("startingOffsets", "latest")
    .load()
)

# 2. Parse JSON Data
# Kafka value is binary, need to cast to string then parse
df_parsed = (
    df_kafka.selectExpr("CAST(value AS STRING) as json_string")
    .select(from_json(col("json_string"), schema).alias("data"))
    .select("data.*")
)

# 3. Processing
# Calculate Revenue = price * quantity
df_processed = df_parsed.withColumn("revenue", col("price") * col("quantity"))

# 4. Aggregation: Total Sales by Product
# We will use 'complete' output mode for aggregation queries on non-windowed streams (if just global grouping)
# OR 'update' mode.
df_summary = df_processed.groupBy("product").agg(
    expr("sum(quantity) as total_quantity"), expr("sum(revenue) as total_revenue")
)

# 5. Output: Write to Memory Table (for querying in notebook)
# For aggregation, outputMode can be 'complete' (whole table every time) or 'update' (only changed rows)
query = (
    df_summary.writeStream.outputMode("complete")
    .format("memory")
    .queryName("sales_summary")
    .start()
)

print("Streaming started... Waiting for data...")

try:
    # Run loop to query the memory table periodically
    # In a real app, you might use awaitTermination()
    import time

    for i in range(10):
        time.sleep(5)
        print(f"\n--- Batch {i+1} Results ---")
        if spark.catalog.tableExists("sales_summary"):
            result = spark.sql(
                "SELECT * FROM sales_summary ORDER BY total_revenue DESC"
            )
            result.show()
        else:
            print("Table not ready yet.")

    # Stop for demonstration purposes (remove if you want it to run forever)
    # query.awaitTermination() # Use this in production script

except KeyboardInterrupt:
    print("Stopping stream...")

finally:
    query.stop()
    spark.stop()
