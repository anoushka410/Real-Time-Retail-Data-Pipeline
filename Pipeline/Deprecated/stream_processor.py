# PHASE 1: Real-Time Data Pipeline (Python + Kafka + Spark)

# --- Step 2: Spark Structured Streaming Consumer ---
# This script reads from Kafka topic and processes the stream in real time.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import json
import os

# Configure Hadoop for Windows
os.environ["HADOOP_HOME"] = "D:/hadoop"
os.environ["HADOOP_USER_NAME"] = "user"  # Add this line
os.environ["PATH"] = f"{os.environ['HADOOP_HOME']}/bin;{os.environ['PATH']}"

# Define schema for incoming data
schema = StructType([
    StructField("InvoiceNo", StringType()),
    StructField("StockCode", StringType()),
    StructField("Description", StringType()),
    StructField("Quantity", IntegerType()),
    StructField("InvoiceDate", StringType()),
    StructField("UnitPrice", DoubleType()),
    StructField("CustomerID", StringType()),
    StructField("Country", StringType())
])

spark = SparkSession.builder \
    .appName("OnlineRetailStreamProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

# 🔧 (Optional but recommended) Reduce noisy logging
spark.sparkContext.setLogLevel("ERROR")

try:
    # Read data from Kafka
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "online_retail") \
        .option("startingOffsets", "earliest") \
        .load()

    # Extract JSON and apply schema
    df = raw_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Clean and transform
    df_clean = df.dropna(subset=["CustomerID", "InvoiceDate"]) \
        .dropDuplicates(["InvoiceNo", "StockCode"]) \
        .withColumn("InvoiceDate", to_timestamp("InvoiceDate", "yyyy-MM-dd HH:mm:ss"))
    
    # Write to PostgreSQL
    def write_to_postgres(batch_df, batch_id):
        try:
            batch_df.write \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://localhost:5432/real_time_db") \
                .option("dbtable", "online_retail") \
                .option("user", "postgres") \
                .option("password", "postgresql") \
                .mode("append") \
                .save()
        except Exception as e:
            print(f"Error writing batch {batch_id} to PostgreSQL: {str(e)}")


    query = df_clean.writeStream \
            .outputMode("append") \
            .format("console") \
            .start()
    
    # query.stop()  # <-- Important to avoid leaks!

    query = df_clean.writeStream \
            .format("memory") \
            .queryName("temp_debug_table") \
            .start()

    # Now query it like a static table
    spark.sql("SELECT * FROM temp_debug_table").show()  # <-- Now show() works!
    query.stop()  # Clean up
    
    query = df_clean.writeStream \
            .foreachBatch(write_to_postgres) \
            .outputMode("append") \
            .option("checkpointLocation", "file:///C:/spark_checkpoints") \
            .start()
    
    query.awaitTermination()

except Exception as e:
    print(f"Streaming error: {str(e)}")
    spark.stop()

# # Save cleaned data to a CSV file
# df_clean.writeStream \
#     .outputMode("append") \
#     .format("csv") \
#     .option("header", True) \
#     .option("path", "output/cleaned_csv") \
#     .option("checkpointLocation", "output/checkpoints") \
#     .start() \
#     .awaitTermination()


# Write to PostgreSQL (batch-like append)
# df_clean.writeStream \
#     .foreachBatch(lambda batch_df, batch_id: batch_df.write \
#         .format("jdbc") \
#         .option("url", "jdbc:postgresql://localhost:5432/real_time_db") \
#         .option("dbtable", "online_retail") \
#         .option("user", "root") \
#         .option("password", "root") \
#         .mode("append") \
#         .save()) \
#     .outputMode("append") \
#     .option("checkpointLocation", "file:///C:/spark_output/pg_checkpoints") \
#     .start() \
#     .awaitTermination()
