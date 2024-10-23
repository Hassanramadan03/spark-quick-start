from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, from_json, sum as spark_sum, expr, when, countDistinct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import json
from datetime import datetime

 
 

 
 
def write_to_mongo(batch_df):
    """Write each batch to MongoDB."""
    batch_df.write \
        .format("mongodb") \
        .mode("append") \
        .option("uri", "mongodb+srv://eslam:sb9hufbS1ds8z52v@cluster0.0zicw.mongodb.net/analytics_db") \
        .option("database", "analytics_db") \
        .option("collection", "detections") \
        .save()
 
def start_streaming(spark):
    schema = StructType([
        StructField("imei", StringType(), True),
        # StructField("coordinates", StructType([
        #     StructField("lat", DoubleType(), True),
        #     StructField("lng", DoubleType(), True)
        # ]), True),
        StructField("time", StringType(), True),
        StructField("status", IntegerType(), True),
        StructField("assetId", StringType(), True),
        StructField("crewId", StringType(), True),
        StructField("timestamp", StringType(), True)  # Use TimestampType if needed
        ])

    # Read stream from Kafka topic "TRANSACTIONSTOPIC"
    stream_df = (spark.readStream
                 .format("kafka")
                 .option("kafka.bootstrap.servers", "kafka:9092")
                 .option("subscribe", "detections_topic")
                 .option("startingOffsets", "latest")
                 .load())

    # Parse the JSON message
    parsed_df = stream_df.selectExpr("CAST(value AS STRING)") \
                         .select(from_json(col("value"), schema).alias("data")) \
                         .select("data.*")

    # Write aggregated data to MongoDB and Kafka RESULT topic
    query = (parsed_df.writeStream
             .outputMode("update")
             .foreachBatch(lambda batch_df, batch_id: (write_to_mongo(batch_df) ))
             .start())

    query.awaitTermination()

if __name__ == "__main__":
    # Initialize Spark session with MongoDB configuration
    spark_conn = SparkSession.builder \
        .appName("KafkaStreamConsumer") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0") \
        .config("spark.mongodb.write.connection.uri", "mongodb+srv://eslam:sb9hufbS1ds8z52v@cluster0.0zicw.mongodb.net/analytics_db") \
        .getOrCreate()
    start_streaming(spark_conn)
