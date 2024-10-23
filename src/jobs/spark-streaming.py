from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, collect_list, concat_ws, current_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,TimestampType
def write_to_mongo(batch_df, batch_id):
    """Write each batch to MongoDB, grouping by crewId, assetId, and today's date."""
    
    # Group by crewId, assetId, and today's date, collect time into an array
    grouped_df = batch_df.groupBy("crewId", "assetId", current_date().alias("today")) \
                         .agg(collect_list("time").alias("times"))  # Collect times into an array
    
    # Write the grouped data to MongoDB
    grouped_df.write \
        .format("mongodb") \
        .mode("append") \
        .option("uri", "mongodb+srv://eslam:sb9hufbS1ds8z52v@cluster0.0zicw.mongodb.net/analytics_db") \
        .option("database", "analytics_db") \
        .option("collection", "detections") \
        .save()

def write_to_mongo(batch_df, batch_id):
    """Write each batch to MongoDB, updating records by concatenated_id."""
    
    # Create a concatenated ID (crewId_assetId)
    batch_df = batch_df.withColumn("concatenated_id", concat_ws("_", col("crewId"), col("assetId"), current_date().alias("today")))
    
    # Group by concatenated_id and today's date, collect time into an array
    grouped_df = batch_df.groupBy("concatenated_id", current_date().alias("today")) \
                         .agg(collect_list("time").alias("times"))  # Collect times into an array
    
    # Convert each row into JSON format for MongoDB update
    for row in grouped_df.collect():
        document = {
            "concatenated_id": row["concatenated_id"],
            "today": row["today"].strftime('%Y-%m-%d'),
            "times": row["times"]
        }
        # Update MongoDB, push times into the array
        mongo_collection.update_one(
            {"concatenated_id": document["concatenated_id"], "today": document["today"]},
            {"$push": {"times": {"$each": document["times"]}}},
            upsert=True  # Create a new document if not exists
        )
def start_streaming(spark):
    schema = StructType([
        StructField("imei", StringType(), True),
        StructField("time", StringType(), True),  # Assuming time is in string format
        StructField("status", IntegerType(), True),
        StructField("assetId", StringType(), True),
        StructField("crewId", StringType(), True),
        StructField("timestamp", TimestampType(), True)  # Use TimestampType if needed
    ])

    # Read stream from Kafka topic "detections_topic"
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

    # Write aggregated data to MongoDB
    query = (parsed_df.writeStream
             .outputMode("update")
             .foreachBatch(lambda batch_df, batch_id: write_to_mongo(batch_df, batch_id))
             .start())

    query.awaitTermination()

if __name__ == "__main__":
    from pymongo import MongoClient
    
    # MongoDB client
    mongo_client = MongoClient("mongodb+srv://eslam:sb9hufbS1ds8z52v@cluster0.0zicw.mongodb.net")
    mongo_db = mongo_client["analytics_db"]
    mongo_collection = mongo_db["detections"]
    # Initialize Spark session with MongoDB configuration
    spark_conn = SparkSession.builder \
        .appName("KafkaStreamConsumer") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0") \
        .config("spark.mongodb.write.connection.uri", "mongodb+srv://eslam:sb9hufbS1ds8z52v@cluster0.0zicw.mongodb.net/analytics_db") \
        .getOrCreate()

    start_streaming(spark_conn)
