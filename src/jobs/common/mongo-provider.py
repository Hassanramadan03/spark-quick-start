
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pymongo import MongoClient
import threading
import logging

MONGO_URI = "mongodb://mongodb:27017/analytics_db"
class SparkManager:
    def __init__(self):
        self.spark = None
        self.cached_df = None
        self.mongo_client = MongoClient(MONGO_URI)
        self.initialize_spark()
        self.start_mongo_watch()

    def initialize_spark(self):
        """Initialize the Spark session."""
        self.spark = SparkSession.builder \
            .appName("DistributedParkingAnalytics") \
            .master("spark://spark-master:7077") \
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0") \
            .config("spark.mongodb.read.connection.uri", f"{MONGO_URI}analytics_db") \
            .getOrCreate()
        self.refresh_cache()

    def refresh_cache(self):
        """Refresh the DataFrame cache from MongoDB."""
        self.cached_df = self.spark.read \
            .format("mongodb") \
            .option("database", "analytics_db") \
            .option("collection", "DETECTIONS") \
            .schema(DETECTION_SCHEMA) \
            .load() \
            .filter(col("TYPE").isin(4, 5))  # Assuming `4` and `5` represent autoFine and manualFine

    def start_mongo_watch(self):
        """Start a MongoDB change stream to watch for changes and refresh cache."""
        db = self.mongo_client.analytics_db
        collection = db.DETECTIONS
        threading.Thread(target=self._watch_collection_changes,
                         args=(collection,)).start()

    def _watch_collection_changes(self, collection):
        """Watch MongoDB collection for changes and refresh cache on update."""
        try:
            with collection.watch() as change_stream:
                for change in change_stream:
                    logging.info("Detected MongoDB change, refreshing cache.")
                    self.refresh_cache()
        except Exception as e:
            logging.error(f"Error in MongoDB watch: {str(e)}")

    def get_cached_dataframe(self):
        """Retrieve the current cached DataFrame."""
        return self.cached_df

    def shutdown(self):
        """Clean up resources."""
        if self.spark:
            self.spark.stop()
        self.mongo_client.close()

