
from pyspark.sql import SparkSession #type:ignore
from pymongo import MongoClient #type:ignore
import threading
import logging
from pyspark.sql.functions import col #type:ignore

class SparkDataPipelineManager:
    _instance = None  # Class variable to store the single instance
    _lock = threading.Lock()  # Lock for thread safety

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:  # Double-checked locking for thread safety
                    cls._instance = super(SparkDataPipelineManager, cls).__new__(
                        cls, *args, **kwargs)
        return cls._instance

    def __init__(self, mongo_uri="mongodb://mongodb:27017/", db_name="analytics_db"):
        if not hasattr(self, 'initialized'):
            self.mongo_uri = mongo_uri
            self.db_name = db_name
            self.collectionsMap = {}  # Dictionary to store DataFrames and schemas
            self.collection_watch_flags = {}  # Flags for stopping threads
            self.watch_threads = []  # List to keep track of threads
            self.spark = None
            self.mongo_client = MongoClient(self.mongo_uri)
            self.initialize_spark()
            self.initialized = True  # Mark as initialized

    def initialize_spark(self):
        """Initialize the Spark session."""
        try:
            self.spark = (SparkSession.builder
                          .appName("DistributedParkingAnalytics")
                          .master("spark://spark-master:7077")
                          .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0")
                          .config("spark.mongodb.read.connection.uri", f"{self.mongo_uri}{self.db_name}")
                          .getOrCreate())
            logging.info("Spark session initialized successfully.")
        except Exception as e:
            logging.error(f"Failed to initialize Spark session: {str(e)}")

    def _watch_collection_changes(self, collection, collection_name):
        """Watch MongoDB collection for changes and refresh cache on update."""
        try:
            with collection.watch() as change_stream:
                for change in change_stream:
                    if not self.collection_watch_flags.get(collection_name, False):
                        logging.info(
                            f"Stopped watching collection '{collection_name}'.")
                        break

                    if change['operationType'] in ['insert', 'update', 'replace', 'delete']:
                        logging.info(
                            f"Detected change in collection '{collection_name}', refreshing cache.")
                        self.set_data_frame(
                            collection_name, self.collectionsMap[collection_name]["schema"])
        except Exception as e:
            logging.error(
                f"Error watching collection '{collection_name}': {str(e)}")
    def get_data_frame(self, collection_name):
        """Retrieve a DataFrame from the collectionsMap and check if it exists."""
        df_entry = self.collectionsMap.get(collection_name)
        if df_entry is None:
            logging.warning(f"DataFrame for collection '{collection_name}' does not exist.")
            return None
        return df_entry["df"]
    def set_data_frame(self, collection_name, collection_schema):
        """Set a DataFrame for a given collection and store it in the collectionsMap."""
        try:
            collection_df = (self.spark.read
                             .format("mongodb")
                             .option("database", self.db_name)
                             .option("collection", collection_name)
                             .schema(collection_schema)
                             .load()
                             )  # Adjust filter condition as needed

            self.collectionsMap[collection_name] = {
                "df": collection_df, "schema": collection_schema
            }
            logging.info(f"DataFrame for collection '{collection_name}' has been set and stored.")
        except Exception as e:
            logging.error(f"Error setting DataFrame for collection '{collection_name}': {str(e)}")

    def add_new_collection(self, collection_name, collection_schema):
        """Add a new collection to collectionsMap and start watching it."""
        if collection_name not in self.collectionsMap:
            self.set_data_frame(collection_name, collection_schema)
            db = self.mongo_client[self.db_name]
            collection = db[collection_name]
            # Set the flag to True for watching
            self.collection_watch_flags[collection_name] = True
            watch_thread = threading.Thread(target=self._watch_collection_changes, args=(
                collection, collection_name), daemon=True)
            watch_thread.start()
            self.watch_threads.append(watch_thread)  # Keep track of the thread
            logging.info(
                f"Started watching new collection '{collection_name}'.")
        else:
            logging.warning(
                f"Collection '{collection_name}' is already being watched.")

    def remove_collection(self, collection_name):
        """Remove a collection from collectionsMap and stop watching it."""
        if collection_name in self.collectionsMap:
            # Signal the thread to stop
            self.collection_watch_flags[collection_name] = False
            del self.collectionsMap[collection_name]
            logging.info(
                f"Stopped watching and removed collection '{collection_name}'.")
        else:
            logging.warning(
                f"Collection '{collection_name}' is not being watched or does not exist.")

    def shutdown(self):
        """Clean up resources."""
        # Stop all collection watch threads
        for collection_name in self.collection_watch_flags.keys():
            # Signal all threads to stop
            self.collection_watch_flags[collection_name] = False
        logging.info("Signaled all threads to stop.")

        # Join all threads to ensure they close before the program exits
        for thread in self.watch_threads:
            thread.join()
        logging.info("All watch threads have been stopped.")

        if self.spark:
            self.spark.stop()
            logging.info("Spark session stopped.")
        self.mongo_client.close()
        logging.info("MongoDB client closed.")
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType, LongType, TimestampType

# DETECTIONS_SCHEMA = StructType([
#     StructField("ID", StringType(), True),
#     StructField("TYPE", IntegerType(), True),
#     StructField("PLATE_SOURCE", StringType(), True),
#     StructField("PLATE_REGION", StringType(), True),
#     StructField("PLATE_COLOR", StringType(), True),
#     StructField("PLATE_CODE", StringType(), True),
#     StructField("PLATE_NUMBER", StringType(), True),
#     StructField("PLATE_TYPE", StringType(), True),
#     StructField("CONFIDENCE_LEVEL", DoubleType(), True),
#     StructField("CONFIDENCE_LEVEL_PERCENTAGE", IntegerType(), True),
#     StructField("VEHICLE_MAKE", StringType(), True),
#     StructField("VEHICLE_MODEL", StringType(), True),
#     StructField("VEHICLE_COLOR", StringType(), True),
#     StructField("VEHICLE_TYPE", StringType(), True),
#     StructField("IMEI", StringType(), True),
#     StructField("CHANNEL", IntegerType(), True),
#     StructField("SPEED", IntegerType(), True),
#     StructField("HEADING", IntegerType(), True),
#     StructField("DETECTION_TIME", LongType(), True),
#     StructField("DETECTED_ID", StringType(), True),
#     StructField("IMAGES_COUNT", IntegerType(), True),
#     StructField("ASSET_ID", StringType(), True),
#     StructField("CREW_ID", StringType(), True),
#     StructField("TRIP_ID", StringType(), True),
#     StructField("ASSIGNED_OPERATOR", StringType(), True),
#     StructField("PARKING_TYPE", StringType(), True),
#     StructField("SECTOR_NAME", StringType(), True),
#     StructField("SECTOR_ID", IntegerType(), True),
#     StructField("LOT_ID", StringType(), True),
#     StructField("LOT_TYPE", StringType(), True),
#     StructField("LOT_TARRIF", StringType(), True),
#     StructField("COORDINATES", ArrayType(DoubleType()), True),
#     StructField("STATUS", IntegerType(), True),
#     StructField("CREATED_DATE", TimestampType(), True),
#     StructField("FINE_TYPE", IntegerType(), True),
#     StructField("FINE_ACTION", IntegerType(), True),
#     StructField("timestamp", TimestampType(), True),
#     StructField("DROP_DESCRIPTION", StringType(), True),
#     StructField("DROP_REASON", IntegerType(), True),
#     StructField("UPDATED_BY", StringType(), True)
# ])

# sm=SparkDataPipelineManager()
# sm.add_new_collection('DETECTIONS',DETECTIONS_SCHEMA)
# df=sm.get_data_frame("DETECTIONS")
# print(df)