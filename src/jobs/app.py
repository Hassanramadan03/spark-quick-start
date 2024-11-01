from fastapi import FastAPI, HTTPException, Query
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType, LongType, TimestampType
from typing import List, Optional
from pydantic import BaseModel
from pymongo import MongoClient
import json
from pyspark.sql.functions import (
    from_json, col, collect_list, concat_ws, current_date,
    count, sum, when, explode, array, lit
)
import json
from datetime import datetime
import socket
import time
import os

import threading
import logging
import time
import uvicorn
import os
# MongoDB URI
# MONGO_URI = "mongodb+srv://eslam:sb9hufbS1ds8z52v@cluster0.0zicw.mongodb.net/analytics_db?retryWrites=true&w=majority&appName=Cluster0"
MONGO_URI = "mongodb://mongodb:27017/analytics_db"

DETECTIONS_SCHEMA = StructType([
    StructField("ID", StringType(), True),
    StructField("TYPE", IntegerType(), True),
    StructField("PLATE_SOURCE", StringType(), True),
    StructField("PLATE_REGION", StringType(), True),
    StructField("PLATE_COLOR", StringType(), True),
    StructField("PLATE_CODE", StringType(), True),
    StructField("PLATE_NUMBER", StringType(), True),
    StructField("PLATE_TYPE", StringType(), True),
    StructField("CONFIDENCE_LEVEL", DoubleType(), True),
    StructField("CONFIDENCE_LEVEL_PERCENTAGE", IntegerType(), True),
    StructField("VEHICLE_MAKE", StringType(), True),
    StructField("VEHICLE_MODEL", StringType(), True),
    StructField("VEHICLE_COLOR", StringType(), True),
    StructField("VEHICLE_TYPE", StringType(), True),
    StructField("IMEI", StringType(), True),
    StructField("CHANNEL", IntegerType(), True),
    StructField("SPEED", IntegerType(), True),
    StructField("HEADING", IntegerType(), True),
    StructField("DETECTION_TIME", LongType(), True),
    StructField("DETECTED_ID", StringType(), True),
    StructField("IMAGES_COUNT", IntegerType(), True),
    StructField("ASSET_ID", StringType(), True),
    StructField("CREW_ID", StringType(), True),
    StructField("TRIP_ID", StringType(), True),
    StructField("ASSIGNED_OPERATOR", StringType(), True),
    StructField("PARKING_TYPE", StringType(), True),
    StructField("SECTOR_NAME", StringType(), True),
    StructField("SECTOR_ID", IntegerType(), True),
    StructField("LOT_ID", StringType(), True),
    StructField("LOT_TYPE", StringType(), True),
    StructField("LOT_TARRIF", StringType(), True),
    StructField("COORDINATES", ArrayType(DoubleType()), True),
    StructField("STATUS", IntegerType(), True),
    StructField("CREATED_DATE", TimestampType(), True),
    StructField("FINE_TYPE", IntegerType(), True),
    StructField("FINE_ACTION", IntegerType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("DROP_DESCRIPTION", StringType(), True),
    StructField("DROP_REASON", IntegerType(), True),
    StructField("UPDATED_BY", StringType(), True)
])

# Pydantic models


class SectorCount(BaseModel):
    name: str
    value: int


class Count(BaseModel):
    value: int
    id: int


class FineCounts(BaseModel):
    toratalViolationCount: Count
    illegalParkingViolationCount: Count
    notPaidViolationCount: Count
    notAuthorizedViolationCount: Count
    overStayViolationCount: Count


class AnalyticsResponse(BaseModel):
    fineCounts: FineCounts
    sectors: List[SectorCount]


# Constants for fine types
FINE_TYPES = {
    "TicketPermitNotDisplayedClearly": 1,
    "InvalidTicketOrPermit": 2,
    "OverStay": 5,
    "illegalParking": 88,
}

# Schema for detections
DETECTION_SCHEMA = StructType([
    StructField("FINE_TYPE", IntegerType(), True),
    StructField("TYPE", IntegerType(), True),
    StructField("SECTOR_NAME", StringType(), True),
    StructField("DETECTION_TIME", IntegerType(), True)
])

# SparkManager class to manage the Spark session and cache


# def create_spark_session():
#     """Create a properly configured Spark session with MongoDB connector"""
#     # Updated package version and configurations
#     return SparkSession.builder \
#         .appName("MasterReportGenerator") \
#         .master("spark://spark-master:7077") \
#         .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0") \
#         .config("spark.mongodb.read.connection.uri", f"{MONGO_URI}analytics_db") \
#         .config("spark.mongodb.input.uri", MONGO_URI) \
#         .config("spark.mongodb.output.uri", MONGO_URI) \
#         .getOrCreate()


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
        self.cached_df.createOrReplaceTempView("DETECTIONS")
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


# class DetectionsAnalytics:
#     """Class to handle all analytics operations on the detections data"""

#     def __init__(self, dataframe):
#         self.df = dataframe.cache()  # Cache the dataframe for better performance

#     def apply_filters(self, query, filter_dict):
#         """Helper method to apply filters to a query"""
#         if not filter_dict:
#             return query
#         filtered_query = query
#         for key, value in filter_dict.items():
#             if isinstance(value, dict) and "$in" in value:
#                 filtered_query = filtered_query.filter(
#                     col(key).isin(value["$in"]))
#             else:
#                 filtered_query = filtered_query.filter(col(key) == value)
#         return filtered_query

#     def get_transaction_count(self, filter_dict=None):
#         """Get total transaction count with optional filters"""
#         try:
#             query = self.df
#             filtered = self.apply_filters(query, filter_dict)
#             return filtered.count()
#         except Exception as e:
#             print(f"Error in get_transaction_count: {str(e)}")
#             return 0

#     def get_sectors_count(self, filter_dict=None):
#         """Get count of unique sectors for fines"""
#         try:
#             query = self.df.filter(col("TYPE").isin([6, 7]))
#             filtered = self.apply_filters(query, filter_dict)
#             return filtered.select("SECTOR_NAME").distinct().count()
#         except Exception as e:
#             print(f"Error in get_sectors_count: {str(e)}")
#             return 0

#     def get_sector_fine_distribution(self, filter_dict=None):
#         """Get distribution of fines across sectors"""
#         try:
#             query = self.df.filter(col("TYPE").isin([6, 7]))
#             filtered = self.apply_filters(query, filter_dict)

#             distribution = filtered.groupBy("SECTOR_NAME") \
#                 .agg(count("*").alias("value")) \
#                 .select(col("SECTOR_NAME").alias("name"), "value")

#             return [row.asDict() for row in distribution.collect()]
#         except Exception as e:
#             print(f"Error in get_sector_fine_distribution: {str(e)}")
#             return []

#     def get_transactions_type_distribution(self, filter_dict=None):
#         """Get distribution of transaction types"""
#         try:
#             base_query = self.df
#             if filter_dict:
#                 type_filter = filter_dict.pop("TYPE", None)
#                 base_query = self.apply_filters(base_query, filter_dict)
#                 if type_filter:
#                     filter_dict["TYPE"] = type_filter

#             result = []
#             base_query = base_query.cache()  # Cache for multiple operations

#             # Calculate counts based on filters
#             if not filter_dict or ("TYPE" in filter_dict and 1 in filter_dict["TYPE"].get("$in", [])):
#                 paid_count = base_query.filter(col("TYPE") == 1).count()
#                 result.append({"name": "PaidTransaction", "value": paid_count})

#             if not filter_dict or ("TYPE" in filter_dict and 5 in filter_dict["TYPE"].get("$in", [])):
#                 dropped_count = base_query.filter(col("TYPE") == 5).count()
#                 result.append({"name": "dropped", "value": dropped_count})

#             if not filter_dict or ("TYPE" in filter_dict and any(t in [6, 7] for t in filter_dict["TYPE"].get("$in", []))):
#                 fine_count = base_query.filter(
#                     col("TYPE").isin([6, 7])).count()
#                 result.append({"name": "fine", "value": fine_count})

#             base_query.unpersist()  # Clean up cache
#             return result
#         except Exception as e:
#             print(f"Error in get_transactions_type_distribution: {str(e)}")
#             return []

#     def get_dropping_reasons_distribution(self, filter_dict=None):
#         """Get distribution of dropping reasons"""
#         try:
#             query = self.df.filter(col("TYPE") == 5)
#             if filter_dict:
#                 type_filter = filter_dict.pop("TYPE", None)
#                 query = self.apply_filters(query, filter_dict)

#             distribution = query.groupBy("DROP_REASON") \
#                 .agg(count("*").alias("value")) \
#                 .select(col("DROP_REASON").alias("name"), "value")

#             return [row.asDict() for row in distribution.collect()]
#         except Exception as e:
#             print(f"Error in get_dropping_reasons_distribution: {str(e)}")
#             return []

#     def get_master_report_charts_data(self, filter_dict=None):
#         """Get all charts data in a single function call"""
#         try:
#             # Cache the filtered dataframe if filters are applied
#             working_df = self.apply_filters(
#                 self.df, filter_dict).cache() if filter_dict else self.df

#             results = {
#                 "transactionCount": self.get_transaction_count(filter_dict),
#                 "sectors": self.get_sectors_count(filter_dict),
#                 "sectorsFineDistribution": self.get_sector_fine_distribution(filter_dict),
#                 "transactionTypesDistribution": self.get_transactions_type_distribution(filter_dict),
#                 "droppedTransactionDistribution": self.get_dropping_reasons_distribution(filter_dict),
#                 "manualReviewedTransactionCount": working_df.filter(col("PLATE_SOURCE") == "MANUAL_VALIDATION").count()
#             }

#             # Add derived metrics
#             transaction_types = results["transactionTypesDistribution"]
#             results["finesCount"] = next(
#                 (item["value"] for item in transaction_types if item["name"] == "fine"), 0)
#             results["droppedCount"] = next(
#                 (item["value"] for item in transaction_types if item["name"] == "dropped"), 0)

#             # Clean up cache
#             if filter_dict:
#                 working_df.unpersist()

#             return results
#         except Exception as e:
#             print(f"Error in get_master_report_charts_data: {str(e)}")
#             return {
#                 "transactionCount": 0,
#                 "sectors": 0,
#                 "sectorsFineDistribution": [],
#                 "transactionTypesDistribution": [],
#                 "droppedTransactionDistribution": [],
#                 "manualReviewedTransactionCount": 0,
#                 "finesCount": 0,
#                 "droppedCount": 0
#             }

def get_fine_report_charts_data( ):
        """Get all charts data in a single function call"""
        illegal_parking_count = df.filter( col("FINE_TYPE") == FINE_TYPES["illegalParking"]).count()
        not_paid_count = df.filter(
            col("FINE_TYPE") == FINE_TYPES["TicketPermitNotDisplayedClearly"]).count()
        not_authorized_count = df.filter(
            col("FINE_TYPE") == FINE_TYPES["InvalidTicketOrPermit"]).count()
        over_stay_count = df.filter(
            col("FINE_TYPE") == FINE_TYPES["OverStay"]).count()
        total_count =df.count()

        # Build fine counts result
        fine_counts = FineCounts(
            toratalViolationCount=Count(value=total_count, id=0),
            illegalParkingViolationCount=Count(
                value=illegal_parking_count, id=FINE_TYPES["illegalParking"]),
            notPaidViolationCount=Count(
                value=not_paid_count, id=FINE_TYPES["TicketPermitNotDisplayedClearly"]),
            notAuthorizedViolationCount=Count(
                value=not_authorized_count, id=FINE_TYPES["InvalidTicketOrPermit"]),
            overStayViolationCount=Count(
                value=over_stay_count, id=FINE_TYPES["OverStay"])
        )

        # Aggregate by sectors
        sector_counts_df = df.groupBy("SECTOR_NAME").count()
        sectors = [
            SectorCount(name=row["SECTOR_NAME"]
                        or "Unknown", value=row["count"])
            for row in sector_counts_df.collect()
        ]

        return AnalyticsResponse(
            fineCounts=fine_counts,
            sectors=sectors
        )


# Initialize SparkManager
spark = SparkManager()
df=spark.cached_df
# Initialize FastAPI app
app = FastAPI()
# df.createOrReplaceTempView("DETECTIONS")
# analytics = DetectionsAnalytics(df)

@app.get("/analytics", response_model=AnalyticsResponse)
async def get_analytics(
    sector_name: Optional[str] = Query(None),
    start_date: Optional[int] = Query(None),
    end_date: Optional[int] = Query(None),
    fine_type: Optional[int] = Query(None)
):
    try:
        # Apply filters
        # conditions = []
        # if sector_name:
        #     conditions.append(col("SECTOR_NAME") == sector_name)
        # if start_date:
        #     conditions.append(col("DETECTION_TIME") >= start_date)
        # if end_date:
        #     conditions.append(col("DETECTION_TIME") <= end_date)
        # if fine_type:
        #     conditions.append(col("FINE_TYPE") == fine_type)

        return get_fine_report_charts_data()
        # Count specific fine types

    except Exception as e:
        logging.error(f"Error in get_analytics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/master_report")
async def get_master_report():
    try:
        filter_obj = {
            "SECTOR_NAME": "AL RIGGA",
            "TYPE": {"$in": [6, 7]}
        }
        return "analytics.get_master_report_charts_data(filter_obj)"

    except Exception as e:
        logging.error(f"Error in get_analytics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


def test_mongodb_connection(host, port, max_retries=3, delay=5):
    """Test if MongoDB is accessible"""
    for attempt in range(max_retries):
        try:
            client = MongoClient(host, port, serverSelectionTimeoutMS=3000)
            client.server_info()
            client.close()
            return True
        except Exception as e:
            print(
                f"Attempt {attempt + 1}: MongoDB connection failed: {str(e)}")
            if attempt < max_retries - 1:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
    return False


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
