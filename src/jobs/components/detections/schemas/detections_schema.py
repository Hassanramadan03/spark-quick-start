from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType, LongType, TimestampType

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
