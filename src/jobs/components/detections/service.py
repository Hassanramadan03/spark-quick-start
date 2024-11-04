from .schemas.fine_types_schema import (FINE_TYPES)
from .schemas.detections_schema import (DETECTIONS_SCHEMA)
from pyspark.sql.functions import (col)  # type: ignore
from .dtos.detections_dtos import (
    FineCounts,
    Count,
    SectorCount,
    AnalyticsResponse)
from ..utils.spark_mongo_manager import SparkDataPipelineManager
from pyspark.sql.functions import (   # type: ignore
    from_json, col, collect_list, concat_ws, current_date,
    count, sum, when, explode, array, lit
)
detection_collection = "DETECTIONS"


class DetectionService:
    def __init__(self,  data_manager: SparkDataPipelineManager):
        data_manager.add_new_collection(
            detection_collection, DETECTIONS_SCHEMA)
        self.df = data_manager.get_data_frame(detection_collection)
        self.fine_df = self.df .filter(col("TYPE").isin(4, 5))
        self.master_df = self.df

    def get_fine_report_charts_data(self, query):
        """Get all charts data in a single function call"""
        illegal_parking_count = self.df.filter(
            col("FINE_TYPE") == FINE_TYPES["illegalParking"]).count()
        not_paid_count = self.df.filter(
            col("FINE_TYPE") == FINE_TYPES["TicketPermitNotDisplayedClearly"]).count()
        not_authorized_count = self.df.filter(
            col("FINE_TYPE") == FINE_TYPES["InvalidTicketOrPermit"]).count()
        over_stay_count = self.df.filter(
            col("FINE_TYPE") == FINE_TYPES["OverStay"]).count()
        total_count = self.df.count()

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
        sector_counts_df = self.df.groupBy("SECTOR_NAME").count()
        sectors = [
            SectorCount(name=row["SECTOR_NAME"]
                        or "Unknown", value=row["count"])
            for row in sector_counts_df.collect()
        ]

        return AnalyticsResponse(
            fineCounts=fine_counts,
            sectors=sectors
        )

    def apply_filters(self, query, filter_dict):
        """Helper method to apply filters to a query"""
        if not filter_dict:
            return query
        filtered_query = query
        for key, value in filter_dict.items():
            if isinstance(value, dict) and "$in" in value:
                filtered_query = filtered_query.filter(
                    col(key).isin(value["$in"]))
            else:
                filtered_query = filtered_query.filter(col(key) == value)
        return filtered_query

    def get_transaction_count(self, filter_dict=None):
        """Get total transaction count with optional filters"""
        try:
            query = self.df
            filtered = self.apply_filters(query, filter_dict)
            return filtered.count()
        except Exception as e:
            print(f"Error in get_transaction_count: {str(e)}")
            return 0

    def get_sectors_count(self, filter_dict=None):
        """Get count of unique sectors for fines"""
        try:
            query = self.df.filter(col("TYPE").isin([6, 7]))
            filtered = self.apply_filters(query, filter_dict)
            return filtered.select("SECTOR_NAME").distinct().count()
        except Exception as e:
            print(f"Error in get_sectors_count: {str(e)}")
            return 0

    def get_sector_fine_distribution(self, filter_dict=None):
        """Get distribution of fines across sectors"""
        try:
            query = self.df.filter(col("TYPE").isin([6, 7]))
            filtered = self.apply_filters(query, filter_dict)

            distribution = filtered.groupBy("SECTOR_NAME") \
                .agg(count("*").alias("value")) \
                .select(col("SECTOR_NAME").alias("name"), "value")

            return [row.asDict() for row in distribution.collect()]
        except Exception as e:
            print(f"Error in get_sector_fine_distribution: {str(e)}")
            return []

    def get_transactions_type_distribution(self, filter_dict=None):
        """Get distribution of transaction types"""
        try:
            base_query = self.df
            if filter_dict:
                type_filter = filter_dict.pop("TYPE", None)
                base_query = self.apply_filters(base_query, filter_dict)
                if type_filter:
                    filter_dict["TYPE"] = type_filter

            result = []
            base_query = base_query.cache()  # Cache for multiple operations

            # Calculate counts based on filters
            if not filter_dict or ("TYPE" in filter_dict and 1 in filter_dict["TYPE"].get("$in", [])):
                paid_count = base_query.filter(col("TYPE") == 1).count()
                result.append({"name": "PaidTransaction", "value": paid_count})

            if not filter_dict or ("TYPE" in filter_dict and 5 in filter_dict["TYPE"].get("$in", [])):
                dropped_count = base_query.filter(col("TYPE") == 5).count()
                result.append({"name": "dropped", "value": dropped_count})

            if not filter_dict or ("TYPE" in filter_dict and any(t in [6, 7] for t in filter_dict["TYPE"].get("$in", []))):
                fine_count = base_query.filter(
                    col("TYPE").isin([6, 7])).count()
                result.append({"name": "fine", "value": fine_count})

            base_query.unpersist()  # Clean up cache
            return result
        except Exception as e:
            print(f"Error in get_transactions_type_distribution: {str(e)}")
            return []

    def get_dropping_reasons_distribution(self, filter_dict=None):
        """Get distribution of dropping reasons"""
        try:
            query = self.df.filter(col("TYPE") == 5)
            if filter_dict:
                type_filter = filter_dict.pop("TYPE", None)
                query = self.apply_filters(query, filter_dict)

            distribution = query.groupBy("DROP_REASON") \
                .agg(count("*").alias("value")) \
                .select(col("DROP_REASON").alias("name"), "value")

            return [row.asDict() for row in distribution.collect()]
        except Exception as e:
            print(f"Error in get_dropping_reasons_distribution: {str(e)}")
            return []

    def get_master_report_charts_data(self, filter_dict=None):
        """Get all charts data in a single function call"""
        try:
            # Cache the filtered dataframe if filters are applied
            working_df = self.apply_filters(
                self.df, filter_dict).cache() if filter_dict else self.df

            results = {
                "transactionCount": self.get_transaction_count(filter_dict),
                "sectors": self.get_sectors_count(filter_dict),
                "sectorsFineDistribution": self.get_sector_fine_distribution(filter_dict),
                "transactionTypesDistribution": self.get_transactions_type_distribution(filter_dict),
                "droppedTransactionDistribution": self.get_dropping_reasons_distribution(filter_dict),
                "manualReviewedTransactionCount": working_df.filter(col("PLATE_SOURCE") == "MANUAL_VALIDATION").count()
            }

            # Add derived metrics
            transaction_types = results["transactionTypesDistribution"]
            results["finesCount"] = next(
                (item["value"] for item in transaction_types if item["name"] == "fine"), 0)
            results["droppedCount"] = next(
                (item["value"] for item in transaction_types if item["name"] == "dropped"), 0)

            # Clean up cache
            if filter_dict:
                working_df.unpersist()

            return results
        except Exception as e:
            print(f"Error in get_master_report_charts_data: {str(e)}")
            return {
                "transactionCount": 0,
                "sectors": 0,
                "sectorsFineDistribution": [],
                "transactionTypesDistribution": [],
                "droppedTransactionDistribution": [],
                "manualReviewedTransactionCount": 0,
                "finesCount": 0,
                "droppedCount": 0
            }


 