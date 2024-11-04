from typing import List, Optional
from fastapi import HTTPException, APIRouter, Query, Depends  # type:ignore
import logging
from .service import DetectionService
from ..utils.spark_mongo_manager import SparkDataPipelineManager

router = APIRouter()


def get_data_service() -> DetectionService:
    data_manager = SparkDataPipelineManager()  # Singleton instance
    return DetectionService(data_manager)


service = get_data_service()


@router.get("/fine-report")
def get_fine_report(
    sector_name: Optional[str] = Query(
        None, min_length=3, max_length=50, description="Name of the sector"),
    start_date: Optional[int] = Query(
        None, ge=0, description="Start date as an integer (timestamp)"),
    end_date: Optional[int] = Query(
        None, ge=0, description="End date as an integer (timestamp)"),
    fine_type: Optional[int] = Query(
        None, ge=0, description="Type of the fine"),
):
    try:
        queryFilter = {
            "sector_name": sector_name,
            "start_date": start_date,
            "end_date": end_date,
            "fine_type": fine_type
        }
        queryFilter = {key: value for key,
                       value in queryFilter.items() if value is not None}
        return service.get_fine_report_charts_data(queryFilter)
    except Exception as e:
        logging.error(f"Error in get_analytics: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.get("/master-report")
def get_master_report():
    try:
        return service.get_master_report_charts_data()
    except Exception as e:
        logging.error(f"Error in get_analytics: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Internal Server Error: {str(e)}")
