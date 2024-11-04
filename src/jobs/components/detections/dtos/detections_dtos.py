
from typing import List
from pydantic import BaseModel  # type: ignore


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
