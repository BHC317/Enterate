
from typing import List
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from core.security import require_api_key
from models.models import RouteRequest, RouteAnalysisResponse
from models.models import Coordinate
from models.models import Incident
from schemas.schemas import IncidentSchema
from services.logistics import LogisticsService

'''
Functionality with Logistics API
'''

router = APIRouter(tags=["logistics"], prefix="/routes")

class NearbyIncidentsResponse(BaseModel):
    count: int
    incidents: List[IncidentSchema]

@router.post("/analyze", response_model=RouteAnalysisResponse, tags=["logistics"])
async def analyze_route(payload: RouteRequest, _=Depends(require_api_key)):
    return LogisticsService.analyze_route(payload)

@router.get("/incidents/nearby", response_model=NearbyIncidentsResponse, tags=["logistics"])
async def get_nearby_incidents(
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
    radius_m: int = Query(1000, ge=50, le=10000),
    category: list[str] | None = Query(None),
    since: str | None = Query(None),
    _=Depends(require_api_key),
):
    center = Coordinate(lat=lat, lng=lng)
    incidents: list[Incident] = LogisticsService.nearby_incidents(center, radius_m, category, since)
    return {"count": len(incidents), "incidents": incidents}
