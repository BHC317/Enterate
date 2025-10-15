from sqlalchemy import Column, Integer, String
from database import Base
from pydantic import BaseModel, Field, conlist, validator, field_validator
from typing import Literal, List, Optional
from datetime import datetime

class Customer(Base):
    __tablename__ = "customers"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(120), nullable=False)
    email = Column(String(200), unique=True, index=True, nullable=False)

# CLASES EJMPLO LOGISTICA
class Coordinate(BaseModel):
    lat: float = Field(..., ge=-90, le=90)
    lng: float = Field(..., ge=-180, le=180)

class RoutePoint(Coordinate):
    t: datetime | None = Field(None, description="Timestamp estimado en ese punto (opcional)")

class VehicleInfo(BaseModel):
    type: Literal["motorcycle", "car", "van", "truck", "bus"] = "truck"
    length_m: float | None = Field(None, ge=0)
    width_m: float | None = Field(None, ge=0)
    height_m: float | None = Field(None, ge=0)

class Incident(BaseModel):
    id: str
    type: Literal[
    "accident", "roadwork", "protest", "flood", "power_outage",
    "police_activity", "event", "traffic_jam", "roadblock"
    ]
    title: str
    description: str | None = None
    location: Coordinate
    radius_m: int = 150
    severity: Literal["low", "medium", "high", "critical"] = "medium"
    start_time: datetime | None = None
    end_time: datetime | None = None
    source: Literal["official", "citizen", "media", "sensor"] = "official"

class Recommendation(BaseModel):
    id: str
    kind: Literal["avoid_segment", "reroute", "time_shift", "speed_advice", "cargo_note"]
    text: str

class AffectedSegment(BaseModel):
    start_index: int
    end_index: int
    distance_m: int
    reason_incident_id: str | None = None


class AlternativeRoute(BaseModel):
    polyline: List[Coordinate]
    added_distance_m: int
    added_time_min: int
    confidence: float = Field(0.8, ge=0, le=1)


class RouteRequest(BaseModel):
    route: List[RoutePoint] = Field(..., min_length=2) 
    vehicle: Optional[VehicleInfo] = None
    depart_at: Optional[datetime] = None
    consider_window_min: int = Field(90, ge=0, le=1440, description="Minutos hacia adelante a considerar")


    @field_validator("depart_at", mode="before")
    @classmethod
    def default_depart_now(cls, v):
        return v or datetime.utcnow()


class RouteAnalysisResponse(BaseModel):
    has_incidents: bool
    total_risk_score: float = Field(..., ge=0, le=1)
    eta_min: int
    eta_with_incidents_min: int
    expected_delay_min: int
    incidents: List[Incident]
    affected_segments: List[AffectedSegment]
    recommendations: List[Recommendation]
    alternatives: List[AlternativeRoute]