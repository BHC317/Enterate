from sqlalchemy import TIMESTAMP, CheckConstraint, Column, Float, Integer, String, Text
from database import Base
from pydantic import BaseModel, Field, conlist, validator, field_validator
from typing import Literal, List, Optional
from datetime import datetime, timezone
from schemas.schemas import IncidentSchema

class Customer(Base):
    __tablename__ = "customers"
    __table_args__ = {"schema": "public"}
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

class Incident(Base):
    __tablename__ = "fct_events"
    __table_args__ = (
        # CheckConstraint("source IN ('gas', 'ayto', 'ide', 'canal')"),
        CheckConstraint("status IN ('planned', 'active', 'unplanned')"),
        # CheckConstraint("category IN ('gas', 'electricity', 'water', 'road_works', 'road')"),
        {"schema": "analytics_analytics"},
    )

    fingerprint = Column(String(100), primary_key=True, index=True)
    source = Column(String(10), nullable=False)
    category = Column(String(15), nullable=False)
    status = Column(String(10), nullable=False)
    city = Column(String(100), nullable=False)
    street = Column(String(255), nullable=True)
    street_number = Column(String(20), nullable=True)
    lat = Column(Float, nullable=True)
    lon = Column(Float, nullable=True)
    start_ts_utc = Column(TIMESTAMP(timezone=True), nullable=False)
    end_ts_utc = Column(TIMESTAMP(timezone=True), nullable=True)
    description = Column(Text, nullable=True)
    event_id = Column(String(100), nullable=True)
    ingested_at_utc = Column(TIMESTAMP(timezone=True), nullable=False)

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
    incidents: List[IncidentSchema]
    affected_segments: List[AffectedSegment]
    recommendations: List[Recommendation]
    alternatives: List[AlternativeRoute]