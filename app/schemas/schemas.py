from typing import Optional, Literal, List
from pydantic import BaseModel, Field
from datetime import datetime

class IncidentSchema(BaseModel):
    source: Literal["gas", "ayto", "ide", "canal"] = Field(description="Source system that reported the incident")
    category: Literal["gas", "electricity", "water", "road_works", "road"] = Field(description="Type of the incident")
    status: Literal["planned", "active", "unplanned"] = Field(description="Current status of the incident")
    city: str = Field(description="City where the incident occurred")
    street: Optional[str] = Field(None, description="Street name where the incident is located")
    street_number: Optional[str] = Field(None, description="Street number of the incident location")
    lat: Optional[float] = Field(description="Latitude coordinate of the incident")
    lon: Optional[float] = Field(description="Longitude coordinate of the incident")
    start_ts_utc: Optional[datetime] = Field(None, description="Start timestamp of the incident in UTC")
    end_ts_utc: Optional[datetime] = Field(None, description="End timestamp of the incident in UTC")
    description: Optional[str] = Field(None, description="Detailed description of the incident")
    event_id: Optional[str] = Field(None, description="Unique event identifier from the source system")
    ingested_at_utc: datetime = Field(description="Timestamp when the incident was ingested into the system (UTC)")
    fingerprint: str = Field(description="Unique fingerprint/hash identifier for the incident")


    model_config = {"from_attributes": True}