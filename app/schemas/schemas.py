from typing import Optional, Literal, List
from pydantic import BaseModel, Field, validator
from datetime import datetime

class IncidentSchema(BaseModel):
    source: str = Field(description="Source system that reported the incident")
    category: str = Field(description="Type of the incident")
    status: str = Field(description="Current status of the incident")
    #status: Literal["planned", "active", "unplanned"] = Field(description="Current status of the incident")
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

class IncidentCreate(BaseModel):
    source: str = Field(..., max_length=50)
    category: str = Field(..., max_length=50)
    status: str = Field(description="Current status of the incident")
    #status: Literal["planned", "active", "unplanned"] = Field(description="Current status of the incident")
    city: str = Field(..., max_length=100)
    street: Optional[str] = Field(None, max_length=1000)
    street_number: Optional[str] = Field(None, max_length=20)
    lat: Optional[float] = None
    lon: Optional[float] = None
    start_ts_utc: datetime
    end_ts_utc: Optional[datetime] = None
    description: Optional[str] = None
    event_id: Optional[str] = Field(None, max_length=100)

    # Asegurar datetimes con TZ. Si vienen naïve, se asumen UTC.
    @validator("start_ts_utc", "end_ts_utc", pre=True)
    def ensure_tz(cls, v):
        if v is None:
            return v
        if isinstance(v, str):
            dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
        else:
            dt = v
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
