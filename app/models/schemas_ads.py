# schemas_ads.py
from pydantic import BaseModel, Field, HttpUrl, EmailStr
from datetime import datetime, date
from typing import Optional, Literal

AdStatus = Literal["draft", "active", "paused", "archived"]

# ---- Planes
class PlanCreate(BaseModel):
    name: str = Field(..., examples=["BÁSICO"])
    monthly_price_usd: float = Field(..., ge=0)
    impressions_quota: int = Field(..., ge=0)

class PlanOut(PlanCreate):
    id: int

# ---- Usuarios (anunciantes)
class AdvertiserCreate(BaseModel):
    name: str
    email: EmailStr
    plan_id: Optional[int] = None

class AdvertiserOut(BaseModel):
    id: int
    name: str
    email: EmailStr
    plan_id: Optional[int]
    created_at: datetime

# ---- Anuncios
class AdCreate(BaseModel):
    user_id: int
    title: str
    media_url: HttpUrl
    target_url: HttpUrl
    status: AdStatus = "draft"

class AdOut(BaseModel):
    id: int
    user_id: int
    title: str
    media_url: HttpUrl
    target_url: HttpUrl
    status: AdStatus
    created_at: datetime
    total_impressions: int
    total_clicks: int

class ServeAdOut(BaseModel):
    ad_id: int
    title: str
    media_url: HttpUrl
    target_url: HttpUrl
    ttl_seconds: int = 60

# ---- Métricas
class StatsOut(BaseModel):
    ad_id: int
    date: date
    impressions: int
    clicks: int
