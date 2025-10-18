# models_ads.py
from sqlalchemy import Column, Integer, String, DateTime, Enum, ForeignKey, Numeric, Date, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import enum

from database import Base  # Usa tu Base existente

class AdStatusEnum(str, enum.Enum):
    draft = "draft"
    active = "active"
    paused = "paused"
    archived = "archived"

class Plan(Base):
    __tablename__ = "plans"
    id = Column(Integer, primary_key=True)
    name = Column(String(80), unique=True, nullable=False)
    monthly_price_usd = Column(Numeric(10, 2), nullable=False)
    impressions_quota = Column(Integer, nullable=False, default=0)

    advertisers = relationship("Advertiser", back_populates="plan")

class Advertiser(Base):
    __tablename__ = "advertisers"
    id = Column(Integer, primary_key=True)
    name = Column(String(120), nullable=False)
    email = Column(String(200), unique=True, index=True, nullable=False)
    plan_id = Column(Integer, ForeignKey("plans.id"), nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)

    plan = relationship("Plan", back_populates="advertisers")
    ads = relationship("Ad", back_populates="owner")

class Ad(Base):
    __tablename__ = "ads"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("advertisers.id"), nullable=False)
    title = Column(String(200), nullable=False)
    media_url = Column(String(500), nullable=False)
    target_url = Column(String(500), nullable=False)
    status = Column(Enum(AdStatusEnum), nullable=False, default=AdStatusEnum.draft)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    total_impressions = Column(Integer, nullable=False, default=0)
    total_clicks = Column(Integer, nullable=False, default=0)

    owner = relationship("Advertiser", back_populates="ads")
    stats = relationship("AdStatsDaily", back_populates="ad", cascade="all, delete-orphan")

class AdStatsDaily(Base):
    __tablename__ = "ad_stats_daily"
    ad_id = Column(Integer, ForeignKey("ads.id"), primary_key=True)
    day = Column(Date, primary_key=True)
    impressions = Column(Integer, nullable=False, default=0)
    clicks = Column(Integer, nullable=False, default=0)

    __table_args__ = (UniqueConstraint("ad_id", "day", name="uq_ad_day"),)

    ad = relationship("Ad", back_populates="stats")
