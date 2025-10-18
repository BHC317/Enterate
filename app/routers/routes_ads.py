# routes_ads.py
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import select, update, func
from datetime import date
import random

from database import get_db, engine  # get_db viene de tu módulo
from models.models_ads import Plan, Advertiser, Ad, AdStatsDaily, AdStatusEnum, Base
from models.schemas_ads import (
    PlanCreate, PlanOut,
    AdvertiserCreate, AdvertiserOut,
    AdCreate, AdOut, ServeAdOut, StatsOut
)

router = APIRouter(prefix="", tags=["ads"])

# Crear tablas si no existen (al cargar el módulo)
Base.metadata.create_all(bind=engine)

# ---------------- PLANES ----------------
@router.post("/plans", response_model=PlanOut)
def create_plan(payload: PlanCreate, db: Session = Depends(get_db)):
    plan = Plan(
        name=payload.name,
        monthly_price_usd=payload.monthly_price_usd,
        impressions_quota=payload.impressions_quota
    )
    db.add(plan)
    db.commit()
    db.refresh(plan)
    return PlanOut(id=plan.id, name=plan.name,
                   monthly_price_usd=float(plan.monthly_price_usd),
                   impressions_quota=plan.impressions_quota)

@router.get("/plans", response_model=list[PlanOut])
def list_plans(db: Session = Depends(get_db)):
    rows = db.execute(select(Plan).order_by(Plan.id)).scalars().all()
    return [
        PlanOut(id=p.id, name=p.name,
                monthly_price_usd=float(p.monthly_price_usd),
                impressions_quota=p.impressions_quota)
        for p in rows
    ]

# ---------------- ANUNCIANTES ----------------
@router.post("/advertisers", response_model=AdvertiserOut)
def create_advertiser(payload: AdvertiserCreate, db: Session = Depends(get_db)):
    if payload.plan_id is not None:
        if not db.get(Plan, payload.plan_id):
            raise HTTPException(404, "Plan no existe")

    adv = Advertiser(name=payload.name, email=payload.email, plan_id=payload.plan_id)
    db.add(adv)
    db.commit()
    db.refresh(adv)

    return AdvertiserOut(id=adv.id, name=adv.name, email=adv.email,
                         plan_id=adv.plan_id, created_at=adv.created_at)

@router.get("/advertisers", response_model=list[AdvertiserOut])
def list_advertisers(db: Session = Depends(get_db)):
    rows = db.execute(select(Advertiser).order_by(Advertiser.id)).scalars().all()
    return [
        AdvertiserOut(id=a.id, name=a.name, email=a.email,
                      plan_id=a.plan_id, created_at=a.created_at)
        for a in rows
    ]

@router.put("/advertisers/{user_id}/plan/{plan_id}", response_model=AdvertiserOut)
def assign_plan(user_id: int, plan_id: int, db: Session = Depends(get_db)):
    plan = db.get(Plan, plan_id)
    if not plan:
        raise HTTPException(404, "Plan no existe")
    adv = db.get(Advertiser, user_id)
    if not adv:
        raise HTTPException(404, "Usuario no existe")

    adv.plan_id = plan_id
    db.commit()
    db.refresh(adv)
    return AdvertiserOut(id=adv.id, name=adv.name, email=adv.email,
                         plan_id=adv.plan_id, created_at=adv.created_at)

# ---------------- SERVIR ANUNCIO (para la app) ----------------
@router.get("/ads/serve-ad", response_model=ServeAdOut)
def serve_ad(
    placement: str | None = Query(None, description="Ubicación en la app"),
    db: Session = Depends(get_db)
):
    # Sencillo: toma un anuncio active al azar
    active_ads = db.execute(
        select(Ad.id, Ad.title, Ad.media_url, Ad.target_url).where(Ad.status == AdStatusEnum.active)
    ).all()
    if not active_ads:
        raise HTTPException(404, "No hay anuncios activos")
    ad_id, title, media_url, target_url = random.choice(active_ads)

    # registrar 1 impresión (totales + diario)
    _increment_metric(db, ad_id, metric="impressions", amount=1)

    return ServeAdOut(ad_id=ad_id, title=title, media_url=media_url, target_url=target_url)


# ---------------- ANUNCIOS ----------------
@router.post("/ads", response_model=AdOut)
def create_ad(payload: AdCreate, db: Session = Depends(get_db)):
    if not db.get(Advertiser, payload.user_id):
        raise HTTPException(404, "Usuario no existe")

    ad = Ad(
        user_id=payload.user_id,
        title=payload.title,
        media_url=str(payload.media_url),
        target_url=str(payload.target_url),
        status=AdStatusEnum(payload.status),
    )
    db.add(ad)
    db.commit()
    db.refresh(ad)
    return _ad_to_out(ad)

@router.get("/ads", response_model=list[AdOut])
def list_ads(
    status: AdStatusEnum | None = Query(default=None, description="Filtra por estado"),
    db: Session = Depends(get_db)
):
    stmt = select(Ad)
    if status:
        stmt = stmt.where(Ad.status == status)
    ads = db.execute(stmt.order_by(Ad.id)).scalars().all()
    return [_ad_to_out(a) for a in ads]

@router.get("/ads/{ad_id}", response_model=AdOut)
def get_ad(ad_id: int, db: Session = Depends(get_db)):
    ad = db.get(Ad, ad_id)
    if not ad:
        raise HTTPException(404, "Anuncio no existe")
    return _ad_to_out(ad)

@router.post("/ads/{ad_id}/activate", response_model=AdOut)
def activate_ad(ad_id: int, db: Session = Depends(get_db)):
    return _set_status(ad_id, AdStatusEnum.active, db)

@router.post("/ads/{ad_id}/pause", response_model=AdOut)
def pause_ad(ad_id: int, db: Session = Depends(get_db)):
    return _set_status(ad_id, AdStatusEnum.paused, db)

def _set_status(ad_id: int, new_status: AdStatusEnum, db: Session) -> AdOut:
    ad = db.get(Ad, ad_id)
    if not ad:
        raise HTTPException(404, "Anuncio no existe")
    ad.status = new_status
    db.commit()
    db.refresh(ad)
    return _ad_to_out(ad)

# ---------------- MÉTRICAS ----------------
@router.post("/ads/{ad_id}/impression")
def register_impression(ad_id: int, db: Session = Depends(get_db)):
    _ensure_ad_exists(db, ad_id)
    _increment_metric(db, ad_id, metric="impressions", amount=1)
    return {"ok": True}

@router.post("/ads/{ad_id}/click")
def register_click(ad_id: int, db: Session = Depends(get_db)):
    _ensure_ad_exists(db, ad_id)
    _increment_metric(db, ad_id, metric="clicks", amount=1)
    return {"ok": True}

# ---------------- REPORTES ----------------
@router.get("/reports/ads/{ad_id}/daily", response_model=list[StatsOut])
def get_daily_stats(ad_id: int, db: Session = Depends(get_db)):
    _ensure_ad_exists(db, ad_id)
    rows = db.execute(
        select(AdStatsDaily.ad_id, AdStatsDaily.day, AdStatsDaily.impressions, AdStatsDaily.clicks)
        .where(AdStatsDaily.ad_id == ad_id)
        .order_by(AdStatsDaily.day.desc())
        .limit(31)
    ).all()
    return [StatsOut(ad_id=r[0], date=r[1], impressions=r[2], clicks=r[3]) for r in rows]

# ---------------- Helpers ----------------
def _ensure_ad_exists(db: Session, ad_id: int):
    if not db.get(Ad, ad_id):
        raise HTTPException(404, "Anuncio no existe")

def _increment_metric(db: Session, ad_id: int, metric: str, amount: int):
    ad = db.get(Ad, ad_id)
    if not ad:
        raise HTTPException(404, "Anuncio no existe")

    if metric == "impressions":
        ad.total_impressions += amount
    else:
        ad.total_clicks += amount

    today = date.today()
    stats = db.get(AdStatsDaily, {"ad_id": ad_id, "day": today})
    if not stats:
        stats = AdStatsDaily(ad_id=ad_id, day=today, impressions=0, clicks=0)
        db.add(stats)

    if metric == "impressions":
        stats.impressions += amount
    else:
        stats.clicks += amount

    db.commit()
    # no hace falta refresh aquí

def _ad_to_out(a: Ad) -> AdOut:
    return AdOut(
        id=a.id, user_id=a.user_id, title=a.title,
        media_url=a.media_url, target_url=a.target_url,
        status=a.status.value if hasattr(a.status, "value") else a.status,
        created_at=a.created_at, total_impressions=a.total_impressions,
        total_clicks=a.total_clicks
    )
