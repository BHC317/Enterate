from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from pydantic import BaseModel, Field
from sqlalchemy import Enum
from database import get_db
from sqlalchemy.orm import Session
from models.models import Incident
import hashlib
from datetime import datetime, timezone

from schemas.schemas import IncidentSchema, IncidentCreate
from sqlalchemy.exc import IntegrityError, DataError


router = APIRouter(tags=["incidents"], prefix="/incidents")

class ResponseError(BaseModel):
    detail: str = Field(description="Error message")    

# GET todos los incidentes
@router.get("/", 
            response_model=List[IncidentSchema],
            summary="Get all incidents",
            responses={
                status.HTTP_200_OK: {"description": "List of all incidents returned", "model": List[IncidentSchema]},
                status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Internal Server Error", "model": ResponseError}
            },
            status_code=status.HTTP_200_OK
)
def get_incidents(db: Session = Depends(get_db)):
    """
    Retrieve all incidents from the database.
    
    Returns a complete list of incidents with all their details.
    """
    incidents = db.query(Incident).all()
    return incidents


# GET incidente por ID
@router.get("/{id}", 
            response_model=IncidentSchema,
            summary="Get incident by ID",
            responses={
                status.HTTP_200_OK: {"description": "Incident found and returned", "model": IncidentSchema},
                status.HTTP_404_NOT_FOUND: {"description": "Incident not found", "model": ResponseError},
                status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Internal Server Error", "model": ResponseError}
            },
            status_code=status.HTTP_200_OK
)
def get_incident_by_id(
    id: str = Path(description="Event ID of the incident to retrieve"),
    db: Session = Depends(get_db)
):
    """
    Retrieve a specific incident by its event ID.
    
    - **id**: The unique event identifier for the incident
    
    Returns the incident details if found, otherwise returns 404.
    """
    incident = db.query(Incident).filter(Incident.event_id == id).first()
    
    if not incident:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail=f"Incident with event_id {id} not found"
        )
    
    return incident


# GET con filtros
@router.get("/filter/", 
            response_model=List[IncidentSchema],
            summary="Filter incidents by criteria",
            responses={
                status.HTTP_200_OK: {"description": "Filtered list of incidents returned", "model": List[IncidentSchema]},
                status.HTTP_400_BAD_REQUEST: {"description": "Invalid filter parameters", "model": ResponseError},
                status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Internal Server Error", "model": ResponseError}
            },
            status_code=status.HTTP_200_OK
)
def filter_incidents(
    source: str = Query(None, enum=["gas", "ayto", "ide", "canal"], description="Filter by incident source"),
    category: str = Query(None, enum=["gas", "road", "road_works", "electricity", "water"], description="Filter by incident category"),
    status: str = Query(None, enum=["planned", "active", "unplanned"], description="Filter by incident status"),
    street: str = Query(None, description="Filter by street name (partial match)"),
    db: Session = Depends(get_db)
):
    """
    Filter incidents based on multiple criteria.
    
    All filters are optional and can be combined:
    - **source**: The source system that reported the incident
    - **category**: The type/category of the incident
    - **status**: Current status of the incident
    - **street**: Street name (supports partial matches)
    
    Returns a list of incidents matching the specified filters.
    """
    query = db.query(Incident)
    
    if source:
        query = query.filter(Incident.source == source)
    if category:
        query = query.filter(Incident.category == category)
    if status:
        query = query.filter(Incident.status == status)
    if street:
        query = query.filter(Incident.street.ilike(f"%{street}%"))
    
    return query.all()


def _norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()

def make_incident_fingerprint(
    *,
    source: str,
    category: str,
    status: str,
    city: str,
    street: Optional[str],
    street_number: Optional[str],
    lat: Optional[float],
    lon: Optional[float],
    start_ts_utc: datetime,
    event_id: Optional[str],
) -> str:
    """
    Crea un hash estable con campos clave para evitar duplicados lógicos.
    Incluye event_id (si existe) y la precisión de coordenadas.
    """
    base = "|".join([
        _norm(source),
        _norm(category),
        _norm(status),
        _norm(city),
        _norm(street),
        _norm(street_number),
        f"{lat:.6f}" if lat is not None else "",
        f"{lon:.6f}" if lon is not None else "",
        start_ts_utc.isoformat(),          # con TZ
        _norm(event_id),
    ])
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

@router.post(
    "/",
    response_model=IncidentSchema,  # usa tu schema de salida
    status_code=status.HTTP_201_CREATED,
    summary="Create a new incident",
    responses={
        status.HTTP_201_CREATED: {"description": "Incident created"},
        status.HTTP_400_BAD_REQUEST: {"description": "Validation/Business error", "model": ResponseError},
        status.HTTP_409_CONFLICT: {"description": "Incident already exists (duplicate)", "model": ResponseError},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Internal Server Error", "model": ResponseError},
    },
)
def create_incident(payload: IncidentCreate, db: Session = Depends(get_db)):
    """
    Inserta una nueva incidencia. Genera `fingerprint` de forma determinística para
    evitar duplicados lógicos del mismo evento (mismo inicio, ubicación, etc.).
    """
    # Regla de negocio simple: end_ts_utc no puede ser anterior a start_ts_utc
    if payload.end_ts_utc and payload.end_ts_utc < payload.start_ts_utc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="end_ts_utc must be greater than or equal to start_ts_utc",
        )

    fingerprint = make_incident_fingerprint(
        source=payload.source,
        category=payload.category,
        status=payload.status,
        city=payload.city,
        street=payload.street,
        street_number=payload.street_number,
        lat=payload.lat,
        lon=payload.lon,
        start_ts_utc=payload.start_ts_utc,
        event_id=payload.event_id,
    )

    # Si ya existe ese fingerprint, devolvemos 409 (conflict)
    exists = db.query(Incident).filter(Incident.fingerprint == fingerprint).first()
    if exists:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Incident already exists (duplicate fingerprint).",
        )

    now_utc = datetime.now(timezone.utc)

    row = Incident(
        fingerprint=fingerprint,
        source=payload.source,
        category=payload.category,
        status=payload.status,
        city=payload.city,
        street=payload.street,
        street_number=payload.street_number,
        lat=payload.lat,
        lon=payload.lon,
        start_ts_utc=payload.start_ts_utc,
        end_ts_utc=payload.end_ts_utc,
        description=payload.description,
        event_id=payload.event_id,
        ingested_at_utc=now_utc,
    )

    try:
        db.add(row)
        db.commit()
        db.refresh(row)
        return row
    except IntegrityError as e:
        db.rollback()
        # Puede ser por CheckConstraint/PK/longitud
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Integrity error inserting incident: {str(e.orig)}",
        )
    except DataError as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Data error inserting incident: {str(e.orig)}",
        )
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unexpected error inserting incident",
        )
