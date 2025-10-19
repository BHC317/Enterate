from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from pydantic import BaseModel, Field
from sqlalchemy import Enum
from database import get_db
from sqlalchemy.orm import Session
from models.models import Incident

from schemas.schemas import IncidentSchema

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
    category: str = Query(None, enum=["gas", "road", "electricity", "water"], description="Filter by incident category"),
    status: str = Query(None, enum=["planned", "active", "1"], description="Filter by incident status"),
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