from datetime import datetime, timedelta
# from ..models.incident import Incident
# from ..models.geo import Coordinate
from models.models import Incident, Coordinate

NOW = datetime.utcnow()

STATIC_INCIDENTS: list[Incident] = [
    Incident(
        id="INC-1001",
        type="roadwork",
        title="Mantenimiento vial Av. Máximo Gómez",
        description="Cierre parcial carril derecho sentido norte-sur",
        location=Coordinate(lat=18.4861, lng=-69.9366),
        radius_m=220,
        severity="medium",
        start_time=NOW - timedelta(hours=2),
        end_time=NOW + timedelta(hours=4),
        source="official",
    ),
    Incident(
        id="INC-1002",
        type="flood",
        title="Inundación puntual en Paso a desnivel 27 de Febrero",
        description="Acumulación de agua reduce velocidad a 10 km/h",
        location=Coordinate(lat=18.4729, lng=-69.9216),
        radius_m=180,
        severity="high",
        start_time=NOW - timedelta(hours=2),
        end_time=NOW + timedelta(hours=4),
        source="official",
    ),
    Incident(
        id="INC-1003",
        type="event",
        title="Concierto Parque Iberoamérica",
        description="Alta afluencia peatonal y desvíos temporales",
        location=Coordinate(lat=18.4716, lng=-69.9309),
        radius_m=300,
        severity="medium",
        start_time=NOW + timedelta(hours=3),
        end_time=NOW + timedelta(hours=7),
        source="media",
    ),
]