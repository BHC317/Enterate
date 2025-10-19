from datetime import datetime, timedelta
from models.models import Incident

NOW = datetime.utcnow()

STATIC_INCIDENTS: list[Incident] = [
    Incident(
        fingerprint="INC-1001-roadwork-2025",
        source="ayto",
        category="road",
        status="active",
        city="Santo Domingo",
        street="Av. Máximo Gómez",
        street_number=None,
        lat=18.4861,
        lon=-69.9366,
        start_ts_utc=NOW - timedelta(hours=2),
        end_ts_utc=NOW + timedelta(hours=4),
        description="Cierre parcial carril derecho sentido norte-sur - Mantenimiento vial",
        event_id="EVT-AYTO-1001",
        ingested_at_utc=NOW
    ),
    Incident(
        fingerprint="INC-1002-flood-2025",
        source="ide",
        category="water",
        status="active",
        city="Santo Domingo",
        street="Paso a desnivel 27 de Febrero",
        street_number=None,
        lat=18.4729,
        lon=-69.9216,
        start_ts_utc=NOW - timedelta(hours=2),
        end_ts_utc=NOW + timedelta(hours=4),
        description="Inundación puntual - Acumulación de agua reduce velocidad a 10 km/h",
        event_id="EVT-IDE-1002",
        ingested_at_utc=NOW
    ),
    Incident(
        fingerprint="INC-1003-event-2025",
        source="ayto",
        category="road",
        status="planned",
        city="Santo Domingo",
        street="Parque Iberoamérica",
        street_number=None,
        lat=18.4716,
        lon=-69.9309,
        start_ts_utc=NOW + timedelta(hours=3),
        end_ts_utc=NOW + timedelta(hours=7),
        description="Concierto - Alta afluencia peatonal y desvíos temporales",
        event_id="EVT-AYTO-1003",
        ingested_at_utc=NOW
    ),
    Incident(
        fingerprint="INC-1004-gas-leak-2025",
        source="gas",
        category="gas",
        status="active",
        city="Santo Domingo",
        street="Av. Winston Churchill",
        street_number="150",
        lat=18.4743,
        lon=-69.9423,
        start_ts_utc=NOW - timedelta(hours=1),
        end_ts_utc=NOW + timedelta(hours=6),
        description="Fuga de gas reportada - Área acordonada",
        event_id="EVT-GAS-1004",
        ingested_at_utc=NOW
    ),
    Incident(
        fingerprint="INC-1005-power-outage-2025",
        source="ide",
        category="electricity",
        status="active",
        city="Santo Domingo",
        street="Av. Abraham Lincoln",
        street_number=None,
        lat=18.4689,
        lon=-69.9392,
        start_ts_utc=NOW - timedelta(minutes=30),
        end_ts_utc=NOW + timedelta(hours=2),
        description="Corte de energía eléctrica - Afecta semáforos en intersección",
        event_id="EVT-IDE-1005",
        ingested_at_utc=NOW
    ),
]