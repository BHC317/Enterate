from database import SessionLocal
from models.models import Coordinate
from models.models import (
RouteRequest,
RouteAnalysisResponse,
AffectedSegment,
AlternativeRoute,
)
from models.models import Incident
from models.models import Recommendation
from data.static_incidents import STATIC_INCIDENTS
from math import radians, cos, sin, asin, sqrt

def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> int:
    """Distancia aproximada en metros (no considera elevación)."""
    R = 6371000.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1))*cos(radians(lat2))*sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    return int(R * c)

def interpolate_points(a: Coordinate, b: Coordinate, spacing_m: int = 200) -> list[Coordinate]:
    """Devuelve puntos (incluye endpoints) cada ~spacing_m usando interp. lineal (ok para tramos cortos en ciudad)."""
    dist = haversine_m(a.lat, a.lng, b.lat, b.lng)
    if dist == 0:
        return [a]
    n = max(1, dist // spacing_m)
    out = []
    for k in range(int(n) + 1):
        t = k / n
        lat = a.lat + (b.lat - a.lat) * t
        lng = a.lng + (b.lng - a.lng) * t
        out.append(Coordinate(lat=lat, lng=lng))
    return out

class LogisticsService:
    @staticmethod
    def find_incidents_near_point(p: Coordinate, max_radius_m: int = 250) -> list[Incident]:
        near: list[Incident] = []
        for inc in STATIC_INCIDENTS:
            d = haversine_m(p.lat, p.lng, inc.lat, inc.lon)
            if d <= max(inc.radius_m, max_radius_m):
                near.append(inc)
        return near
    

    @staticmethod
    def analyze_route(req: RouteRequest) -> RouteAnalysisResponse:
        incidents_found: list[Incident] = []
        affected_segments: list[AffectedSegment] = []
        total_delay_min = 0

        # Detectar incidencias cerca de segmentos de la ruta
        for i in range(len(req.route) - 1):
            a, b = req.route[i], req.route[i+1]
            
            # Muestreamos el segmento, incluyendo endpoints
            samples = interpolate_points(
                Coordinate(lat=a.lat, lng=a.lng),
                Coordinate(lat=b.lat, lng=b.lng),
                spacing_m=200,   # ajusta 100–300 según precisión/CPU
            )

            # Busca incidencias cerca de CUALQUIER sample del tramo
            near: list[Incident] = []
            for p in samples:
                # puedes ampliar el radio si quieres ser más laxo: p. ej., 400–600
                near.extend(LogisticsService.find_incidents_near_point(p, max_radius_m=250))

            if near:
                incident = sorted(
                    near,
                    key=lambda x: ["low","medium","high","critical"].index(x.severity)
                )[-1]
                if incident.id not in {x.id for x in incidents_found}:
                    incidents_found.append(incident)
                seg_distance = haversine_m(a.lat, a.lng, b.lat, b.lng)
                affected_segments.append(
                    AffectedSegment(start_index=i, end_index=i+1, distance_m=seg_distance, reason_incident_id=incident.id)
                )
                delay_map = {"low": 1, "medium": 3, "high": 8, "critical": 15}
                total_delay_min += delay_map.get(incident.severity, 3)

        # ETA base: 30 km/h sobre distancia geométrica
        total_distance_m = 0
        for i in range(len(req.route) - 1):
            a, b = req.route[i], req.route[i+1]
            total_distance_m += haversine_m(a.lat, a.lng, b.lat, b.lng)
        eta_min = max(1, int((total_distance_m/1000) / 30 * 60))

        # Recomendaciones
        recs: list[Recommendation] = []
        if incidents_found:
            recs.append(Recommendation(id="REC-1", kind="reroute", text="Considere desviar 2-3 cuadras para evitar obra vial."))
            recs.append(Recommendation(id="REC-2", kind="time_shift", text="Si puede, difiera salida 20-30 min hasta que disminuya la congestión."))
        else:
            recs.append(Recommendation(id="REC-3", kind="speed_advice", text="Condiciones normales. Mantenga velocidad segura."))

        # Alternativa simulada
        alternatives: list[AlternativeRoute] = []
        if incidents_found and len(req.route) >= 3:
            alt_poly = [
                req.route[0],
                Coordinate(lat=req.route[1].lat+0.002, lng=req.route[1].lng-0.002),
                req.route[-1]
            ]
            alternatives.append(
                AlternativeRoute(
                    polyline=[Coordinate(lat=p.lat, lng=p.lng) for p in alt_poly],
                    added_distance_m=350,
                    added_time_min=5,
                    confidence=0.7,
                )
            )

        # Riesgo (0..1) segun severidad acumulada
        sev_weight = {"low": 0.1, "medium": 0.25, "high": 0.6, "critical": 0.9}
        risk = 0.0
        for inc in incidents_found:
            risk = min(1.0, risk + sev_weight.get(inc.severity, 0.2))

        return RouteAnalysisResponse(
            has_incidents=bool(incidents_found),
            total_risk_score=round(risk, 2),
            eta_min=eta_min,
            eta_with_incidents_min=eta_min + total_delay_min,
            expected_delay_min=total_delay_min,
            incidents=incidents_found,
            affected_segments=affected_segments,
            recommendations=recs,
            alternatives=alternatives,
        )

    @staticmethod
    def nearby_incidents(center: Coordinate, radius_m: int, types: list[str] | None, since: str | None) -> list[Incident]:
        from datetime import datetime

        since_dt = None
        if since:
            try:
                since_dt = datetime.fromisoformat(since)
            except Exception:
                since_dt = None        
        session = SessionLocal()
        query = session.query(Incident)
        if types:
            query = query.filter(Incident.type.in_(types))
        if since_dt:
            query = query.filter(Incident.start_ts_utc >= since_dt)
        candidates = query.all()


        results: list[Incident] = []

        for inc in candidates:
            if types and inc.type not in types:
                continue
            if since_dt and inc.start_ts_utc and inc.start_ts_utc < since_dt:
                continue
            d = haversine_m(center.lat, center.lng, inc.lat, inc.lon)
            if d <= radius_m:
                results.append(inc)
        return results
