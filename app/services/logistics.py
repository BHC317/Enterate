from models import Coordinate
from models import (
RouteRequest,
RouteAnalysisResponse,
AffectedSegment,
AlternativeRoute,
)
from models import Incident
from models import Recommendation
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

class LogisticsService:
    @staticmethod
    def find_incidents_near_point(p: Coordinate, max_radius_m: int = 250) -> list[Incident]:
        near: list[Incident] = []
        for inc in STATIC_INCIDENTS:
            d = haversine_m(p.lat, p.lng, inc.location.lat, inc.location.lng)
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
            mid = Coordinate(lat=(a.lat + b.lat) / 2, lng=(a.lng + b.lng) / 2)
            near = LogisticsService.find_incidents_near_point(mid)
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
        results: list[Incident] = []
        since_dt = None
        if since:
            try:
                since_dt = datetime.fromisoformat(since)
            except Exception:
                since_dt = None
        for inc in STATIC_INCIDENTS:
            if types and inc.type not in types:
                continue
            if since_dt and inc.start_time and inc.start_time < since_dt:
                continue
            d = haversine_m(center.lat, center.lng, inc.location.lat, inc.location.lng)
            if d <= radius_m:
                results.append(inc)
        return results
