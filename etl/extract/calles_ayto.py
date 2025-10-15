import os, json, requests, xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone

URL = "https://informo.madrid.es/informo/tmadrid/incid_aytomadrid.xml"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "etl" / "data_raw" / "ayto"
TODAYUTC = datetime.now(timezone.utc).strftime("%Y%m%d")
DEFAULT_OUT_DIR = (RAW_DIR / TODAYUTC).resolve()
if os.getenv("DATA_DIR"):
    DEFAULT_OUT_DIR = Path(os.getenv("DATA_DIR")).resolve()

def _fetch_xml() -> str:
    r = requests.get(URL, timeout=25)
    r.raise_for_status()
    r.encoding = r.apparent_encoding or "utf-8"
    return r.text

def _xml_rows(xml_text: str):
    root = ET.fromstring(xml_text)
    rows = []
    for inc in root.findall(".//Incidencia"):
        d = {c.tag: (c.text or "").strip() for c in inc}
        rows.append(d)
    return rows

def _to_bool(x): return str(x or "").strip().upper() in {"S","SI","Y","YES","1","TRUE"}

def _safe_float(x):
    try: return float(str(x).replace(",", "."))
    except: return None

def _norm_ts(s: str) -> str:
    s = (s or "").strip()
    if not s: return ""
    try:
        dt = datetime.fromisoformat(s.replace("Z","").split(".")[0])
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except:
        try:
            dt = datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        except:
            return s

def _to_events(rows):
    out=[]
    for r in rows:
        eid = f"tmadrid-{r.get('id_incidencia','').strip() or (r.get('codigo','').replace('/','-'))}"
        start_ts = _norm_ts(r.get("fh_inicio",""))
        end_ts   = _norm_ts(r.get("fh_final",""))
        out.append({
            "event_id": eid,
            "tipo": r.get("nom_tipo_incidencia") or r.get("cod_tipo_incidencia"),
            "programado": _to_bool(r.get("incid_prevista")) or _to_bool(r.get("incid_planificada")),
            "descripcion": r.get("descripcion") or "",
            "start_ts": start_ts,
            "end_ts": end_ts,
            "municipio": "Madrid",
            "lat": _safe_float(r.get("latitud")),
            "lon": _safe_float(r.get("longitud")),
            "estado": r.get("incid_estado"),
            "es_obras": _to_bool(r.get("es_obras")),
            "es_accidente": _to_bool(r.get("es_accidente")),
            "es_contaminacion": _to_bool(r.get("es_contaminacion")),
            "codigo": r.get("codigo"),
            "id_incidencia": r.get("id_incidencia")
        })
    return out

def run(output_dir: str | os.PathLike | None = None):
    out_dir = Path(output_dir).resolve() if output_dir else DEFAULT_OUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    xml_text = _fetch_xml()
    rows = _xml_rows(xml_text)
    (out_dir / "incid_ayto_raw.json").write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    events = _to_events(rows)
    (out_dir / "incid_ayto_events.json").write_text(json.dumps(events, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[ayto] raw={len(rows)} events={len(events)} -> incid_ayto_events.json")

if __name__ == "__main__":
    run()
