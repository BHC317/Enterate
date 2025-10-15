import os, re, json
from pathlib import Path
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup

URL = "https://oficinavirtual.canaldeisabelsegunda.es/gestiones-on-line/incidencias-en-el-suministro"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "etl" / "data_raw" / "canal"
TODAYUTC = datetime.now(timezone.utc).strftime("%Y%m%d")
DEFAULT_OUT_DIR = (RAW_DIR / TODAYUTC).resolve()
if os.getenv("DATA_DIR"):
    DEFAULT_OUT_DIR = Path(os.getenv("DATA_DIR")).resolve()

def _fetch_html() -> str:
    r = requests.get(URL, headers={"User-Agent":"Mozilla/5.0"}, timeout=25)
    r.raise_for_status()
    return r.text

def _extract_markers(html: str):
    soup = BeautifulSoup(html, "html.parser")
    script_text = next((s.string for s in soup.find_all("script") if s.string and "var markers" in s.string), None)
    if not script_text:
        m = re.search(r"var\s+markers\s*=\s*(\[[\s\S]*?\]);", html, re.I)
        script_text = m.group(0) if m else ""
    m = re.search(r"var\s+markers\s*=\s*(\[[\s\S]*?\]);", script_text or "", re.I)
    if not m: return []
    return json.loads(m.group(1))

def _to_events(markers):
    out=[]
    for i, inc in enumerate(markers):
        direccion = (inc.get("direccion") or "").lower()
        if "madrid" not in direccion: continue
        out.append({
            "event_id": f"agua-{datetime.utcnow().strftime('%Y%m%d')}-{i+1:05d}",
            "tipo": "mantenimiento" if inc.get("tipoIncidencia") == "TRA" else "avería",
            "programado": str(inc.get("programado","")).lower() == "true",
            "direccion": inc.get("direccion"),
            "lat": inc.get("latitud"),
            "lon": inc.get("longitud"),
            "start_ts": inc.get("fechaInicio"),
            "end_ts": inc.get("fechaFin") or None,
            "mensaje": (inc.get("mensaje") or "").strip().capitalize()
        })
    return out

def run(output_dir: str | os.PathLike | None = None):
    out_dir = Path(output_dir).resolve() if output_dir else DEFAULT_OUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    html = _fetch_html()
    markers = _extract_markers(html)
    events = _to_events(markers)
    out_file = out_dir / "cortes_agua_canalisabelii.json"
    out_file.write_text(json.dumps(events, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[agua] {len(events)} cortes -> {out_file.name}")

if __name__ == "__main__":
    run()
