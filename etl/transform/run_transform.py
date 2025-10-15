import os
import re
import json
import time
import sqlite3
import unicodedata
import requests
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional
import pandas as pd

BASE = Path(__file__).resolve().parents[1]
RAW = BASE / "data_raw"
CUR = BASE / "data_curated"
CACHE = BASE / ".cache"
SOURCES = ["ide", "canal", "ayto", "gas"]

# ------------------------ FS UTILS ------------------------

def _iter_json_files(source: str) -> List[Tuple[Path, str]]:
    out = []
    src_dir = RAW / source
    if not src_dir.exists():
        return out
    for dt_dir in sorted(src_dir.iterdir()):
        if not dt_dir.is_dir():
            continue
        dt_raw = dt_dir.name.strip()
        if len(dt_raw) == 8 and dt_raw.isdigit():
            dt = f"{dt_raw[0:4]}-{dt_raw[4:6]}-{dt_raw[6:8]}"
        elif re.match(r"^\d{4}-\d{2}-\d{2}$", dt_raw):
            dt = dt_raw
        else:
            dt = datetime.now(timezone.utc).date().isoformat()
        for fp in dt_dir.glob("*.json"):
            out.append((fp, dt))
    return out

def _read_json(fp: Path) -> List[Dict]:
    data = json.loads(fp.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for k in ("items","results","data","events","incidencias"):
            v = data.get(k)
            if isinstance(v, list):
                return v
        return [data]
    return []

def _write_clean_json(original_fp: Path, records: List[Dict]) -> Optional[Path]:
    if not records:
        return None
    clean_dir = original_fp.parent / "clean"
    clean_dir.mkdir(parents=True, exist_ok=True)
    out_fp = clean_dir / f"{original_fp.stem}.clean.json"
    out_fp.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_fp

# ------------------------ MADRID CHECKS ------------------------

def _strip_accents(s: str) -> str:
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()

def _is_madrid_strict(s: Optional[str]) -> bool:
    if not s:
        return False
    t = _strip_accents(str(s)).lower().strip()
    t = re.sub(r"\s+", " ", t)
    return bool(re.fullmatch(r"madrid(?:\s*\(capital\))?", t))

def _is_madrid_soft(s: Optional[str]) -> bool:
    if not s:
        return False
    return "madrid" in str(s).casefold()

def _in_bbox(lat: Optional[float], lon: Optional[float]) -> bool:
    try:
        if lat is None or lon is None:
            return False
        return 40.2 <= float(lat) <= 40.6 and -3.9 <= float(lon) <= -3.4
    except:
        return False

# ------------------------ GEOCODING ------------------------

def _gc_db():
    CACHE.mkdir(parents=True, exist_ok=True)
    cx = sqlite3.connect(CACHE / "geocode.sqlite")
    cx.execute("create table if not exists gc(address text primary key, lat real, lon real)")
    cx.commit()
    return cx

def _addr(street: Optional[str], number: Optional[str]) -> Optional[str]:
    s = (street or "").strip()
    n = (number or "").strip()
    if not s and not n:
        return None
    return f"{n} {s}, Madrid, Spain".strip()

def _geocode(street: Optional[str], number: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
    address = _addr(street, number)
    if not address:
        return None, None
    cx = _gc_db()
    row = cx.execute("select lat,lon from gc where address=?", (address,)).fetchone()
    if row:
        return float(row[0]), float(row[1])
    url = "https://nominatim.openstreetmap.org/search"
    ua = os.getenv("GEOCODE_UA", "enterate-etl/1.0")
    r = requests.get(url, params={"q": address, "format": "json", "limit": 1}, headers={"User-Agent": ua}, timeout=20)
    if r.ok:
        data = r.json()
        if isinstance(data, list) and data:
            lat = float(data[0]["lat"])
            lon = float(data[0]["lon"])
            cx.execute("insert or replace into gc(address,lat,lon) values(?,?,?)", (address, lat, lon))
            cx.commit()
            time.sleep(1.0)
            return lat, lon
    return None, None

# ------------------------ CLEANING HELPERS ------------------------

_VIA_TOKEN = r"(?:Cl|C/|Calle|Avda?|Av\.?|Paseo|Ps\.?|Plaza|Pl\.?|Ctra|Ronda|Camino|Cmno|Pza\.?)"

def _clean_via(v: Optional[str]) -> Optional[str]:
    if not v:
        return None
    s = str(v).strip()
    s = re.sub(r"\s+", " ", s)
    m = re.search(rf"({_VIA_TOKEN})\s+.+$", s, flags=re.IGNORECASE)
    if m:
        start = m.start(1)
        return s[start:]
    return s

def _clean_num(n: Optional[str]) -> Optional[str]:
    if not n:
        return None
    s = str(n).strip()
    s = s.replace(" ", "")
    s = s.replace("º", "").replace("ª", "")
    s = re.sub(r"(?i)madrid.*$", "", s)
    s = re.sub(r"\d{1,2}/\d{1,2}/\d{2,4}.*$", "", s)
    s = re.sub(r"\d{2}:\d{2}.*$", "", s)
    s = re.sub(r"(?i)cl[A-Za-zÁÉÍÓÚÜÑ]+:?\d+[A-Za-z]?$", "", s)
    s = re.sub(r"[^0-9A-Za-z]", "", s)
    m = re.match(r"^(\d+[A-Za-z]{0,2})", s)
    return m.group(1) if m else None

def _clean_ide_record(rec: Dict) -> Optional[Dict]:
    m = str(rec.get("municipio") or "").strip()
    if not _is_madrid_strict(m):
        return None
    out = dict(rec)
    out.pop("fuente", None)
    out.pop("source", None)
    via_raw = out.get("via")
    num_raw = out.get("numero")
    via = _clean_via(via_raw)
    num = _clean_num(num_raw)
    out["via"] = via
    out["numero"] = num
    lat_g, lon_g = _geocode(via, num)
    if lat_g is not None and lon_g is not None:
        out["lat"] = lat_g
        out["lon"] = lon_g
    return out

def _ayto_extract_from_desc(desc: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not desc:
        return None, None
    t = desc.strip()
    mnum = re.search(r"(?i)\b(nº|n°|num\.?|numero)\s*[:\-]?\s*(\d+[A-Za-z]?)", t)
    number = mnum.group(2) if mnum else None
    street = None
    if mnum:
        head = t[:mnum.start()].strip(" .,:;")
        head = re.sub(r"\s+", " ", head)
        street = head if head else None
    return (street or None), (number or None)

def _ayto_enrich_with_desc(records: List[Dict]) -> List[Dict]:
    out = []
    for r in records:
        rec = dict(r)
        if (not rec.get("via")) or (not rec.get("numero")):
            street, num = _ayto_extract_from_desc(rec.get("descripcion") or rec.get("mensaje"))
            if (not rec.get("via")) and street:
                rec["via"] = street
            if (not rec.get("numero")) and num:
                rec["numero"] = num
        out.append(rec)
    return out

def _clean_generic_madrid(rec: Dict, city_keys: List[str], addr_keys: List[str], lat_key: str = "lat", lon_key: str = "lon") -> Optional[Dict]:
    out = dict(rec)
    out.pop("fuente", None)
    out.pop("source", None)
    city = None
    for ck in city_keys:
        if ck in rec and rec[ck] is not None:
            city = str(rec[ck]).strip()
            break
    if _is_madrid_strict(city) or _is_madrid_soft(city):
        return out
    lat = rec.get(lat_key)
    lon = rec.get(lon_key)
    if _in_bbox(lat, lon):
        return out
    addr_text = " ".join(str(rec.get(k) or "") for k in addr_keys)
    if _is_madrid_soft(addr_text):
        return out
    return None

# ------------------------ DATAFRAME BUILDERS ------------------------

def _df_ide(rows: List[Dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    need = ["municipio","fecha","hora_inicio","hora_fin","via","numero","lat","lon"]
    for c in need:
        if c not in df.columns:
            df[c] = None
    df = df[df["municipio"].apply(_is_madrid_strict)]
    return df.reset_index(drop=True)

def _df_passthrough(rows: List[Dict], required: List[str] | None = None) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if required:
        for c in required:
            if c not in df.columns:
                df[c] = None
    return df.reset_index(drop=True)

# ------------------------ WRITING ------------------------

def _write_daily(df: pd.DataFrame, source: str, dt: str) -> Path:
    out_dir = CUR / source / f"dt={dt}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_fp = out_dir / "part-000.parquet"
    df.to_parquet(out_fp, index=False)
    return out_fp

def _build_history(source: str) -> Path:
    parts = []
    src_dir = CUR / source
    for p in sorted(src_dir.glob("dt=*/part-*.parquet")):
        parts.append(pd.read_parquet(p))
    if not parts:
        return src_dir / "history.parquet"
    hist = pd.concat(parts, ignore_index=True)
    out_fp = src_dir / "history.parquet"
    hist.to_parquet(out_fp, index=False)
    return out_fp

def _union_for_date(dt: str) -> Path:
    parts = []
    for source in SOURCES:
        p = CUR / source / f"dt={dt}" / "part-000.parquet"
        if p.exists():
            df = pd.read_parquet(p)
            df["source"] = source
            parts.append(df)
    out_dir = CUR / "union" / f"dt={dt}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_fp = out_dir / "part-000.parquet"
    if parts:
        pd.concat(parts, ignore_index=True).to_parquet(out_fp, index=False)
    else:
        pd.DataFrame().to_parquet(out_fp, index=False)
    return out_fp

def _build_union_history() -> Path:
    parts = []
    uni_dir = CUR / "union"
    for p in sorted(uni_dir.glob("dt=*/part-*.parquet")):
        parts.append(pd.read_parquet(p))
    if not parts:
        return uni_dir / "history.parquet"
    hist = pd.concat(parts, ignore_index=True)
    out_fp = uni_dir / "history.parquet"
    hist.to_parquet(out_fp, index=False)
    return out_fp

# ------------------------ PIPELINE ------------------------

def _process_source(source: str) -> List[str]:
    written_dt = set()
    for fp, dt in _iter_json_files(source):
        raw_rows = _read_json(fp)
        if not raw_rows:
            continue

        if source == "ide":
            cleaned = [c for r in raw_rows if (c := _clean_ide_record(r)) is not None]
            if not cleaned:
                continue
            _write_clean_json(fp, cleaned)
            df = _df_ide(cleaned)

        elif source == "canal":
            cleaned = [c for r in raw_rows if (c := _clean_generic_madrid(r, ["municipio","city"], ["via","street","direccion"])) is not None]
            if not cleaned:
                continue
            _write_clean_json(fp, cleaned)
            df = _df_passthrough(cleaned)

        elif source == "ayto":
            # Solo limpiar los ficheros 'events', no los raw XML parseados
            if "events" not in fp.stem.lower():
                continue
            base_clean = [c for r in raw_rows if (c := _clean_generic_madrid(r, ["municipio","city"], ["via","street","calle","direccion","descripcion"])) is not None]
            if not base_clean:
                continue
            cleaned = _ayto_enrich_with_desc(base_clean)
            _write_clean_json(fp, cleaned)
            df = _df_passthrough(cleaned)

        elif source == "gas":
            cleaned = [c for r in raw_rows if (c := _clean_generic_madrid(r, ["city"], ["street","direccion"])) is not None]
            if not cleaned:
                continue
            _write_clean_json(fp, cleaned)
            df = _df_passthrough(cleaned)

        else:
            continue

        if not df.empty:
            _write_daily(df, source, dt)
            written_dt.add(dt)

    _build_history(source)
    return sorted(written_dt)

def run() -> None:
    all_dt = set()
    for source in SOURCES:
        for dt in _process_source(source):
            all_dt.add(dt)
    for dt in sorted(all_dt):
        _union_for_date(dt)
    _build_union_history()

if __name__ == "__main__":
    run()
