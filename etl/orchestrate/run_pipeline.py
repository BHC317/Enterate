# etl/orchestrate/run_pipeline.py
import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime, timezone

BASE = Path(__file__).resolve().parents[1]
RAW = BASE / "data_raw"
CUR = BASE / "data_curated"
SOURCES_DAILY = ["ide","canal","ayto"]
SOURCES_WEEKLY = ["gas"]

def _today_yymmdd():
    return datetime.now(timezone.utc).strftime("%Y%m%d")

def _to_iso(d):
    return f"{d[0:4]}-{d[4:6]}-{d[6:8]}"

def _run(cmd):
    r = subprocess.run(cmd)
    if r.returncode != 0:
        raise SystemExit(r.returncode)

def _raw_has_json(src, yyyymmdd):
    d = RAW / src / yyyymmdd
    if not d.exists():
        return False
    return any(p.is_file() and p.suffix.lower()==".json" for p in d.iterdir())

def _verify_extraction(mode):
    need = list(SOURCES_DAILY)
    if mode in ("weekly","all"):
        need += SOURCES_WEEKLY
    today = _today_yymmdd()
    missing = [s for s in need if not _raw_has_json(s, today)]
    if missing:
        raise SystemExit(f"[extract][missing] {today} -> {', '.join(missing)}")
    return _to_iso(today)

def _curated_ok_for_dt(src, dt_iso):
    p = CUR / src / f"dt={dt_iso}" / "part-000.parquet"
    return p.exists() and p.is_file() and p.stat().st_size > 0

def _verify_transform(dt_iso, mode):
    need = list(SOURCES_DAILY)
    if mode in ("weekly","all"):
        need += SOURCES_WEEKLY
    missing = []
    for s in need:
        if not _curated_ok_for_dt(s, dt_iso):
            missing.append(f"{s}/dt={dt_iso}")
    if missing:
        raise SystemExit(f"[transform][missing] {', '.join(missing)}")
    u = CUR / "union" / f"dt={dt_iso}" / "part-000.parquet"
    if not (u.exists() and u.is_file() and u.stat().st_size > 0):
        raise SystemExit(f"[union][missing] dt={dt_iso}")

def main():
    mode = os.getenv("PIPELINE_MODE","daily").strip().lower()
    if mode not in ("daily","weekly","all"):
        mode = "daily"
    # 1) Extraer
    _run([sys.executable, "-m", "etl.orchestrate.run_extract", "--mode", mode])
    # 2) Validar extracción
    dt_iso = _verify_extraction(mode)
    # 3) Transformar (parquets)
    _run([sys.executable, "-m", "etl.transform.run_transform"])
    # 4) Validar transform
    _verify_transform(dt_iso, mode)
    # 5) Cargar + dbt (usa el “todo en uno”)
    _run([sys.executable, "-m", "etl.orchestrate.run_load"])
    print("[pipeline] ok")

if __name__ == "__main__":
    main()
