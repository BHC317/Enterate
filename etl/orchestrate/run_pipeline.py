# etl/orchestrate/run_pipeline.py
import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime, timezone

BASE = Path(__file__).resolve().parents[1]
RAW = BASE / "data_raw"
CUR = BASE / "data_curated"
SOURCES_DAILY = ["ide", "canal", "ayto"]
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
    return d.exists() and any(p.is_file() and p.suffix.lower() == ".json" for p in d.iterdir())

def _list_missing_and_available(mode):
    need = list(SOURCES_DAILY)
    if mode in ("weekly", "all"):
        need += SOURCES_WEEKLY
    ymd = _today_yymmdd()
    avail, missing = [], []
    for s in need:
        (_raw_has_json(s, ymd) and avail.append(s)) or missing.append(s)
    return ymd, avail, missing

def _curated_ok_for_dt(src, dt_iso):
    p = CUR / src / f"dt={dt_iso}" / "part-000.parquet"
    return p.exists() and p.is_file() and p.stat().st_size > 0

def _soft_verify_transform(dt_iso, sources):
    if not sources:
        print(f"[warn][transform] no hay fuentes disponibles para verificar dt={dt_iso}")
        return
    not_ok = [s for s in sources if not _curated_ok_for_dt(s, dt_iso)]
    if not_ok:
        print(f"[warn][transform] faltan parquets de dt={dt_iso}: {', '.join(not_ok)}")
    u = CUR / "union" / f"dt={dt_iso}" / "part-000.parquet"
    if not (u.exists() and u.is_file() and u.stat().st_size > 0):
        print(f"[warn][union] falta union dt={dt_iso}")

def main():
    mode = os.getenv("PIPELINE_MODE", "all").strip().lower()
    if mode not in ("daily", "weekly", "all"):
        mode = "all"

    # 1) Extraer (incluye fallback/simulación dentro de run_extract)
    _run([sys.executable, "-m", "etl.orchestrate.run_extract", "--mode", mode])

    # 2) Verificar extracción de forma NO bloqueante y continuar siempre
    ymd, avail, missing = _list_missing_and_available(mode)
    dt_iso = _to_iso(ymd)
    if missing:
        print(f"[warn][extract] {ymd} faltan: {', '.join(missing)}")
    else:
        print(f"[ok][extract] {ymd} todas las fuentes presentes")

    # 3) Transformar (parquets) — SIEMPRE
    _run([sys.executable, "-m", "etl.transform.run_transform"])

    # 4) Verificación suave de transform — NO bloquea
    _soft_verify_transform(dt_iso, avail)

    # 5) Cargar + dbt — SIEMPRE
    _run([sys.executable, "-m", "etl.orchestrate.run_load"])

    print("[pipeline] ok")

if __name__ == "__main__":
    main()
