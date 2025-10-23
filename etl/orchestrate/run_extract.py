# etl/orchestrate/run_extract.py
import os
import sys
import argparse
import subprocess
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import json
import shutil

from etl.extract import electricidad_ide, agua_canal, calles_ayto, gas_sim
from etl.extract import ide_simulate

DETACHED_PROCESS = 0x00000008
CREATE_NO_WINDOW = 0x08000000


def _print(enabled: bool, msg: str) -> None:
    if enabled:
        print(msg)


def _safe(tag, fn, *args, quiet: bool = False, **kwargs):
    try:
        fn(*args, **kwargs)
        _print(not quiet, f"[OK] {tag}")
        return tag, True, None
    except Exception as e:
        _print(True, f"[WARN] {tag} falló: {e}")
        if tag == "IDE":
            _print(True, "[IDE] Activando simulación por fallo en scraping…")
            out_dir = Path(args[0]) if len(args) > 0 else None
            if out_dir is None:
                today = datetime.now(timezone.utc).strftime("%Y%m%d")
                out_dir = Path("etl") / "data_raw" / "ide" / today
            out_dir.mkdir(parents=True, exist_ok=True)

            try:
                try:
                    ide_simulate.main(output_dir=str(out_dir))
                except TypeError:
                    ide_simulate.main()
            except Exception as e2:
                _print(True, f"[WARN] ide_simulate falló: {e2}")

            def _has_events(p: Path) -> bool:
                try:
                    data = json.loads(p.read_text(encoding="utf-8"))
                    if isinstance(data, list):
                        return len(data) > 0
                    if isinstance(data, dict):
                        return len(data.get("events", [])) > 0
                    return False
                except Exception:
                    return False

            candidates = [p for p in out_dir.glob("*.json") if _has_events(p)]

            if not candidates:
                ide_simulate.run(output_dir=str(out_dir), dias=7, por_dia=3, seed=42)
                candidates = [p for p in out_dir.glob("*.json") if _has_events(p)]

            if candidates:
                candidates.sort(key=lambda x: x.stat().st_mtime, reverse=True)
                canon = out_dir / "cortes_ide_events.json"
                if candidates[0] != canon:
                    shutil.copyfile(candidates[0], canon)
                _print(True, f"[IDE] Simulación OK -> {canon}")
                return tag, True, None

        return tag, False, str(e)


def _cwd_data_base() -> Path:
    base = Path.cwd() / "etl" / "data_raw"
    base.mkdir(parents=True, exist_ok=True)
    return base


def _relaunch_background(argv: list[str]) -> None:
    args = [sys.executable] + argv
    kwargs = {"stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL}
    if os.name == "nt":
        kwargs["creationflags"] = DETACHED_PROCESS | CREATE_NO_WINDOW
    else:
        kwargs["start_new_session"] = True
    subprocess.Popen(args, **kwargs)


def main():
    load_dotenv()

    base = _cwd_data_base().resolve()

    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["daily", "weekly", "all"], default="daily")
    p.add_argument("--background", action="store_true")
    p.add_argument("--quiet", action="store_true")
    args = p.parse_args()

    if args.background:
        script_path = str(Path(__file__).resolve())
        _relaunch_background([script_path, "--mode", args.mode, "--quiet"])
        return

    if args.quiet:
        os.environ["PYTHONWARNINGS"] = "ignore"

    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    out_luz = base / "ide" / today
    out_agua = base / "canal" / today
    out_calles = base / "ayto" / today
    out_gas = base / "gas" / today

    if args.mode in ("daily", "all"):
        jobs = [
            ("IDE", electricidad_ide.run, str(out_luz)),
            ("CANAL", agua_canal.run, str(out_agua)),
            ("AYTO", calles_ayto.run, str(out_calles)),
        ]
        with ThreadPoolExecutor(max_workers=3) as ex:
            futs = {ex.submit(_safe, tag, fn, out, quiet=args.quiet): tag for tag, fn, out in jobs}
            for fut in as_completed(futs):
                tag, ok, err = fut.result()
                _print(not args.quiet, f"[{tag}] {'OK' if ok else 'FAIL'}{'' if ok else f' -> {err}'}")

    if args.mode in ("weekly", "all"):
        _safe("GAS", gas_sim.run, str(out_gas), dias=7, por_dia=3, seed=42, quiet=args.quiet)

    _print(not args.quiet, ">> extracción terminada")


if __name__ == "__main__":
    main()
