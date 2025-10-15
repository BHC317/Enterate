# etl/orchestrate/run_extract.py
import os
import sys
import argparse
import subprocess
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

from etl.extract import electricidad_ide, agua_canal, calles_ayto, gas_sim

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
