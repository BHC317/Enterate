# etl/extract/electricidad_ide.py
import os
import io
import re
import json
import time
import base64
import shutil
import traceback
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Tuple, Optional

import requests
import pdfplumber
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

URL = "https://www.i-de.es/documents/1951486/1960840/Madrid.pdf/79b09d21-7309-1e84-6503-436f8901e830"

TODAYUTC = datetime.now(timezone.utc).strftime("%Y%m%d")


def _cwd_base() -> Path:
    base = Path.cwd() / "etl" / "data_raw"
    base.mkdir(parents=True, exist_ok=True)
    return base


_BASE = _cwd_base()
DEFAULT_OUT_DIR = (_BASE / "ide" / TODAYUTC).resolve()

HTTP_CONNECT = int(os.getenv("IDE_HTTP_TIMEOUT_CONNECT", "20"))
HTTP_READ = int(os.getenv("IDE_HTTP_TIMEOUT_READ", "150"))
HTTP_RETRIES = int(os.getenv("IDE_HTTP_RETRIES", "2"))
CDP_WAIT = int(os.getenv("IDE_CDP_WAIT", "45"))
DIR_WAIT = int(os.getenv("IDE_DIR_WAIT", "240"))
TIME_BUDGET = int(os.getenv("IDE_TIME_BUDGET", "180"))
STRATEGY = os.getenv("IDE_STRATEGY", "cdp_then_dir_then_http").strip().lower()
HEADLESS = False


def _unique_name(base_dir: str, base_filename: str) -> str:
    stem, ext = os.path.splitext(base_filename)
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    dst = os.path.join(base_dir, f"{stem}_{ts}{ext}")
    if os.path.exists(dst):
        k = 1
        while True:
            cand = os.path.join(base_dir, f"{stem}_{ts}_{k}{ext}")
            if not os.path.exists(cand):
                dst = cand
                break
            k += 1
    return dst


def _requests_session() -> requests.Session:
    s = requests.Session()
    r = Retry(
        total=HTTP_RETRIES,
        connect=HTTP_RETRIES,
        read=HTTP_RETRIES,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.mount("http://", HTTPAdapter(max_retries=r))
    s.headers.update({"User-Agent": "Mozilla/5.0", "Referer": URL})
    return s


def _download_http(url: str, out_path: str) -> str:
    s = _requests_session()
    with s.get(url, stream=True, timeout=(HTTP_CONNECT, HTTP_READ)) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(64 * 1024):
                if chunk:
                    f.write(chunk)
    return out_path


def _build_driver(download_dir: str):
    opts = Options()
    if HEADLESS:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1280,900")
    opts.set_capability("goog:loggingPrefs", {"performance": "ALL"})
    opts.add_experimental_option(
        "prefs",
        {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True,
        },
    )
    d = webdriver.Chrome(options=opts)
    try:
        d.execute_cdp_cmd("Page.setDownloadBehavior", {"behavior": "allow", "downloadPath": download_dir})
    except Exception:
        pass
    try:
        d.execute_cdp_cmd("Network.enable", {})
    except Exception:
        pass
    return d


def _poll_pdf_logs(driver, max_wait: int) -> List[str]:
    end = time.time() + max_wait
    seen = set()
    ids: List[str] = []
    while time.time() < end:
        try:
            logs = driver.get_log("performance")
        except Exception:
            time.sleep(0.3)
            continue
        for e in logs:
            try:
                msg = json.loads(e["message"])["message"]
            except Exception:
                continue
            if msg.get("method") != "Network.responseReceived":
                continue
            p = msg.get("params", {})
            resp = p.get("response", {})
            mime = (resp.get("mimeType") or "").lower()
            rurl = (resp.get("url") or "").lower()
            rid = p.get("requestId")
            key = (rid, rurl)
            if key in seen:
                continue
            seen.add(key)
            if ("pdf" in mime) or rurl.endswith(".pdf"):
                if rid:
                    ids.append(rid)
        if ids:
            break
        time.sleep(0.3)
    return ids


def _capture_cdp(driver, url: str, out_path: str, wait: int) -> str:
    sep = "&" if "?" in url else "?"
    bust = f"{url}{sep}ts={int(time.time())}"
    driver.get(bust)
    reqs = _poll_pdf_logs(driver, wait)
    if not reqs:
        bust = f"{url}{sep}ts={int(time.time())}"
        driver.get(bust)
        reqs = _poll_pdf_logs(driver, wait)
    if not reqs:
        raise RuntimeError("CDP: no PDF")
    for rid in reqs:
        try:
            body = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": rid})
            data = body.get("body", "")
            raw = base64.b64decode(data) if body.get("base64Encoded") else data.encode("utf-8", "ignore")
            with open(out_path, "wb") as f:
                f.write(raw)
            return out_path
        except Exception:
            continue
    raise RuntimeError("CDP: sin cuerpo")


def _dir_download(driver, url: str, download_dir: str, wait: int) -> str:
    baseline = 0.0
    for f in os.listdir(download_dir):
        if f.lower().endswith(".pdf"):
            try:
                baseline = max(baseline, os.path.getmtime(os.path.join(download_dir, f)))
            except Exception:
                pass
    sep = "&" if "?" in url else "?"
    bust = f"{url}{sep}ts={int(time.time())}"
    driver.get(bust)
    end = time.time() + wait
    candidate = None
    while time.time() < end:
        for f in os.listdir(download_dir):
            if not f.lower().endswith(".pdf"):
                continue
            p = os.path.join(download_dir, f)
            if os.path.exists(p + ".crdownload"):
                continue
            try:
                if os.path.getmtime(p) > baseline:
                    candidate = p
                    break
            except Exception:
                pass
        if candidate:
            break
        time.sleep(0.3)
    if not candidate:
        raise RuntimeError("DIR: no PDF")
    return candidate


def open_and_download(url: str, download_dir: str) -> str:
    os.makedirs(download_dir, exist_ok=True)
    start = time.time()

    def budget_ok() -> bool:
        return (time.time() - start) < TIME_BUDGET

    def try_http() -> Optional[str]:
        try:
            p = os.path.join(download_dir, "Madrid_requests.pdf")
            _download_http(url, p)
            u = _unique_name(download_dir, os.path.basename(p))
            try:
                shutil.move(p, u)
                return u
            except Exception:
                return p
        except Exception:
            traceback.print_exc()
            return None

    def try_cdp() -> Optional[str]:
        d = _build_driver(download_dir)
        try:
            p = os.path.join(download_dir, "Madrid_cdp.pdf")
            _capture_cdp(d, url, p, CDP_WAIT)
            u = _unique_name(download_dir, os.path.basename(p))
            try:
                shutil.move(p, u)
                return u
            except Exception:
                return p
        except Exception:
            traceback.print_exc()
            return None
        finally:
            try:
                d.quit()
            except Exception:
                pass

    def try_dir() -> Optional[str]:
        d = _build_driver(download_dir)
        try:
            p = _dir_download(d, url, download_dir, DIR_WAIT)
            u = _unique_name(download_dir, os.path.basename(p))
            try:
                shutil.move(p, u)
                return u
            except Exception:
                return p
        except Exception:
            traceback.print_exc()
            return None
        finally:
            try:
                d.quit()
            except Exception:
                pass

    order = {
        "http_only": [try_http],
        "cdp_only": [try_cdp],
        "dir_only": [try_dir],
        "http_then_cdp_then_dir": [try_http, try_cdp, try_dir],
        "cdp_then_dir_then_http": [try_cdp, try_dir, try_http],
    }.get(STRATEGY, [try_http, try_cdp, try_dir])

    for fn in order:
        if not budget_ok():
            break
        out = fn()
        if out:
            return out
    raise RuntimeError("i-DE: no se pudo obtener PDF (todas las estrategias fallaron)")


def parse_pdf_rows(pdf_path: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    start = re.compile(r"^([A-ZÁÉÍÓÚÜÑ ][A-ZÁÉÍÓÚÜÑ \-/ºª\.]+?)\s+(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2})\s+(\d{2}:\d{2})\s+(.*)$")
    skip = ("Internal Use", "Página", "Población | Municipio")
    with open(pdf_path, "rb") as f, pdfplumber.open(io.BytesIO(f.read())) as pdf:
        cur: Optional[Dict[str, str]] = None
        for page in pdf.pages:
            txt = page.extract_text() or ""
            for raw in txt.splitlines():
                ln = re.sub(r"\s+", " ", raw).strip()
                if not ln or ln.startswith(skip):
                    continue
                m = start.match(ln)
                if m:
                    if cur:
                        rows.append(cur)
                    cur = {
                        "municipio": m.group(1).title().strip(),
                        "fecha": m.group(2),
                        "hora_inicio": m.group(3),
                        "hora_fin": m.group(4),
                        "direcciones": m.group(5),
                    }
                elif cur:
                    cur["direcciones"] = (cur["direcciones"] + " " + ln).strip()
        if cur:
            rows.append(cur)
    return rows


def expand_numbers(blob: str) -> List[str]:
    out: List[str] = []
    for tok in [t.strip() for t in blob.split(",") if t.strip()]:
        m = re.match(r"^(\d+)\s*-\s*(\d+)$", tok)
        if m:
            a, b = int(m.group(1)), int(m.group(2))
            if a <= b and b - a <= 2000:
                out.extend([str(x) for x in range(a, b + 1)])
            else:
                out.append(tok.replace(" ", ""))
        else:
            out.append(tok.replace(" ", ""))
    return out


def explode_calles(direcciones: str) -> List[Tuple[str, str]]:
    items: List[Tuple[str, str]] = []
    for seg in [s.strip(" .") for s in direcciones.split(";") if s.strip()]:
        if ":" in seg:
            via, nums = seg.split(":", 1)
            via = via.strip()
            nums = nums.strip()
            if nums:
                for n in expand_numbers(nums):
                    items.append((via, n))
            else:
                items.append((via, ""))
        else:
            items.append((seg.strip(), ""))
    return items


def to_items(rows: List[Dict[str, str]], fuente: str) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []
    for r in rows:
        for via, numero in explode_calles(r["direcciones"]):
            items.append(
                {
                    "municipio": r["municipio"],
                    "fecha": r["fecha"],
                    "hora_inicio": r["hora_inicio"],
                    "hora_fin": r["hora_fin"],
                    "via": via,
                    "numero": numero,
                    "fuente": fuente,
                }
            )
    return items


def week_span_from_rows(rows: List[Dict[str, str]]) -> Tuple[str, str]:
    dates = sorted({datetime.strptime(r["fecha"], "%d/%m/%Y") for r in rows})
    if not dates:
        s = datetime.now().strftime("%d-%m-%Y")
        return s, s
    return dates[0].strftime("%d-%m-%Y"), dates[-1].strftime("%d-%m-%Y")


def run(output_dir: Optional[str | os.PathLike] = None) -> None:
    out_dir = Path(output_dir).resolve() if output_dir else DEFAULT_OUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    try:
        pdf_path = open_and_download(URL, str(out_dir))
    except Exception:
        traceback.print_exc()
        raise
    rows = parse_pdf_rows(pdf_path)
    items = to_items(rows, URL)
    wstart, wend = week_span_from_rows(rows)
    out_name = out_dir / f"Cortes electricidad {wstart} a {wend}.json"
    out_name.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    run()
