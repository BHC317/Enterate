import os, json, random
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo  # Python 3.9+

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "etl" / "data_raw" / "ide"
TODAY = datetime.now(ZoneInfo("Europe/Madrid"))
TODAY_YYYYMMDD = TODAY.strftime("%Y%m%d")
DEFAULT_OUT_DIR = (RAW_DIR / TODAY_YYYYMMDD).resolve()
if os.getenv("DATA_DIR"):
    DEFAULT_OUT_DIR = Path(os.getenv("DATA_DIR")).resolve()

FUENTE_IDE = "https://www.i-de.es/documents/1951486/1960840/Madrid.pdf/79b09d21-7309-1e84-6503-436f8901e830"

# Mapa sencillo barrio -> calles reales (muestrario suficiente para simulación)
CALLES_MADRID = {
    "Centro": [
        "C/ Atocha", "C/ Mayor", "C/ Toledo", "C/ Fuencarral", "C/ Hortaleza",
        "C/ Preciados", "C/ Embajadores", "C/ Montera", "C/ Segovia"
    ],
    "Arganzuela": ["Pº de las Delicias", "C/ Méndez Álvaro", "C/ Embajadores"],
    "Retiro": ["Av. Menéndez Pelayo", "C/ Doctor Esquerdo", "C/ O'Donnell"],
    "Salamanca": ["C/ Serrano", "C/ Velázquez", "C/ Goya", "C/ Príncipe de Vergara"],
    "Chamartín": ["Av. Alberto Alcocer", "C/ Padre Damián", "C/ Príncipe de Vergara"],
    "Tetuán": ["C/ Bravo Murillo", "C/ Marqués de Viana", "C/ Orense"],
    "Chamberí": ["C/ Santa Engracia", "C/ Luchana", "C/ Fuencarral"],
    "Fuencarral-El Pardo": ["Av. Monforte de Lemos", "C/ Sinesio Delgado"],
    "Moncloa-Aravaca": ["Pº de la Florida", "Av. de Valladolid", "C/ Princesa"],
    "Latina": ["Pº de Extremadura", "C/ Valmojado", "C/ General Fanjul"],
    "Carabanchel": ["C/ General Ricardos", "C/ Eugenia de Montijo"],
    "Usera": ["Av. de Rafael Ybarra", "Av. de Marcelo Usera"],
    "Puente de Vallecas": ["Av. de la Albufera", "C/ Monte Perdido"],
    "Moratalaz": ["C/ Camino de los Vinateros", "Av. Moratalaz"],
    "Ciudad Lineal": ["C/ Alcalá", "C/ Arturo Soria"],
    "Hortaleza": ["C/ Silvano", "C/ López de Hoyos"],
    "Villaverde": ["Av. de Andalucía", "C/ Alcocer"],
    "Villa de Vallecas": ["C/ Real de Arganda", "Av. del Ensanche de Vallecas"],
    "Vicálvaro": ["C/ San Cipriano", "C/ Minerva"],
    "San Blas-Canillejas": ["C/ Alcalá", "Av. de Arcentales"],
    "Barajas": ["Av. de Logroño", "C/ Galeón"]
}

def _pick_via() -> str:
    barrio = random.choice(list(CALLES_MADRID.keys()))
    via = random.choice(CALLES_MADRID[barrio])
    return via  # ya incluye prefijo (C/, Av., Pº, etc.)

def _rand_numero() -> str:
    # números realistas, ocasionalmente con bis o letra
    n = random.randint(1, 250)
    suf = random.choices(["", " BIS", " A", " B"], weights=[0.85, 0.05, 0.05, 0.05], k=1)[0]
    return f"{n}{suf}".strip()

def run(
    output_dir: str | os.PathLike | None = None,
    dias: int = 7,
    por_dia: int = 3,
    seed: int | None = None,
    municipio: str = "Madrid"
):
    if seed is not None:
        random.seed(seed)

    tz = ZoneInfo("Europe/Madrid")
    hoy = datetime.now(tz)
    fecha_fin = hoy + timedelta(days=dias)

    out_dir = Path(output_dir).resolve() if output_dir else DEFAULT_OUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    total = dias * por_dia
    eventos: list[dict] = []

    # Distribución de horarios: más probabilidad en horario laboral
    horario = [7,8,9,10,11,12,13,14,15,16,17,18,6,19,20,21,22,23,0,1,2,3,4,5]
    pesos   = [3,4,8,9,9,9,8,7,7,7,7,6,2,5,4,3,2,1,1,1,1,1,1,1]

    for i in range(total):
        d_offset = random.randint(0, dias - 1)
        start_hour = random.choices(horario, weights=pesos, k=1)[0]
        start_min = random.choice([0, 15, 30, 45])
        dur_h = random.uniform(1.0, 6.0)

        start_dt = (hoy + timedelta(days=d_offset)).replace(hour=start_hour, minute=start_min, second=0, microsecond=0)
        end_dt = start_dt + timedelta(hours=dur_h)

        eventos.append({
            "municipio": municipio,
            "fecha": start_dt.strftime("%d/%m/%Y"),
            "hora_inicio": start_dt.strftime("%H:%M"),
            "hora_fin": end_dt.strftime("%H:%M"),
            "via": _pick_via(),
            "numero": _rand_numero(),
            "fuente": FUENTE_IDE
        })

    fname = f"cortes_electricidad_{hoy.date()}_al_{fecha_fin.date()}.json"
    (out_dir / fname).write_text(json.dumps(eventos, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[ide] {len(eventos)} simulados -> {fname}")

if __name__ == "__main__":
    run()
