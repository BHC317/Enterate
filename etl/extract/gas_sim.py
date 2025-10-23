import os, json, random
from pathlib import Path
from datetime import datetime, timedelta, timezone

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "etl" / "data_raw" / "gas"
TODAYUTC = datetime.now(timezone.utc).strftime("%Y%m%d")
DEFAULT_OUT_DIR = (RAW_DIR / TODAYUTC).resolve()
if os.getenv("DATA_DIR"):
    DEFAULT_OUT_DIR = Path(os.getenv("DATA_DIR")).resolve()

DISTRITOS = [
    {"nombre":"Centro","lat":40.4168,"lon":-3.7038},
    {"nombre":"Arganzuela","lat":40.4009,"lon":-3.6947},
    {"nombre":"Retiro","lat":40.4114,"lon":-3.6768},
    {"nombre":"Salamanca","lat":40.4305,"lon":-3.6789},
    {"nombre":"Chamartín","lat":40.4517,"lon":-3.6785},
    {"nombre":"Tetuán","lat":40.4587,"lon":-3.7047},
    {"nombre":"Chamberí","lat":40.4342,"lon":-3.7033},
    {"nombre":"Fuencarral-El Pardo","lat":40.5122,"lon":-3.7148},
    {"nombre":"Moncloa-Aravaca","lat":40.4411,"lon":-3.7381},
    {"nombre":"Latina","lat":40.3953,"lon":-3.7454},
    {"nombre":"Carabanchel","lat":40.3802,"lon":-3.7455},
    {"nombre":"Usera","lat":40.3794,"lon":-3.7072},
    {"nombre":"Puente de Vallecas","lat":40.3871,"lon":-3.6629},
    {"nombre":"Moratalaz","lat":40.4076,"lon":-3.6547},
    {"nombre":"Ciudad Lineal","lat":40.4451,"lon":-3.6488},
    {"nombre":"Hortaleza","lat":40.4745,"lon":-3.6412},
    {"nombre":"Villaverde","lat":40.3440,"lon":-3.7103},
    {"nombre":"Villa de Vallecas","lat":40.3678,"lon":-3.6019},
    {"nombre":"Vicálvaro","lat":40.4011,"lon":-3.6015},
    {"nombre":"San Blas-Canillejas","lat":40.4398,"lon":-3.6153},
    {"nombre":"Barajas","lat":40.4746,"lon":-3.5796}
]

# NUEVO: muestrario de calles reales por distrito (igual que el de luz)
CALLES_MADRID = {
    "Centro": ["C/ Atocha","C/ Mayor","C/ Toledo","C/ Fuencarral","C/ Hortaleza","C/ Preciados","C/ Montera","C/ Segovia"],
    "Arganzuela": ["Pº de las Delicias","C/ Méndez Álvaro","C/ Embajadores"],
    "Retiro": ["Av. Menéndez Pelayo","C/ Doctor Esquerdo","C/ O'Donnell"],
    "Salamanca": ["C/ Serrano","C/ Velázquez","C/ Goya","C/ Príncipe de Vergara"],
    "Chamartín": ["Av. Alberto Alcocer","C/ Padre Damián","C/ Príncipe de Vergara"],
    "Tetuán": ["C/ Bravo Murillo","C/ Marqués de Viana","C/ Orense"],
    "Chamberí": ["C/ Santa Engracia","C/ Luchana","C/ Fuencarral"],
    "Fuencarral-El Pardo": ["Av. Monforte de Lemos","C/ Sinesio Delgado"],
    "Moncloa-Aravaca": ["Pº de la Florida","Av. de Valladolid","C/ Princesa"],
    "Latina": ["Pº de Extremadura","C/ Valmojado","C/ General Fanjul"],
    "Carabanchel": ["C/ General Ricardos","C/ Eugenia de Montijo"],
    "Usera": ["Av. de Rafael Ybarra","Av. de Marcelo Usera"],
    "Puente de Vallecas": ["Av. de la Albufera","C/ Monte Perdido"],
    "Moratalaz": ["C/ Camino de los Vinateros","Av. Moratalaz"],
    "Ciudad Lineal": ["C/ Alcalá","C/ Arturo Soria"],
    "Hortaleza": ["C/ Silvano","C/ López de Hoyos"],
    "Villaverde": ["Av. de Andalucía","C/ Alcocer"],
    "Villa de Vallecas": ["C/ Real de Arganda","Av. del Ensanche de Vallecas"],
    "Vicálvaro": ["C/ San Cipriano","C/ Minerva"],
    "San Blas-Canillejas": ["C/ Alcalá","Av. de Arcentales"],
    "Barajas": ["Av. de Logroño","C/ Galeón"]
}

TIPOS = [
    {"tipo":"mantenimiento","peso":0.50,"duracion_h":(2,6),"programado":True,"mensaje":"Intervención programada en la red de gas."},
    {"tipo":"avería","peso":0.35,"duracion_h":(1,4),"programado":False,"mensaje":"Avería inesperada en la red de gas."},
    {"tipo":"obra pública","peso":0.10,"duracion_h":(4,8),"programado":True,"mensaje":"Corte por trabajos coordinados con el Ayuntamiento."},
    {"tipo":"emergencia","peso":0.05,"duracion_h":(0.5,2),"programado":False,"mensaje":"Incidente urgente en la red."}
]

def _pick_via(distrito_nombre: str) -> str:
    calles = CALLES_MADRID.get(distrito_nombre) or []
    if not calles:
        return f"Simulado - {distrito_nombre}"
    return random.choice(calles)

def _rand_numero() -> str:
    n = random.randint(1, 250)
    suf = random.choices(["", " BIS", " A", " B"], weights=[0.85, 0.05, 0.05, 0.05], k=1)[0]
    return f"{n}{suf}".strip()

def run(output_dir: str | os.PathLike | None = None, dias=7, por_dia=3, seed: int | None = None):
    if seed is not None: random.seed(seed)
    out_dir = Path(output_dir).resolve() if output_dir else DEFAULT_OUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    hoy = datetime.now(timezone.utc)
    fecha_fin = hoy + timedelta(days=dias)
    total = dias * por_dia

    eventos=[]
    for i in range(total):
        tipo = random.choices(TIPOS, weights=[t["peso"] for t in TIPOS])[0]
        d = random.choice(DISTRITOS)

        start = hoy + timedelta(days=random.randint(0, dias-1), hours=random.randint(0,23), minutes=random.randint(0,59))
        fin = start + timedelta(hours=random.uniform(*tipo["duracion_h"]))

        via = _pick_via(d["nombre"])
        numero = _rand_numero()

        eventos.append({
            "event_id": f"gas-next7-{i+1:04d}",
            "tipo": tipo["tipo"],
            "programado": tipo["programado"],
            "direccion": f"{via} {numero}",          
            "via": via,                               
            "numero": numero,                         
            "lat": d["lat"] + random.uniform(-0.002, 0.002),
            "lon": d["lon"] + random.uniform(-0.002, 0.002),
            "start_ts": start.isoformat(),           
            "end_ts": fin.isoformat(),
            "mensaje": tipo["mensaje"]
        })

    fname = f"cortes_gas_{hoy.date()}_al_{fecha_fin.date()}.json"
    (out_dir / fname).write_text(json.dumps(eventos, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[gas] {len(eventos)} simulados -> {fname}")

if __name__ == "__main__":
    run()
