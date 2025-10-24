from fastapi import FastAPI, Depends
from fastapi.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from routers import routes, routes_ads, incidents
from logger import log
from sqlalchemy.orm import Session
from database import Base, engine, get_db
from models.models import Customer

descripcion = "Enterate: API REST"
    
app = FastAPI(
    description=descripcion,
    version="0.1.0",
    title="ENTERATE - API REST",
    license_info={
        "name": "GPLv3",
        "url": "https://www.gnu.org/licenses/gpl-3.0.en.html", 
    }, 
    openapi_tags= [ {
                        "name": "Enterate API",
                        "description": "Enterate API"
                    }                   
                ]
)

app.include_router(routes.router)
app.include_router(routes_ads.router)
app.include_router(incidents.router)

@app.on_event("startup")
def init_db():
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE EXTENSION IF NOT EXISTS postgis")
        schemas = ["staging", "analytics_analytics"]
        for schema in schemas:
            conn.exec_driver_sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    # Crear tablas si no existen (al cargar el módulo)
    Base.metadata.create_all(bind=engine)

@app.get("/", include_in_schema=False)
def redirigir():
    log.info("Petición a /, redirigiendo a /docs...")
    return RedirectResponse(url="/docs")

app.add_middleware(CORSMiddleware, allow_origins="http://localhost:3000", allow_methods=["*"], allow_headers=["*"])

if __name__ == "__main__":
    uvicorn.run("main:app", port=8080, reload=True)
