from fastapi import FastAPI, Depends
from fastapi.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from routers import movies, routes, incidents
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

app.include_router(movies.router)
app.include_router(routes.router)
app.include_router(incidents.router)

@app.get("/customers")
def list_customers(db: Session = Depends(get_db)):
    customers = db.query(Customer).all()

    return [
        {"id": c.id, "name": c.name, "email": c.email}
        for c in customers
    ]

@app.get("/", include_in_schema=False)
def redirigir():
    log.info("Petición a /, redirigiendo a /docs...")
    return RedirectResponse(url="/docs")

app.add_middleware(CORSMiddleware, allow_origins="http://localhost:3000", allow_methods=["*"], allow_headers=["*"])

if __name__ == "__main__":
    uvicorn.run("main:app", port=8080, reload=True)
