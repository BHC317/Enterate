# ENTÉRATE API · Proyecto Final de Ingeniería de Datos

Repositorio de un proyecto de **Ingeniería de Datos** que combina:

- **API** desarrollada con **FastAPI**, contenedorizada junto con **PostgreSQL** vía **Docker Compose**.
- **Pipeline ETL** en la carpeta `etl/` para **extracción y carga** usando **dbt** hacia la **misma base de datos** de PostgreSQL.  

## Tabla de contenidos

1. [Arquitectura](#arquitectura)
2. [Requisitos](#requisitos)
4. [Variables de entorno](#variables-de-entorno)
5. [Puesta en marcha (Docker Compose)](#puesta-en-marcha-docker-compose)
6. [API (FastAPI)](#api-fastapi)
7. [Base de datos (PostgreSQL)](#base-de-datos-postgresql)
8. [ETL con dbt](#etl-con-dbt)

---

## Arquitectura

```
┌───────────┐        requests         ┌─────────────┐
│  Cliente  ├────────────────────────▶│   FastAPI   │
└───────────┘                         │ (app/)      │
                                      └─────┬───────┘
                                            │ SQL (read/write)
                                            ▼
                                      ┌─────────────┐
                                      │ PostgreSQL  │
                                      │ (docker)    │
                                      └─────┬───────┘
                                            │
                              cargas (dbt)  ▼
                                      ┌─────────────┐
                                      │   ETL/dbt   │
                                      │ (etl/)      │
                                      └─────────────┘
```

- La **API** consulta tablas materializadas por el **ETL**.  
- El **ETL (dbt)** carga y transforma datos en el **mismo PostgreSQL**.

---

## Requisitos

- **Docker** ≥ 24.x y **Docker Compose** ≥ 2.x
- **Git** ≥ 2.x
- (Opcional para desarrollo local) **Python** ≥ 3.11 y **Poetry** o **pip**

---

## Variables de entorno

Crea un archivo `.env` en la raíz (puedes basarte en `.env.example`):

```env
# App
API_HOST=0.0.0.0 (localhost)
API_PORT=8001


# DB
POSTGRES_DB=appdb
POSTGRES_USER=appuser
POSTGRES_PASSWORD=apppass
POSTGRES_HOST=db
POSTGRES_PORT=5432

# Para FastAPI/SQLAlchemy
DATABASE_URL=postgresql+psycopg2://appuser:apppass@db:5432/appdb
```

---

## Puesta en marcha (Docker Compose)

Construir e iniciar en segundo plano:

```bash
docker compose up -d --build
```

Ver logs:

```bash
docker compose logs -f api
```

Detener:

```bash
docker compose down
```

Recrear sólo la API (p. ej., tras cambios de código):

```bash
docker compose up -d --build api
```

---

## API (FastAPI)

- URL base (local): `http://localhost:8001`
- Documentación interactiva:
  - Swagger UI: `http://localhost:8001/docs`
  - Redoc: `http://localhost:8001/redoc`

Ejemplo de health check:

```bash
curl http://localhost:8001/health
```

> La API expone endpoints que **leen** las tablas **cargadas por el ETL dbt**. Si aún no se han cargado datos, algunas respuestas pueden venir vacías.

---

## Base de datos (PostgreSQL)

- Host local: `localhost`
- Puerto: `5433`
- DB: `appdb`
- Usuario: `appuser` / Password: `apppass` (ajustar en `.env` para producción)


Extensiones y esquemas (opcional): si usas **PostGIS** u otros, agrega scripts a `docker/db/init.sql` y monta ese archivo en `docker-compose.yml` para que se apliquen al iniciar.

---

## ETL con dbt

El directorio `etl/` contiene el proyecto dbt que **extrae/transforma/carga** datos en **PostgreSQL** (mismo `POSTGRES_DB`).  
