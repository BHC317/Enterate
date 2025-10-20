# etl/orchestrate/run_load.py
import os
import sys
import pathlib
import subprocess
import duckdb

ROOT = pathlib.Path(__file__).resolve().parents[1]
DUCK_SQL = ROOT / "warehouse" / "duckdb_load.sql"
DBT_DIR = ROOT / "warehouse" / "dbt"

REQUIRED_ENV = ["PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"]


def _check_env():
    missing = [k for k in REQUIRED_ENV if not os.getenv(k)]
    if missing:
        raise SystemExit(
            "Faltan variables de entorno para Postgres: " + ", ".join(missing)
        )


def _ensure_staging_schema():
    """
    Evita errores de 'schema not found' creando el esquema staging si no existe.
    """
    sql = f"""
    INSTALL postgres; LOAD postgres;
    ATTACH 'dbname={os.environ["PGDATABASE"]} user={os.environ["PGUSER"]}
            password={os.environ["PGPASSWORD"]} host={os.environ["PGHOST"]}
            port={os.environ["PGPORT"]}' AS pg (TYPE POSTGRES);
    CREATE SCHEMA IF NOT EXISTS pg.staging;
    DETACH pg;
    """
    duckdb.sql(sql)


def _load_staging_via_duckdb():
    """
    Lee duckdb_load.sql, sustituye variables ${PG...} y crea/reemplaza tablas staging.*
    """
    sql = DUCK_SQL.read_text(encoding="utf-8")
    for k in REQUIRED_ENV:
        sql = sql.replace("${" + k + "}", os.environ[k])
    duckdb.sql(sql)


def _run_dbt_build():
    """
    Ejecuta dbt build sobre el proyecto de etl/warehouse/dbt.
    """
    # Usamos 'python -m dbt' para no depender del wrapper en PATH
    cmd = [sys.executable, "-m", "dbt", "build", "--project-dir", str(DBT_DIR)]
    print("→ Ejecutando:", " ".join(cmd))
    subprocess.run(cmd, check=True)


def main():
    _check_env()
    print("== [LOAD] Asegurando schema staging en Postgres ==")
    _ensure_staging_schema()
    print("== [LOAD] Cargando staging (DuckDB → Postgres) ==")
    _load_staging_via_duckdb()
    print("== [LOAD] Ejecutando dbt build ==")
    _run_dbt_build()
    print("✅ [LOAD] Completado.")


if __name__ == "__main__":
    main()
