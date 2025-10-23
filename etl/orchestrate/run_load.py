import os
import subprocess
from pathlib import Path
from textwrap import dedent
import duckdb
from dotenv import load_dotenv

ETL_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = ETL_ROOT.parent
CURATED = ETL_ROOT / "data_curated"
INFRA_DIR = ETL_ROOT / "infra"
DOCKER_COMPOSE_FILE = ETL_ROOT / "infra" / "docker-compose.yml"
LAST_SQL = INFRA_DIR / "_last_duckdb.sql"
DBT_PROJECT_DIR = ETL_ROOT / "warehouse" / "dbt"
DBT_PROFILES_DIR = Path(os.path.expanduser("~")) / ".dbt"
DBT_PROFILES_YML = DBT_PROFILES_DIR / "profiles.yml"
REQUIRED_ENV = ["PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"]

DEFAULT_PG = {
    "PGHOST": "localhost",
    "PGPORT": "5433",
    "PGDATABASE": "appdb",
    "PGUSER": "appuser",
    "PGPASSWORD": "apppass",
}

def sh(cmd: list[str], check: bool = True) -> int:
    print("==> $", " ".join(cmd))
    proc = subprocess.run(cmd)
    if check and proc.returncode != 0:
        raise SystemExit(proc.returncode)
    return proc.returncode

def load_env_defaults():
    load_dotenv(REPO_ROOT / ".env")
    for k, v in DEFAULT_PG.items():
        os.environ.setdefault(k, v)

def ensure_env():
    print("== [LOAD] Verificando variables de entorno ==")
    load_env_defaults()
    missing = [k for k in REQUIRED_ENV if not os.getenv(k)]
    if missing:
        for k, v in DEFAULT_PG.items():
            os.environ[k] = v
        print("== [LOAD] Variables de entorno configuradas con valores por defecto ==")

def ensure_postgres_up():
    print("== [LOAD] Levantando Postgres con Docker Compose ==")
    sh(["docker", "compose", "-f", str(DOCKER_COMPOSE_FILE), "up", "-d"], check=False)

def ensure_schemas():
    print("== [LOAD] Asegurando schemas staging/analytics ==")
    psql_cmd = (
        "CREATE SCHEMA IF NOT EXISTS staging AUTHORIZATION appuser;"
        "CREATE SCHEMA IF NOT EXISTS analytics AUTHORIZATION appuser;"
        "CREATE SCHEMA IF NOT EXISTS analytics_staging AUTHORIZATION appuser;"
    )
    sh(["docker", "exec", "-i", "enterate-postgres",
        "psql", "-U", "appuser", "-d", "appdb", "-c", psql_cmd], check=False)

def ensure_profiles_yml():
    print(f"== [LOAD] profiles.yml en {DBT_PROFILES_YML} ==")
    DBT_PROFILES_DIR.mkdir(parents=True, exist_ok=True)
    if DBT_PROFILES_YML.exists():
        print(f"profiles.yml ya existe en {DBT_PROFILES_YML}")
        return
    yml = dedent("""
    enterate:
      target: dev
      outputs:
        dev:
          type: postgres
          host: "{{ env_var('PGHOST') }}"
          user: "{{ env_var('PGUSER') }}"
          password: "{{ env_var('PGPASSWORD') }}"
          dbname: "{{ env_var('PGDATABASE') }}"
          schema: analytics
          port: "{{ env_var('PGPORT') | int }}"
    """).lstrip()
    DBT_PROFILES_YML.write_text(yml, encoding="utf-8")

def _parquet(rel_path: Path) -> str:
    p = rel_path.resolve()
    if not p.exists():
        raise SystemExit(f"No existe el Parquet: {p}")
    return p.as_posix()

def build_duckdb_sql() -> str:
    pg_host = os.environ["PGHOST"]
    pg_port = os.environ["PGPORT"]
    pg_db = os.environ["PGDATABASE"]
    pg_user = os.environ["PGUSER"]
    pg_pass = os.environ["PGPASSWORD"]

    p_elec = _parquet(CURATED / "ide" / "history.parquet")
    p_water = _parquet(CURATED / "canal" / "history.parquet")
    p_road = _parquet(CURATED / "ayto" / "history.parquet")
    p_gas = _parquet(CURATED / "gas" / "history.parquet")

    sql = f"""
    INSTALL postgres;
    LOAD postgres;

    ATTACH 'dbname={pg_db} user={pg_user} password={pg_pass} host={pg_host} port={pg_port}' AS pg (TYPE POSTGRES);
    CREATE SCHEMA IF NOT EXISTS pg.staging;

    DROP TABLE IF EXISTS pg.staging.electricity CASCADE;
    CREATE TABLE pg.staging.electricity AS
    SELECT * FROM read_parquet('{p_elec}') WHERE 1=0;
    INSERT INTO pg.staging.electricity
    SELECT * FROM read_parquet('{p_elec}');

    DROP TABLE IF EXISTS pg.staging.water CASCADE;
    CREATE TABLE pg.staging.water AS
    SELECT * FROM read_parquet('{p_water}') WHERE 1=0;
    INSERT INTO pg.staging.water
    SELECT * FROM read_parquet('{p_water}');

    DROP TABLE IF EXISTS pg.staging.road CASCADE;
    CREATE TABLE pg.staging.road AS
    SELECT * FROM read_parquet('{p_road}') WHERE 1=0;
    INSERT INTO pg.staging.road
    SELECT * FROM read_parquet('{p_road}');

    DROP TABLE IF EXISTS pg.staging.gas CASCADE;
    CREATE TABLE pg.staging.gas AS
    SELECT * FROM read_parquet('{p_gas}') WHERE 1=0;
    INSERT INTO pg.staging.gas
    SELECT * FROM read_parquet('{p_gas}');

    DETACH pg;
    """
    return dedent(sql).strip() + "\n"

def load_staging_inline():
    print("== [LOAD] Cargando staging (DuckDB → Postgres) ==")
    sql = build_duckdb_sql()
    print("== [LOAD] SQL inline (sin DROP) ==\n")
    print(sql)
    INFRA_DIR.mkdir(parents=True, exist_ok=True)
    LAST_SQL.write_text(sql, encoding="utf-8")
    duckdb.sql(sql)
    print("== [LOAD] staging cargado correctamente ==")

def run_dbt_build():
    print("== [LOAD] Ejecutando dbt build ==")
    sh(["dbt", "build", "--project-dir", str(DBT_PROJECT_DIR)])

def main():
    ensure_env()
    ensure_postgres_up()
    ensure_schemas()
    ensure_profiles_yml()
    load_staging_inline()
    run_dbt_build()
    print("== [LOAD] OK ==")

if __name__ == "__main__":
    main()
