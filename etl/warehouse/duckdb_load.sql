INSTALL postgres;
LOAD postgres;

ATTACH 'dbname=${PGDATABASE} user=${PGUSER} password=${PGPASSWORD} host=${PGHOST} port=${PGPORT}' AS pg (TYPE POSTGRES);

-- ELECTRICITY
CREATE TABLE IF NOT EXISTS pg.staging.electricity AS
SELECT * FROM read_parquet('etl/data_curated/ide/history.parquet');
DELETE FROM pg.staging.electricity;
INSERT INTO pg.staging.electricity
SELECT * FROM read_parquet('etl/data_curated/ide/history.parquet');

-- WATER
CREATE TABLE IF NOT EXISTS pg.staging.water AS
SELECT * FROM read_parquet('etl/data_curated/canal/history.parquet');
DELETE FROM pg.staging.water;
INSERT INTO pg.staging.water
SELECT * FROM read_parquet('etl/data_curated/canal/history.parquet');

-- ROAD
CREATE TABLE IF NOT EXISTS pg.staging.road AS
SELECT * FROM read_parquet('etl/data_curated/ayto/history.parquet');
DELETE FROM pg.staging.road;
INSERT INTO pg.staging.road
SELECT * FROM read_parquet('etl/data_curated/ayto/history.parquet');

-- GAS
CREATE TABLE IF NOT EXISTS pg.staging.gas AS
SELECT * FROM read_parquet('etl/data_curated/gas/history.parquet');
DELETE FROM pg.staging.gas;
INSERT INTO pg.staging.gas
SELECT * FROM read_parquet('etl/data_curated/gas/history.parquet');

DETACH pg;
