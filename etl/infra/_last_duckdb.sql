INSTALL postgres;
LOAD postgres;

ATTACH 'dbname=appdb user=appuser password=apppass host=localhost port=5433' AS pg (TYPE POSTGRES);
CREATE SCHEMA IF NOT EXISTS pg.staging;

CREATE TABLE IF NOT EXISTS pg.staging.electricity AS
SELECT * FROM read_parquet('C:/Users/diego/Documents/Diego/Master/TFM/Enterate/etl/data_curated/ide/history.parquet') WHERE 1=0;
DELETE FROM pg.staging.electricity;
INSERT INTO pg.staging.electricity
SELECT * FROM read_parquet('C:/Users/diego/Documents/Diego/Master/TFM/Enterate/etl/data_curated/ide/history.parquet');

CREATE TABLE IF NOT EXISTS pg.staging.water AS
SELECT * FROM read_parquet('C:/Users/diego/Documents/Diego/Master/TFM/Enterate/etl/data_curated/canal/history.parquet') WHERE 1=0;
DELETE FROM pg.staging.water;
INSERT INTO pg.staging.water
SELECT * FROM read_parquet('C:/Users/diego/Documents/Diego/Master/TFM/Enterate/etl/data_curated/canal/history.parquet');

CREATE TABLE IF NOT EXISTS pg.staging.road AS
SELECT * FROM read_parquet('C:/Users/diego/Documents/Diego/Master/TFM/Enterate/etl/data_curated/ayto/history.parquet') WHERE 1=0;
DELETE FROM pg.staging.road;
INSERT INTO pg.staging.road
SELECT * FROM read_parquet('C:/Users/diego/Documents/Diego/Master/TFM/Enterate/etl/data_curated/ayto/history.parquet');

CREATE TABLE IF NOT EXISTS pg.staging.gas AS
SELECT * FROM read_parquet('C:/Users/diego/Documents/Diego/Master/TFM/Enterate/etl/data_curated/gas/history.parquet') WHERE 1=0;
DELETE FROM pg.staging.gas;
INSERT INTO pg.staging.gas
SELECT * FROM read_parquet('C:/Users/diego/Documents/Diego/Master/TFM/Enterate/etl/data_curated/gas/history.parquet');

DETACH pg;
