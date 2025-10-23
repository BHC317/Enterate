INSTALL postgres;
LOAD postgres;

ATTACH 'dbname=appdb user=appuser password=apppass host=localhost port=5433' AS pg (TYPE POSTGRES);
CREATE SCHEMA IF NOT EXISTS pg.staging;

DROP TABLE IF EXISTS pg.staging.electricity CASCADE;
CREATE TABLE pg.staging.electricity AS
SELECT * FROM read_parquet('C:/Users/diego/Documents/Diego/Master/TFM/Enterate/etl/data_curated/ide/history.parquet') WHERE 1=0;
INSERT INTO pg.staging.electricity
SELECT * FROM read_parquet('C:/Users/diego/Documents/Diego/Master/TFM/Enterate/etl/data_curated/ide/history.parquet');

DROP TABLE IF EXISTS pg.staging.water CASCADE;
CREATE TABLE pg.staging.water AS
SELECT * FROM read_parquet('C:/Users/diego/Documents/Diego/Master/TFM/Enterate/etl/data_curated/canal/history.parquet') WHERE 1=0;
INSERT INTO pg.staging.water
SELECT * FROM read_parquet('C:/Users/diego/Documents/Diego/Master/TFM/Enterate/etl/data_curated/canal/history.parquet');

DROP TABLE IF EXISTS pg.staging.road CASCADE;
CREATE TABLE pg.staging.road AS
SELECT * FROM read_parquet('C:/Users/diego/Documents/Diego/Master/TFM/Enterate/etl/data_curated/ayto/history.parquet') WHERE 1=0;
INSERT INTO pg.staging.road
SELECT * FROM read_parquet('C:/Users/diego/Documents/Diego/Master/TFM/Enterate/etl/data_curated/ayto/history.parquet');

DROP TABLE IF EXISTS pg.staging.gas CASCADE;
CREATE TABLE pg.staging.gas AS
SELECT * FROM read_parquet('C:/Users/diego/Documents/Diego/Master/TFM/Enterate/etl/data_curated/gas/history.parquet') WHERE 1=0;
INSERT INTO pg.staging.gas
SELECT * FROM read_parquet('C:/Users/diego/Documents/Diego/Master/TFM/Enterate/etl/data_curated/gas/history.parquet');

DETACH pg;
