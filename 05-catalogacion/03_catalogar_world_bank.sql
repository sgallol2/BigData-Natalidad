-- ============================================================
-- 03_catalogar_world_bank.sql
-- Tabla externa para indicadores del Banco Mundial (Data360 API)
-- Formato Parquet consolidado (coalesce 1)
-- ============================================================

CREATE EXTERNAL TABLE IF NOT EXISTS natalidad_db.trusted_world_bank (
  indicador STRING,
  pais STRING,
  anio INT,
  valor DOUBLE
)
STORED AS PARQUET
LOCATION 's3://bigdata-p2-190565384867/trusted/world_bank/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');
