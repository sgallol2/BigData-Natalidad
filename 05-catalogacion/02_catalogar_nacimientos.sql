-- ============================================================
-- 02_catalogar_nacimientos.sql
-- Tabla externa para microdatos de nacimientos (DANE 2021-2025)
-- Particionada por anio en S3. Formato columnar Parquet.
-- ============================================================

CREATE EXTERNAL TABLE IF NOT EXISTS natalidad_db.trusted_nacimientos (
  cod_dpto STRING,
  cod_munic STRING,
  areanac STRING,
  sit_parto STRING,
  numconsul INT,
  tipo_parto STRING,
  mul_parto STRING,
  apgar1 INT,
  apgar2 INT,
  idhemoclas STRING,
  idfactorrh STRING,
  peso_nac STRING,
  talla_nac STRING,
  idpertet STRING,
  edad_madre STRING,
  est_civm STRING,
  niv_edum STRING,
  ultcurmad STRING,
  codptore STRING,
  codmunre STRING,
  area_res STRING,
  n_hijosv INT,
  n_emb INT,
  seg_social STRING,
  idclasadmi STRING,
  edad_padre INT,
  niv_edup STRING,
  ultcurpad STRING,
  profesion_p STRING,
  t_ges STRING,
  t_ges_agru_cie STRING,
  codpaisnacmad STRING,
  cod_mpio_ocu STRING,
  cod_mpio_res STRING,
  mes INT
)
PARTITIONED BY (anio INT)
STORED AS PARQUET
LOCATION 's3://bigdata-p2-190565384867/trusted/nacimientos/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

-- IMPORTANTE: Descubrir y cargar las particiones existentes en S3 (anio=2021 .. anio=2025)
MSCK REPAIR TABLE natalidad_db.trusted_nacimientos;
