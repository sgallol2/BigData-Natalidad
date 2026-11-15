-- ============================================================
-- 04_catalogar_dimensiones_rds.sql
-- Tablas externas para las dimensiones de apoyo extraídas de RDS MariaDB
-- ============================================================

-- 1. Dimensión Departamentos
CREATE EXTERNAL TABLE IF NOT EXISTS natalidad_db.dim_departamento (
  cod_dpto STRING,
  nombre STRING
)
STORED AS PARQUET
LOCATION 's3://bigdata-p2-190565384867/trusted/rds/dim_departamento/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

-- 2. Dimensión Municipios
CREATE EXTERNAL TABLE IF NOT EXISTS natalidad_db.dim_municipio (
  cod_mpio STRING,
  cod_dpto STRING,
  cod_munic STRING,
  nombre STRING,
  tipo STRING
)
STORED AS PARQUET
LOCATION 's3://bigdata-p2-190565384867/trusted/rds/dim_municipio/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

-- 3. Catálogo de variables DANE (diccionario de códigos)
CREATE EXTERNAL TABLE IF NOT EXISTS natalidad_db.dim_catalogo (
  variable STRING,
  codigo STRING,
  descripcion STRING
)
STORED AS PARQUET
LOCATION 's3://bigdata-p2-190565384867/trusted/rds/dim_catalogo/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');
