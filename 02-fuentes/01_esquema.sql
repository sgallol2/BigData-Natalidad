-- ============================================================
-- 01_esquema.sql
-- Base de datos y tablas de apoyo (dimensiones) para el caso de
-- estudio de natalidad. Motor: MariaDB (Amazon RDS).
-- Es idempotente: se puede ejecutar varias veces.
-- Ejecutar con:
--   mysql -h <DB_HOST> -u admin -p --default-character-set=utf8mb4 < 01_esquema.sql
-- ============================================================

CREATE DATABASE IF NOT EXISTS natalidad
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE natalidad;

-- Departamentos (codigo DIVIPOLA de 2 digitos, como texto para conservar ceros)
CREATE TABLE IF NOT EXISTS dim_departamento (
  cod_dpto CHAR(2)      NOT NULL,
  nombre   VARCHAR(100) NOT NULL,
  PRIMARY KEY (cod_dpto)
) ENGINE=InnoDB;

-- Municipios (cod_mpio = cod_dpto + cod_munic, 5 digitos, como texto)
CREATE TABLE IF NOT EXISTS dim_municipio (
  cod_mpio  CHAR(5)      NOT NULL,
  cod_dpto  CHAR(2)      NOT NULL,
  cod_munic CHAR(3)      NOT NULL,
  nombre    VARCHAR(120) NOT NULL,
  tipo      VARCHAR(40)  NULL,
  PRIMARY KEY (cod_mpio),
  KEY idx_municipio_dpto (cod_dpto),
  CONSTRAINT fk_municipio_dpto
    FOREIGN KEY (cod_dpto) REFERENCES dim_departamento (cod_dpto)
) ENGINE=InnoDB;

-- Catalogo unico de codigos de las variables del DANE:
-- una fila por (variable, codigo). Se une con los datos asi:
--   JOIN dim_catalogo c ON c.variable = 'NIV_EDUM' AND c.codigo = n.NIV_EDUM
CREATE TABLE IF NOT EXISTS dim_catalogo (
  variable    VARCHAR(30)  NOT NULL,
  codigo      VARCHAR(10)  NOT NULL,
  descripcion VARCHAR(150) NOT NULL,
  PRIMARY KEY (variable, codigo)
) ENGINE=InnoDB;




