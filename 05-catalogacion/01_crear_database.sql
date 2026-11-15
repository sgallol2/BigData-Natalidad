-- ============================================================
-- 01_crear_database.sql
-- Crear la base de datos en AWS Glue Data Catalog / Amazon Athena
-- ============================================================

CREATE DATABASE IF NOT EXISTS natalidad_db
COMMENT 'Base de datos catalogada para el proyecto de Natalidad DANE y Banco Mundial'
LOCATION 's3://bigdata-p2-190565384867/trusted/';
