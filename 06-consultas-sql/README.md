# Punto 6: Consultas y Procesamiento Analítico Descriptivo con SQL

Este módulo contiene las consultas analíticas en SQL diseñadas para responder a las preguntas de negocio e hipótesis formuladas en el caso de estudio.

Las consultas están escritas en SQL estándar compatible con **Amazon Athena**, **Apache Hive** y **SparkSQL**, aprovechando las tablas catalogadas en el **AWS Glue Data Catalog**.

---

## 1. Preguntas de Negocio y Scripts SQL

| Archivo | Pregunta de Negocio | Concepto Analítico Clave |
|---|---|---|
| [`pregunta_1_evolucion_departamentos.sql`](pregunta_1_evolucion_departamentos.sql) | ¿Cómo evolucionó el número de nacimientos por año y departamento entre 2021 y 2025? | Agrupación temporal y espacial; join con `dim_departamento`; tendencia decreciente general. |
| [`pregunta_2_estacionalidad_meses.sql`](pregunta_2_estacionalidad_meses.sql) | ¿Qué meses concentran más nacimientos en Colombia? | Estacionalidad y agregaciones por mes calendario (`CASE WHEN` para meses). |
| [`pregunta_3_madres_adolescentes.sql`](pregunta_3_madres_adolescentes.sql) | ¿Qué departamentos concentran mayor proporción de madres adolescentes (10 a 19 años)? | Conteo condicional con `CASE WHEN` sobre grupos de edad (`1` y `2`). |
| [`pregunta_4_educacion_controles_parto.sql`](pregunta_4_educacion_controles_parto.sql) | ¿Cómo se relaciona el nivel educativo con los controles prenatales y el tipo de parto? | Decodificación mediante `dim_catalogo`, cálculo de promedios y proporciones de cesárea vs espontáneo. |
| [`pregunta_5_comparativa_banco_mundial.sql`](pregunta_5_comparativa_banco_mundial.sql) | ¿Cómo se compara la tasa de natalidad de Colombia internacionalmente? | Análisis de series temporales de la API Banco Mundial (`WB_WDI_SP_DYN_CBRT_IN`). |

---

## 2. Ejecución en Motores Analíticos

### A. Amazon Athena (Serverless SQL)
1. Abrir la consola de **Athena**.
2. Seleccionar `natalidad_db` como base de datos activa.
3. Abrir y ejecutar cada archivo SQL.
4. Exportar resultados o capturar pantallas de las tablas resultantes como evidencia.

### B. Apache Hive / Hue (en EMR)
Si se dispone de un clúster EMR activo con Hive:
```bash
hive -e "USE natalidad_db; $(cat pregunta_1_evolucion_departamentos.sql)"
```

### C. SparkSQL (PySpark / spark-shell)
```python
spark.sql("USE natalidad_db")
df = spark.sql("SELECT anio, COUNT(*) FROM trusted_nacimientos GROUP BY anio ORDER BY anio")
df.show()
```

---

## 3. Puntos de Sustentación para el Examen Oral

- **¿Por qué las consultas en Athena son rápidas y económicas?**  
  Porque la tabla `trusted_nacimientos` está en formato **Parquet** (columnar) y particionada por `anio`. Athena solo cobra por los bytes leídos de S3; al leer solo las columnas necesarias y usar compresión Snappy, el costo por consulta es de fracciones de centavo y la ejecución toma milisegundos.
- **¿Qué es Partition Pruning?**  
  Si agregamos un filtro como `WHERE anio = 2024`, el motor descarta por completo los directorios `anio=2021`, `anio=2022`, `anio=2023` y `anio=2025`, reduciendo el escaneo de datos en un 80%.
