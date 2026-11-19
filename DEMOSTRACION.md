# Guía demostrativa del pipeline de Big Data

> Esta guía asume que el bucket, EC2, RDS y roles de AWS ya existen. No se deben recrear durante la demostración.

## 1. Preparar variables

En una consola **CMD de Windows**:

```bat
call vars.cmd

aws sts get-caller-identity
aws s3 ls s3://%BUCKET%/
```

Verificar recursos:

```bat
aws ec2 describe-instances ^
  --instance-ids %EC2_ID% ^
  --query "Reservations[0].Instances[0].{State:State.Name,IP:PublicIpAddress}" ^
  --output table

aws rds describe-db-instances ^
  --db-instance-identifier %DB_ID% ^
  --query "DBInstances[0].{Status:DBInstanceStatus,Endpoint:Endpoint.Address}" ^
  --output table

aws emr list-clusters --active
```

---

## 2. Mostrar la ingesta hacia `raw`

Conectarse a la EC2:

```bat
ssh -i %KEY_FILE% ubuntu@%EC2_IP%
```

En la EC2, definir las variables:

```bash
export AWS_DEFAULT_REGION=us-east-1
export BUCKET=bigdata-p2-ACCOUNT_ID
export DB_HOST=ENDPOINT_RDS
export DB_USER=admin
export DB_NAME=natalidad
export DB_PASS='CONTRASENA'
```

Ejecutar la ingesta:

```bash
cd ~/BigData-Natalidad/03-ingesta

bash ingesta_archivos.sh
bash ingesta_rds.sh
python3 ingesta_api.py
```

Verificar la zona `raw`:

```bash
aws s3 ls s3://$BUCKET/raw/ --recursive
```

Se deben observar rutas como:

```text
raw/archivos/nacimientos/anio=2021/nacimientos_2021.csv
raw/archivos/nacimientos/anio=2022/nacimientos_2022.csv
raw/rds/dim_departamento.tsv
raw/rds/dim_municipio.tsv
raw/rds/dim_catalogo.tsv
raw/url/WB_WDI_SP_DYN_CBRT_IN/REF_AREA=COL/page-000000.json
```

La ingesta está documentada en `03-ingesta/README.md`.

---

## 3. Subir los scripts de Glue

Desde Windows, en la raíz del proyecto:

```bat
aws s3 cp 04-glue-spark\glue_raw_a_trusted_nacimientos.py ^
  s3://%BUCKET%/scripts/glue_raw_a_trusted_nacimientos.py

aws s3 cp 04-glue-spark\glue_raw_a_trusted_api.py ^
  s3://%BUCKET%/scripts/glue_raw_a_trusted_api.py

aws s3 cp 04-glue-spark\glue_raw_a_trusted_rds.py ^
  s3://%BUCKET%/scripts/glue_raw_a_trusted_rds.py
```

---

## 4. Crear los Glue Jobs

Esta operación se realiza **una sola vez**.

```bat
aws glue create-job ^
  --name %PROJECT%-nacimientos-trusted ^
  --role %LAB_ROLE_ARN% ^
  --glue-version 4.0 ^
  --command Name=glueetl,ScriptLocation=s3://%BUCKET%/scripts/glue_raw_a_trusted_nacimientos.py,PythonVersion=3 ^
  --worker-type G.1X ^
  --number-of-workers 2
```

Para API:

```bat
aws glue create-job ^
  --name %PROJECT%-api-trusted ^
  --role %LAB_ROLE_ARN% ^
  --glue-version 4.0 ^
  --command Name=glueetl,ScriptLocation=s3://%BUCKET%/scripts/glue_raw_a_trusted_api.py,PythonVersion=3 ^
  --worker-type G.1X ^
  --number-of-workers 2
```

Para RDS:

```bat
aws glue create-job ^
  --name %PROJECT%-rds-trusted ^
  --role %LAB_ROLE_ARN% ^
  --glue-version 4.0 ^
  --command Name=glueetl,ScriptLocation=s3://%BUCKET%/scripts/glue_raw_a_trusted_rds.py,PythonVersion=3 ^
  --worker-type G.1X ^
  --number-of-workers 2
```

Si los jobs ya existen, no ejecutar nuevamente `create-job`. En ese caso se usa `start-job-run`.

---

## 5. Ejecutar Glue: `raw` a `trusted`

Ejecutar el job de nacimientos:

```bat
aws glue start-job-run ^
  --job-name %PROJECT%-nacimientos-trusted ^
  --arguments "{\"--BUCKET\":\"%BUCKET%\"}"
```

Ejecutar el job de API:

```bat
aws glue start-job-run ^
  --job-name %PROJECT%-api-trusted ^
  --arguments "{\"--BUCKET\":\"%BUCKET%\"}"
```

Ejecutar el job de RDS:

```bat
aws glue start-job-run ^
  --job-name %PROJECT%-rds-trusted ^
  --arguments "{\"--BUCKET\":\"%BUCKET%\"}"
```

Consultar ejecuciones:

```bat
aws glue get-job-runs ^
  --job-name %PROJECT%-nacimientos-trusted ^
  --query "JobRuns[].[Id,JobRunState]" ^
  --output table
```

Consultar el resultado de una ejecución:

```bat
aws glue get-job-run ^
  --job-name %PROJECT%-nacimientos-trusted ^
  --run-id ID_DEL_RUN ^
  --query "JobRun.[JobRunState,ErrorMessage]" ^
  --output table
```

Esperar hasta obtener:

```text
SUCCEEDED
```

Verificar la zona `trusted`:

```bat
aws s3 ls s3://%BUCKET%/trusted/ --recursive
```

La salida esperada es similar a:

```text
trusted/nacimientos/anio=2021/part-....snappy.parquet
trusted/nacimientos/anio=2022/part-....snappy.parquet
trusted/world_bank/part-....snappy.parquet
trusted/rds/dim_departamento/part-....snappy.parquet
```

Los jobs están en `04-glue-spark/`.

---

## 6. Catalogar las tablas

Se debe escoger **un solo método**: DDL manual o Glue Crawler.

### Opción A: DDL manual en Athena

Ejecutar los archivos:

```text
05-catalogacion/01_crear_database.sql
05-catalogacion/02_catalogar_nacimientos.sql
05-catalogacion/03_catalogar_world_bank.sql
05-catalogacion/04_catalogar_dimensiones_rds.sql
```

Antes de ejecutarlos, verificar que la ubicación `s3://...` corresponda al bucket actual.

Para las tablas de nacimientos, ejecutar:

```sql
MSCK REPAIR TABLE natalidad_db.trusted_nacimientos;
```

Esto registra las particiones `anio=2021` hasta `anio=2025`.

### Verificar el catálogo

En Athena:

```sql
SHOW TABLES IN natalidad_db;
```

```sql
SHOW PARTITIONS natalidad_db.trusted_nacimientos;
```

La catalogación está documentada en `05-catalogacion/README.md`.

---

## 7. Ejecutar una consulta en Athena

Consulta de conteo por año:

```sql
SELECT
    anio,
    COUNT(*) AS total_nacimientos
FROM natalidad_db.trusted_nacimientos
GROUP BY anio
ORDER BY anio;
```

Consulta para demostrar *partition pruning*:

```sql
SELECT COUNT(*) AS nacimientos_2024
FROM natalidad_db.trusted_nacimientos
WHERE anio = 2024;
```

Consulta con dimensión:

```sql
SELECT
    n.anio,
    d.nombre AS departamento,
    COUNT(*) AS total_nacimientos
FROM natalidad_db.trusted_nacimientos n
LEFT JOIN natalidad_db.dim_departamento d
    ON n.cod_dpto = d.cod_dpto
GROUP BY n.anio, d.nombre
ORDER BY n.anio, total_nacimientos DESC;
```

Las consultas completas están en `06-consultas-sql/`.

---

## 8. Ejecutar el análisis PySpark en EMR

Verificar que el clúster esté disponible:

```bat
aws emr describe-cluster ^
  --cluster-id %EMR_CLUSTER_ID% ^
  --query "Cluster.Status.State" ^
  --output text
```

Cuando esté en estado `WAITING`, conectarse al nodo principal:

```bat
ssh -i %KEY_FILE% hadoop@DNS_DEL_MASTER_EMR
```

Copiar el script:

```bash
aws s3 cp s3://$BUCKET/scripts/analisis_descriptivo_pyspark.py .
```

Ejecutarlo:

```bash
export BUCKET=bigdata-p2-ACCOUNT_ID

spark-submit \
  --deploy-mode client \
  analisis_descriptivo_pyspark.py
```

Verificar los resultados:

```bash
aws s3 ls s3://$BUCKET/refined/ --recursive
```

Se esperan estas salidas:

```text
refined/p1_evolucion_departamentos/
refined/p2_estacionalidad_meses/
refined/p3_madres_adolescentes/
refined/p4_educacion_parto/
refined/p5_comparativa_wb/
```

El análisis está en `07-pyspark/analisis_descriptivo_pyspark.py`.

---

## 9. Qué operaciones son seguras de repetir

| Operación | Resultado al repetirla |
|---|---|
| `aws s3 sync` con los mismos archivos | No crea duplicados; sincroniza cambios |
| `aws s3 cp` sobre la misma clave | Sobrescribe el mismo objeto |
| `python3 ingesta_api.py` | Sobrescribe las mismas páginas |
| `start-job-run` de Glue | Reprocesa la salida con `overwrite` |
| `MSCK REPAIR TABLE` | Actualiza o descubre particiones |
| Consultas `SELECT` | No modifican los datos |
| Análisis PySpark | Reemplaza `refined` por usar `overwrite` |

---

## 10. Qué no se debe repetir

No ejecutar nuevamente:

```bat
aws s3api create-bucket ...
aws ec2 run-instances ...
aws rds create-db-instance ...
aws emr create-cluster ...
aws glue create-job ...
```

Pueden crear recursos duplicados, producir errores o generar costos adicionales.

Tampoco se debe:

- Ejecutar dos jobs de Glue simultáneamente sobre la misma salida.
- Cambiar `overwrite` por `append` al reprocesar los mismos datos.
- Copiar el mismo CSV con nombres diferentes dentro de la misma ruta.
- Ejecutar DDL y crawler con esquemas contradictorios.
- Eliminar EMR antes de confirmar que los resultados están en S3.
- Reingerir la API sin considerar que podrían quedar páginas antiguas.
- Borrar `raw`, porque es la evidencia original de la ingesta.

---

## 11. Finalizar la demostración

Confirmar que los resultados están en S3:

```bat
aws s3 ls s3://%BUCKET%/trusted/ --recursive
aws s3 ls s3://%BUCKET%/refined/ --recursive
```

Terminar EMR para evitar costos:

```bat
aws emr terminate-clusters --cluster-ids %EMR_CLUSTER_ID%
```

No eliminar el bucket si se desea conservar la demostración o repetir el flujo.