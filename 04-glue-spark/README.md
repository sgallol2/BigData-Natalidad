# Preparación de datos con AWS Glue y Spark

## Conceptos para la sustentación

### ¿Qué resuelve la zona `trusted`?

`raw` conserva los datos tal como llegaron. `trusted` contiene datos
tipificados, limpiados y validados para que Athena, Spark y los análisis
posteriores trabajen con un esquema confiable. Este paso corresponde a la
preparación de datos de una arquitectura medallion (`bronze` a `silver`).

### ¿Por qué AWS Glue y Apache Spark?

Glue ofrece Spark administrado, por lo que no es necesario mantener
manualmente un clúster para ejecutar las transformaciones. Spark divide el
trabajo en particiones y puede escalar horizontalmente agregando workers.
Además, el mismo código puede procesar archivos locales de prueba o rutas S3.

El volumen es moderado (2,6 millones de registros, 424 MB), así que un solo
equipo podría procesarlo. Lo elegimos porque cumple el objetivo del proyecto:
practicar un pipeline de Big Data con múltiples fuentes y formatos, calidad de
datos real y una arquitectura que escala sin cambiar el diseño: si tuviéramos
100 veces más datos, solo añadiríamos workers.

### Calidad y evolución del esquema

Los CSV de nacimientos no tienen exactamente el mismo esquema todos los años.
El job lee cada año por separado, valida el conteo y el año, y usa
`unionByName` para no depender de la posición de las columnas. Los códigos
geográficos se mantienen como texto para no perder ceros a la izquierda.

Los valores que representan “sin información” se convierten en nulos según la
variable. Las columnas numéricas se convierten después de la limpieza, y los
controles de calidad quedan registrados en los logs de Glue.

### Parquet, particiones y archivos pequeños

Parquet es columnar, comprimido y conserva el esquema; por eso es más
apropiado que CSV para consultas analíticas. Los nacimientos se particionan
solo por `anio`: `mes` queda como columna normal y el filtro por año permite
*partition pruning*, evitando leer otros años.

La salida de World Bank se consolida con `coalesce(1)` y no usa
`partitionBy`, porque son pocos registros y crear un archivo por indicador y
país produciría archivos pequeños. Esta decisión es adecuada para este
volumen; con un crecimiento grande habría que reconsiderar el tamaño de
partición y el paralelismo.

Este módulo transforma los microdatos de nacimientos del DANE desde la zona
`raw` hacia la zona `trusted` del data lake en Amazon S3.

Están implementados jobs para los archivos CSV de nacimientos de 2021 a 2025,
los indicadores de la API Data360 y las tablas de apoyo de RDS.

## Job implementado

Archivo del job:

```text
glue_raw_a_trusted_nacimientos.py
```

El job espera los archivos en las siguientes rutas:

```text
s3://<BUCKET>/raw/archivos/nacimientos/anio=2021/nacimientos_2021.csv
s3://<BUCKET>/raw/archivos/nacimientos/anio=2022/nacimientos_2022.csv
s3://<BUCKET>/raw/archivos/nacimientos/anio=2023/nacimientos_2023.csv
s3://<BUCKET>/raw/archivos/nacimientos/anio=2024/nacimientos_2024.csv
s3://<BUCKET>/raw/archivos/nacimientos/anio=2025/nacimientos_2025.csv
```

Los encabezados deben contener las columnas `ANO` y `MES`.

### Transformaciones realizadas

El job:

1. Lee cada año por separado.
2. Valida el número esperado de filas y que `ANO` coincida con la carpeta.
3. Une los DataFrames por nombre de columna.
4. Limpia espacios y convierte cadenas vacías en valores nulos.
5. Reemplaza códigos de “sin información” según la variable.
6. Convierte las columnas numéricas configuradas a enteros.
7. Conserva como texto los códigos geográficos para mantener ceros a la izquierda.
8. Crea `cod_mpio_ocu` y `cod_mpio_res`.
9. Crea las columnas de partición `anio` y `mes`.
10. Reparticiona por `anio` y escribe archivos Parquet particionados solo por
    `anio`; `mes` queda como columna normal de tipo entero.
11. Registra controles de calidad en los logs de AWS Glue.

La salida queda en:

```text
s3://<BUCKET>/trusted/nacimientos/
```

Ejemplo de estructura:

```text
trusted/nacimientos/
├── anio=2021/
│   └── part-*.snappy.parquet
├── anio=2024/
│   └── part-*.snappy.parquet
└── ...
```

La partición se hace solo por `anio` para evitar cientos de archivos pequeños
por mes y conservar el *partition pruning* cuando las consultas filtran por
año. La columna `mes` sigue disponible dentro de cada archivo Parquet.

## Requisitos

- AWS CLI configurada con credenciales válidas.
- Permisos para S3 y AWS Glue.
- Región configurada como `us-east-1`.
- Los archivos CSV cargados previamente en `raw/archivos/nacimientos/`.
- Variables definidas en `vars.cmd`.

Desde una consola **CMD**, cargar las variables:

```cmd
call vars.cmd
```

## Ejecución en AWS Glue

### 1. Subir el script a S3

```cmd
aws s3 cp 04-glue-spark\glue_raw_a_trusted_nacimientos.py s3://%BUCKET%/scripts/glue_raw_a_trusted_nacimientos.py
```

Para confirmar que se subió la versión correcta:

```cmd
aws s3 cp s3://%BUCKET%/scripts/glue_raw_a_trusted_nacimientos.py glue_s3_check.py
findstr /n "mes" glue_s3_check.py
```

### 2. Crear el job

```cmd
aws glue create-job ^
  --name %PROJECT%-nacimientos-trusted ^
  --role %LAB_ROLE_ARN% ^
  --glue-version 4.0 ^
  --command Name=glueetl,ScriptLocation=s3://%BUCKET%/scripts/glue_raw_a_trusted_nacimientos.py,PythonVersion=3 ^
  --worker-type G.1X ^
  --number-of-workers 2
```

Este comando solo crea la definición del job; todavía no lo ejecuta.

### 3. Ejecutar el job

En Windows CMD, pasar los argumentos como JSON evita que `--BUCKET` se
interprete como una opción adicional de AWS CLI:

```cmd
aws glue start-job-run ^
  --job-name %PROJECT%-nacimientos-trusted ^
  --arguments "{\"--BUCKET\":\"%BUCKET%\"}"
```

La respuesta contiene un identificador similar a:

```json
{
    "JobRunId": "jr_..."
}
```

Guardar ese valor para consultar el run.

### 4. Consultar el estado

Reemplazar `ID_DEL_RUN` por el identificador real:

```cmd
aws glue get-job-run ^
  --job-name %PROJECT%-nacimientos-trusted ^
  --run-id ID_DEL_RUN ^
  --query "JobRun.[JobRunState,ErrorMessage]" ^
  --output text
```

Estados importantes:

- `RUNNING`: el job sigue procesando datos.
- `SUCCEEDED None`: terminó correctamente.
- `FAILED`: terminó con error; consultar `ErrorMessage`.

Para ver el error completo:

```cmd
aws glue get-job-run ^
  --job-name %PROJECT%-nacimientos-trusted ^
  --run-id ID_DEL_RUN ^
  --query "JobRun.ErrorMessage" ^
  --output text
```

### 5. Verificar la salida en S3

```cmd
aws s3 ls s3://%BUCKET%/trusted/nacimientos/ --recursive
```

La salida debe contener archivos `.parquet` dentro de carpetas
`anio=AAAA/mes=M`.

## Job de API Data360

Archivo:

```text
glue_raw_a_trusted_api.py
```

Lee las respuestas JSON de ambos indicadores desde `raw/url/`, extrae la
colección `value`, valida `INDICATOR`, `REF_AREA`, `TIME_PERIOD` y `OBS_VALUE`,
y escribe:

```text
s3://<BUCKET>/trusted/world_bank/
└── part-*.snappy.parquet
```

El job usa `coalesce(1)` y no usa `partitionBy`: `indicador` y `pais` quedan
como columnas normales. Esto evita los 12 archivos pequeños que producía la
partición por indicador y país.

```cmd
aws s3 cp 04-glue-spark\glue_raw_a_trusted_api.py s3://%BUCKET%/scripts/glue_raw_a_trusted_api.py
aws glue create-job ^
  --name %PROJECT%-api-trusted ^
  --role %LAB_ROLE_ARN% ^
  --glue-version 4.0 ^
  --command Name=glueetl,ScriptLocation=s3://%BUCKET%/scripts/glue_raw_a_trusted_api.py,PythonVersion=3 ^
  --worker-type G.1X ^
  --number-of-workers 2
aws glue start-job-run ^
  --job-name %PROJECT%-api-trusted ^
  --arguments "{\"--BUCKET\":\"%BUCKET%\"}"
```

## Job de RDS

Archivo:

```text
glue_raw_a_trusted_rds.py
```

Lee `dim_departamento.tsv`, `dim_municipio.tsv` y `dim_catalogo.tsv` desde
`raw/rds/` como texto y genera un Parquet independiente para cada tabla:

```text
s3://<BUCKET>/trusted/rds/dim_departamento/
s3://<BUCKET>/trusted/rds/dim_municipio/
s3://<BUCKET>/trusted/rds/dim_catalogo/
```

```cmd
aws s3 cp 04-glue-spark\glue_raw_a_trusted_rds.py s3://%BUCKET%/scripts/glue_raw_a_trusted_rds.py
aws glue create-job ^
  --name %PROJECT%-rds-trusted ^
  --role %LAB_ROLE_ARN% ^
  --glue-version 4.0 ^
  --command Name=glueetl,ScriptLocation=s3://%BUCKET%/scripts/glue_raw_a_trusted_rds.py,PythonVersion=3 ^
  --worker-type G.1X ^
  --number-of-workers 2
aws glue start-job-run ^
  --job-name %PROJECT%-rds-trusted ^
  --arguments "{\"--BUCKET\":\"%BUCKET%\"}"
```

## Problemas frecuentes

### Ejecuciones concurrentes

Si aparece `ConcurrentRunsExceededException`, ya existe un run en ejecución:

```cmd
aws glue get-job-runs ^
  --job-name %PROJECT%-nacimientos-trusted ^
  --query "JobRuns[].[Id,JobRunState]" ^
  --output table
```

Esperar a que termine antes de iniciar otro run. Si es necesario detenerlo:

```cmd
aws glue batch-stop-job-run ^
  --job-name %PROJECT%-nacimientos-trusted ^
  --job-run-ids ID_DEL_RUN
```

### Diferencias entre mayúsculas y minúsculas

Los nombres de columnas usados por el script deben coincidir con los
encabezados de los CSV, especialmente `ANO` y `MES`. Después de la
transformación, las columnas de partición de salida se llaman `anio` y `mes`.

## Pendientes

Queda pendiente catalogar las nuevas salidas en AWS Glue Data Catalog.
