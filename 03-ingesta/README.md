# Ingesta automática

## Conceptos para la sustentación

### ¿Qué significa ingestar?

La ingesta es el movimiento controlado desde las fuentes hacia la zona `raw`
del data lake. En esta etapa no se aplican reglas analíticas ni se mezclan las
fuentes: se conserva una copia reproducible de cada entrada.

La zona `raw` funciona como evidencia de origen. La limpieza, el tipado y la
unificación se realizan después en AWS Glue, hacia `trusted`.

### Ingesta por fuente

- **Archivos:** `ingesta_archivos.sh` sincroniza cada CSV del DANE a
  `raw/archivos/nacimientos/anio=AAAA/`.
- **RDS:** `ingesta_rds.sh` extrae las tres tablas de apoyo como TSV y las
  guarda en `raw/rds/`.
- **URL:** `ingesta_api.py` consulta Data360, pagina con `skip` y guarda cada
  respuesta JSON en `raw/url/<indicador>/REF_AREA=<pais>/`.

### Idempotencia y reejecución

Las rutas de salida son deterministas. Si se ejecuta nuevamente la ingesta,
el mismo objeto se reemplaza en lugar de crear una ruta nueva. Esto permite
actualizar los datos sin duplicar claves lógicas y facilita reprocesar la
zona `raw`.

### Paginación y validación

La API puede entregar una respuesta parcial. El script continúa aumentando
`skip` según la cantidad de registros recibidos hasta encontrar una página sin
datos. Antes de escribir verifica cada indicador con COL; una respuesta con
error o vacía no produce un JSON vacío.

Cada combinación indicador-país informa páginas, registros y último año. Si
una combinación falla, las demás continúan y el proceso termina con código
distinto de cero para que un automatizador detecte el problema.

### Limitaciones

- La descarga inicial de microdatos del DANE es manual por el formulario y la
  protección anti-bot del portal.
- La disponibilidad de años e indicadores de Data360 depende del proveedor.
  Por ejemplo, dos indicadores pueden tener distintos años más recientes.
- La ingesta se ejecuta manualmente o mediante un programador en la EC2; no es
  un servicio administrado de streaming.

Estos scripts se ejecutan en la EC2 y cargan las fuentes que no son los CSV
del DANE:

- `ingesta_archivos.sh`: sincroniza automáticamente los CSV normalizados de
  `datos/dane/nacimientos/` con la partición S3 correspondiente a cada año.
- `ingesta_api.py`: consulta estos dos indicadores de World Bank Data360 por
  país, recorre la paginación y sube las respuestas JSON originales a
  `raw/url/`:
  - `WB_WDI_SP_DYN_CBRT_IN` (tasa bruta de natalidad)
  - `WB_WDI_SP_POP_TOTL` (población total)
  Los países son `COL`, `BRA`, `MEX`, `ARG`, `CHL` y `PER`.
- `ingesta_rds.sh`: extrae las tablas de apoyo de MariaDB y las sube como TSV
  a `raw/rds/`.

## Estado actual de la ingesta

La ingesta fue probada y produjo:

```text
raw/rds/dim_departamento.tsv
raw/rds/dim_municipio.tsv
raw/rds/dim_catalogo.tsv
raw/url/WB_WDI_SP_DYN_CBRT_IN/REF_AREA=ARG/page-000000.json
raw/url/WB_WDI_SP_DYN_CBRT_IN/REF_AREA=BRA/page-000000.json
raw/url/WB_WDI_SP_DYN_CBRT_IN/REF_AREA=CHL/page-000000.json
raw/url/WB_WDI_SP_DYN_CBRT_IN/REF_AREA=COL/page-000000.json
raw/url/WB_WDI_SP_DYN_CBRT_IN/REF_AREA=MEX/page-000000.json
raw/url/WB_WDI_SP_DYN_CBRT_IN/REF_AREA=PER/page-000000.json
raw/url/WB_WDI_SP_POP_TOTL/REF_AREA=ARG/page-000000.json
raw/url/WB_WDI_SP_POP_TOTL/REF_AREA=BRA/page-000000.json
raw/url/WB_WDI_SP_POP_TOTL/REF_AREA=CHL/page-000000.json
raw/url/WB_WDI_SP_POP_TOTL/REF_AREA=COL/page-000000.json
raw/url/WB_WDI_SP_POP_TOTL/REF_AREA=MEX/page-000000.json
raw/url/WB_WDI_SP_POP_TOTL/REF_AREA=PER/page-000000.json
```

Los CSV del DANE se deben descargar manualmente. El portal responde por HTTP,
pero el flujo de descarga de los microdatos requiere formulario y protección
anti-bot; no se encontró un enlace directo estable para automatizarlo con
`curl`. Esta es una limitación conocida del proyecto.

En la EC2, los archivos se organizaron así:

```text
datos/dane/
├── zip/           # archivos originales transferidos desde la máquina local
└── nacimientos/  # CSV extraídos y con nombres normalizados
```

Los `.zip` se transfirieron desde la máquina local con `scp`, se extrajeron en
la EC2 y los CSV resultantes se guardaron en `datos/dane/nacimientos/`. Los
`.zip` se conservan como respaldo local de la fuente, pero no se cargan al
data lake. Los CSV deben tener el formato normalizado
`nacimientos_AÑO.csv`. El script los sincroniza automáticamente a la
partición correspondiente:

```bash
bash ingesta_archivos.sh
```

El directorio se puede cambiar con `DIR`:

```bash
DIR=/ruta/a/nacimientos bash ingesta_archivos.sh
```

Verificar los archivos cargados:

```bash
aws s3 ls s3://$BUCKET/raw/archivos/nacimientos/ --recursive
```

## Variables de entorno

En la EC2 definir, como mínimo:

```bash
export AWS_DEFAULT_REGION=us-east-1
export BUCKET=bigdata-p2-ACCOUNT_ID
export DB_HOST=bigdata-p2-db.xxxxxx.us-east-1.rds.amazonaws.com
export DB_USER=admin
export DB_NAME=natalidad
export DB_PASS='CONTRASEÑA_LOCAL_NO_VERSIONADA'
```

También se puede usar `DB_PASSWORD` en lugar de `DB_PASS`. Si ninguna está
definida, `ingesta_rds.sh` solicita la contraseña de forma interactiva.

## Ejecución

```bash
bash ingesta_rds.sh
bash ingesta_archivos.sh
python3 ingesta_api.py
```

Por defecto, la API consulta `COL,BRA,MEX,ARG,CHL,PER`. Se puede cambiar la
lista sin modificar el código:

```bash
export WB_COUNTRIES=COL,BRA,ECU,PER
python3 ingesta_api.py
```

Los dos indicadores se consultan para `COL,BRA,MEX,ARG,CHL,PER`. Antes de
escribir en S3 se prueba cada indicador con `COL`; si falla o no devuelve
datos, no se escribe ningún archivo para ese indicador. Cada página se guarda
separadamente para conservar la respuesta original y facilitar reprocesos.

La API se ejecuta manualmente cuando se requiera actualizar los datos; no se
configura mediante `cron`.

El archivo `ingesta.env` debe tener permisos restringidos:

```bash
chmod 600 /home/ubuntu/ingesta.env
```

No guardar ese archivo ni contraseñas en Git.
