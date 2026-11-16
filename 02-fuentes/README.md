# 2. Fuentes de datos

Este módulo documenta las fuentes que alimentan el caso de estudio:

1. **RDS MariaDB:** tablas de apoyo geográficas y catálogos del DANE.
2. **Archivos en EC2:** microdatos de nacimientos descargados del DANE.
3. **URL en Internet:** indicadores del Banco Mundial Data360, ingeridos desde
   la EC2.

## Conceptos para la sustentación

### ¿Por qué se usan tres fuentes?

El enunciado exige simular un escenario real de integración de datos. Cada
fuente representa una característica diferente:

- **Base de datos:** datos estructurados, con esquema, claves y relaciones.
- **Archivos:** grandes volúmenes tabulares con cambios de esquema entre años.
- **URL/API:** datos externos, paginados y sujetos a disponibilidad o cambios
  del proveedor.

El data lake unifica estas fuentes sin obligarlas a tener el mismo formato en
la zona `raw`.

### RDS MariaDB y tablas de apoyo

RDS contiene dimensiones relativamente pequeñas que ayudan a interpretar los
microdatos:

- `dim_departamento`: códigos DIVIPOLA de departamentos.
- `dim_municipio`: códigos de municipio, departamento, nombre y tipo.
- `dim_catalogo`: descripción de códigos de variables del DANE.

El archivo [`01_esquema.sql`](01_esquema.sql) crea la base `natalidad`, las
claves primarias y la relación entre municipio y departamento.
[`02_datos_base.sql`](02_datos_base.sql) carga los registros de referencia
usando `REPLACE INTO`, por lo que la carga es idempotente.

Los códigos se modelan como texto (`CHAR`/`VARCHAR`) porque un código como
`05001` no es una cantidad numérica: el cero inicial forma parte de su
identidad.

### Archivos DANE en EC2

Los CSV de nacimientos corresponden a los años 2021 a 2025. La EC2 funciona
como fuente de archivos porque el portal del DANE requiere una descarga manual
con formulario y protección anti-bot; no hay un enlace directo estable para
automatizarla con `curl`.

Los archivos no se versionan en Git por su tamaño y naturaleza de microdatos.
Se transfieren a la EC2, se normalizan y luego se cargan a
`raw/archivos/nacimientos/`.

Los años 2021-2023 tienen 39 columnas y 2024-2025 tienen 40. Por eso el
procesamiento posterior lee cada año por separado y hace `unionByName`, no una
unión por posición.

### API Data360

La API del Banco Mundial usa el endpoint:

```text
https://data360api.worldbank.org/data360/data
```

La consulta utiliza `DATABASE_ID=WB_WDI`, un indicador, `REF_AREA` y
paginación mediante `skip`. Actualmente se ingieren:

- `WB_WDI_SP_DYN_CBRT_IN`: tasa bruta de natalidad.
- `WB_WDI_SP_POP_TOTL`: población total.

La API devuelve un objeto con una colección `value`. Cada registro contiene,
entre otros, `OBS_VALUE`, `INDICATOR`, `REF_AREA` y `TIME_PERIOD`.

## Limitaciones y decisiones

- El volumen de los CSV es moderado: aproximadamente 2,6 millones de registros
  y 424 MB. Un solo equipo podría procesarlo.
- Lo elegimos porque cumple el objetivo del proyecto: practicar un pipeline de
  Big Data con múltiples fuentes y formatos, calidad de datos real y una
  arquitectura que escala sin cambiar el diseño.
- Si tuviéramos 100 veces más datos, solo añadiríamos workers.
- Los datos externos pueden cambiar, tener años faltantes o responder con
  errores temporales; por eso la ingesta conserva la respuesta original y
  registra fallos.

## Archivos de esta etapa

```text
02-fuentes/
├── 01_esquema.sql
├── 02_datos_base.sql
└── *.zip                       # fuentes DANE, no se cargan a Git
```
