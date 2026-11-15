"""
glue_raw_a_trusted_nacimientos.py
---------------------------------
Glue Job (PySpark): zona raw -> zona trusted para los microdatos de
nacimientos del DANE (2021-2025).

Entrada : s3://<BUCKET>/raw/archivos/nacimientos/anio=AAAA/nacimientos_AAAA.csv
Salida  : s3://<BUCKET>/trusted/nacimientos/  (Parquet, particionado por anio y mes)

Que hace:
  1. Lee cada anio por separado (los anios 2021-2023 tienen 39 columnas y
     2024-2025 tienen 40; CODPAISNACMAD esta en el medio) y los une POR NOMBRE.
  2. Verifica que no se perdieron filas (conteo por anio) y que ANO coincide
     con la carpeta.
  3. Limpia: espacios, vacios -> NULL, codigos de "sin informacion" -> NULL
     (regla POR VARIABLE, tabla NULOS).
  4. Convierte a entero solo las columnas numericas, DESPUES de limpiar.
  5. Crea cod_mpio_ocu / cod_mpio_res (clave de cruce con dim_municipio).
  6. Escribe Parquet y registra un perfil de calidad en el log.

Los codigos (COD_DPTO, NIV_EDUM, etc.) se mantienen como TEXTO para conservar
los ceros a la izquierda. No se usa inferSchema.

Argumentos del job:  --BUCKET <nombre-del-bucket>
"""
import sys
from functools import reduce

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"])
BUCKET = args["BUCKET"]

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# ---------------------------------------------------------------- Configuracion
ENCODING = "UTF-8"  # cambiar a "ISO-8859-1" si "file" indica ese encoding

# Filas esperadas por anio = lineas del archivo menos el encabezado (wc -l - 1)
ESPERADOS = {
    2021: 616914,
    2022: 573625,
    2023: 515549,
    2024: 453901,
    2025: 441537,
}

# Codigos de "sin informacion" POR VARIABLE.
# NO incluir COD_DPTO ni CODPTORE: alli el "99" es Vichada (dato valido).
NULOS = {
    # Confirmados (diccionario / catalogo)
    "EDAD_MADRE": ["99"],
    "NIV_EDUM": ["99"],
    "AREANAC": ["9"],
    "SIT_PARTO": ["9"],
    "TIPO_PARTO": ["9"],
    "SEG_SOCIAL": ["9"],
    "EDAD_PADRE": ["999"],
    # Fuertes por el perfil de columnas (confirmar en el diccionario)
    "NUMCONSUL": ["99"],
    "APGAR1": ["99"],
    "APGAR2": ["99"],
    "NIV_EDUP": ["99"],
    "N_HIJOSV": ["99"],
    "N_EMB": ["99"],
    "ULTCURMAD": ["99"],
    "ULTCURPAD": ["99"],
    # Categoricas de un digito: el 9 seria "sin informacion" (POR CONFIRMAR)
    "PESO_NAC": ["9"],
    "TALLA_NAC": ["9"],
    "ATEN_PAR": ["9"],
    "T_GES": ["9"],
    "T_GES_AGRU_CIE": ["9"],
    "MUL_PARTO": ["9"],
    "IDHEMOCLAS": ["9"],
    "IDFACTORRH": ["9"],
    "IDPERTET": ["9"],
    "EST_CIVM": ["9"],
    "AREA_RES": ["9"],
    "IDCLASADMI": ["9"],
}

# Columnas que son numeros reales (se convierten a entero tras limpiar)
NUMERICAS = ["EDAD_PADRE", "NUMCONSUL", "APGAR1", "APGAR2", "N_HIJOSV", "N_EMB"]


# ---------------------------------------------------------------- Lectura por anio
def leer_anio(anio):
    ruta = f"s3://{BUCKET}/raw/archivos/nacimientos/anio={anio}/"
    df = (
        spark.read.option("header", True)
        .option("encoding", ENCODING)
        .option("quote", '"')
        .option("escape", '"')
        .csv(ruta)  # sin inferSchema: todo entra como texto
    )
    # Limpia posible BOM / espacios en los nombres de columna
    df = df.toDF(*[c.replace("\ufeff", "").strip() for c in df.columns])

    fila = df.agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(F.col("ANO") != str(anio), 1).otherwise(0)).alias("ano_distinto"),
    ).collect()[0]
    total, ano_distinto = fila["total"], fila["ano_distinto"] or 0

    print(f"[{anio}] columnas={len(df.columns)} filas={total} (esperadas={ESPERADOS[anio]}) "
          f"filas_con_ANO_distinto_a_la_carpeta={ano_distinto}")
    if total != ESPERADOS[anio]:
        raise ValueError(
            f"[{anio}] el conteo no coincide: leidas={total}, esperadas={ESPERADOS[anio]}. "
            "Revisar saltos de linea dentro de campos, encoding o archivo incompleto."
        )
    if ano_distinto:
        print(f"[{anio}] ADVERTENCIA: {ano_distinto} filas con ANO distinto al de la carpeta")
    return df


frames = [leer_anio(a) for a in ESPERADOS]

# Union POR NOMBRE: en 2021-2023 falta CODPAISNACMAD y queda en NULL
df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)
print(f"Union: columnas={len(df.columns)}")

# ---------------------------------------------------------------- Limpieza
def texto_limpio(c):
    """trim y cadena vacia -> NULL"""
    v = F.trim(F.col(c))
    return F.when(v == "", None).otherwise(v)


df = df.select([texto_limpio(c).alias(c) for c in df.columns])

# Codigos de "sin informacion" -> NULL (por variable)
df = df.select([
    (F.when(F.col(c).isin(NULOS[c]), None).otherwise(F.col(c)).alias(c)) if c in NULOS else F.col(c)
    for c in df.columns
])

# Control: valores numericos que no se pueden convertir (deberia ser 0)
malos = df.agg(*[
    F.sum(F.when(F.col(c).isNotNull() & F.col(c).cast("int").isNull(), 1).otherwise(0)).alias(c)
    for c in NUMERICAS
]).collect()[0].asDict()
print(f"Valores numericos no convertibles (esperado 0): {malos}")

# Conversion a entero solo en columnas numericas y ya limpias
df = df.select([
    F.col(c).cast("int").alias(c) if c in NUMERICAS else F.col(c)
    for c in df.columns
])

# ---------------------------------------------------------------- Columnas derivadas
df = (
    df.withColumn("cod_mpio_ocu", F.concat(F.col("COD_DPTO"), F.col("COD_MUNIC")))
    .withColumn(
        "cod_mpio_res",
        F.when(F.col("CODPTORE").isNotNull() & F.col("CODMUNRE").isNotNull(),
               F.concat(F.col("CODPTORE"), F.col("CODMUNRE"))),
    )
    .withColumn("anio", F.col("ANO").cast("int"))
    # OJO: Spark no distingue mayusculas en nombres de columna, asi que "mes" y
    # "MES" son la misma columna. Se crea con un nombre temporal, se eliminan
    # las originales y despues se renombra.
    .withColumn("mes_tmp", F.col("MES").cast("int"))
    .drop("ANO", "MES")
    .withColumnRenamed("mes_tmp", "mes")
)

# ---------------------------------------------------------------- Perfil de calidad
total = df.count()
print(f"Total de filas en trusted: {total} (esperado {sum(ESPERADOS.values())})")

pct_nulos = df.agg(*[
    F.round(100 * F.avg(F.col(c).isNull().cast("int")), 2).alias(c) for c in df.columns
]).collect()[0].asDict()
print("Porcentaje de nulos por columna:")
for c, p in pct_nulos.items():
    print(f"  {c:16} {p}")

rango = df.agg(F.min("EDAD_PADRE").alias("min"), F.max("EDAD_PADRE").alias("max")).collect()[0]
print(f"EDAD_PADRE min={rango['min']} max={rango['max']} (el 999 ya no debe aparecer)")

# ---------------------------------------------------------------- Escritura
(
    df.repartition("anio")
    .write.mode("overwrite")
    .partitionBy("anio")
    .parquet(f"s3://{BUCKET}/trusted/nacimientos/")
)
print("Escritura completada: trusted/nacimientos/")

job.commit()
