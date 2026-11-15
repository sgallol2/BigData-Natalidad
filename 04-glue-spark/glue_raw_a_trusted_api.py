"""
Glue Job (PySpark): zona raw -> zona trusted para World Bank Data360.

Entrada:
  s3://<BUCKET>/raw/url/WB_WDI_SP_DYN_CBRT_IN/REF_AREA=*/page-*.json
  s3://<BUCKET>/raw/url/WB_WDI_SP_POP_TOTL/REF_AREA=*/page-*.json

Salida:
  s3://<BUCKET>/trusted/world_bank/ (Parquet, particionado por indicador y país)

Argumentos del job: --BUCKET <nombre-del-bucket>
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"])
bucket = args["BUCKET"]

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

INDICADORES = (
    "WB_WDI_SP_DYN_CBRT_IN",
    "WB_WDI_SP_POP_TOTL",
)

paths = [
    f"s3://{bucket}/raw/url/{indicator}/REF_AREA=*/page-*.json"
    for indicator in INDICADORES
]

raw = spark.read.option("multiline", "true").json(paths)
if "value" not in raw.columns:
    raise ValueError("Los JSON de Data360 no contienen el campo value")

df = raw.select(F.explode("value").alias("record")).select("record.*")
required = {"INDICATOR", "REF_AREA", "TIME_PERIOD", "OBS_VALUE"}
missing = required.difference(df.columns)
if missing:
    raise ValueError(f"Faltan campos requeridos en Data360: {sorted(missing)}")

df = (
    df.withColumn("indicador", F.col("INDICATOR"))
    .withColumn("pais", F.col("REF_AREA"))
    .withColumn("anio", F.col("TIME_PERIOD").cast("int"))
    .withColumn("valor", F.col("OBS_VALUE").cast("double"))
)

invalid = df.filter(
    F.col("indicador").isNull()
    | F.col("pais").isNull()
    | F.col("anio").isNull()
    | F.col("valor").isNull()
).count()
if invalid:
    raise ValueError(f"Hay {invalid} registros API con campos obligatorios inválidos")

total = df.count()
print(f"API Data360: {total} registros válidos")
df.groupBy("indicador", "pais").count().orderBy("indicador", "pais").show(
    100, truncate=False
)

(
    df.coalesce(1)
    .write.mode("overwrite")
    .parquet(f"s3://{bucket}/trusted/world_bank/")
)
print("Escritura completada: trusted/world_bank/")

job.commit()
