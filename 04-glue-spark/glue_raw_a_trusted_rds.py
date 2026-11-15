"""
Glue Job (PySpark): zona raw -> zona trusted para las dimensiones de RDS.

Entrada:
  s3://<BUCKET>/raw/rds/dim_departamento.tsv
  s3://<BUCKET>/raw/rds/dim_municipio.tsv
  s3://<BUCKET>/raw/rds/dim_catalogo.tsv

Salida:
  s3://<BUCKET>/trusted/rds/<tabla>/ (Parquet)

Los códigos se leen como texto para conservar ceros a la izquierda.
Argumentos del job: --BUCKET <nombre-del-bucket>
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext


args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"])
bucket = args["BUCKET"]

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

TABLAS = (
    "dim_departamento",
    "dim_municipio",
    "dim_catalogo",
)

for tabla in TABLAS:
    input_path = f"s3://{bucket}/raw/rds/{tabla}.tsv"
    output_path = f"s3://{bucket}/trusted/rds/{tabla}/"
    df = (
        spark.read.option("header", "true")
        .option("sep", "\t")
        .option("encoding", "UTF-8")
        .option("inferSchema", "false")
        .csv(input_path)
    )
    df = df.toDF(*[column.replace("\ufeff", "").strip() for column in df.columns])
    total = df.count()
    if total == 0:
        raise ValueError(f"La tabla RDS {tabla} está vacía")
    print(f"RDS {tabla}: {total} registros, columnas={df.columns}")
    df.write.mode("overwrite").parquet(output_path)

print("Escritura completada: trusted/rds/")
job.commit()
