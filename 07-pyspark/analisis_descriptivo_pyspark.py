"""
analisis_descriptivo_pyspark.py
--------------------------------
Punto 7: Consultas y procesamiento analítico descriptivo con Apache Spark en Python (PySpark).
Lee los datos de la zona trusted (Parquet), ejecuta estadísticas descriptivas,
agrupaciones y transformaciones para responder a las preguntas de negocio, y
guarda los resultados consolidados en la zona refined del data lake.

Compatible con: AWS Glue Job, AWS EMR y ejecución local.
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    # 1. Inicializar la sesión de Spark
    spark = SparkSession.builder \
        .appName("Analisis-Descriptivo-Natalidad-PySpark") \
        .getOrCreate()

    # Obtener el bucket desde variable de entorno o usar el por defecto
    bucket = os.environ.get("BUCKET", "bigdata-p2-190565384867")
    base_s3 = f"s3://{bucket}"

    print(f"=== Iniciando procesamiento con PySpark sobre {base_s3} ===")

    # 2. Cargar DataFrames de la zona trusted
    ruta_nacimientos = f"{base_s3}/trusted/nacimientos/"
    ruta_wb = f"{base_s3}/trusted/world_bank/"
    ruta_deptos = f"{base_s3}/trusted/rds/dim_departamento/"
    ruta_catalogo = f"{base_s3}/trusted/rds/dim_catalogo/"

    print("Cargando datos Parquet...")
    df_nacimientos = spark.read.parquet(ruta_nacimientos)
    df_wb = spark.read.parquet(ruta_wb)
    df_deptos = spark.read.parquet(ruta_deptos)
    df_catalogo = spark.read.parquet(ruta_catalogo)

    # Normalizar todos los nombres de columnas a minúsculas para consistencia total
    df_nacimientos = df_nacimientos.toDF(*[c.lower() for c in df_nacimientos.columns])
    df_deptos = df_deptos.toDF(*[c.lower() for c in df_deptos.columns])
    df_catalogo = df_catalogo.toDF(*[c.lower() for c in df_catalogo.columns])
    df_wb = df_wb.toDF(*[c.lower() for c in df_wb.columns])

    # Optimización: Broadcast caching para tablas pequeñas de dimensiones
    df_deptos.cache()
    df_catalogo.cache()

    total_registros = df_nacimientos.count()
    print(f"Total registros en trusted_nacimientos: {total_registros:,}")

    # ============================================================
    # ESTADÍSTICAS DESCRIPTIVAS GENERALES
    # ============================================================
    print("\n--- Estadísticas Descriptivas de Variables Numéricas ---")
    vars_num = [c for c in ["numconsul", "edad_padre", "apgar1", "apgar2", "n_hijosv", "n_emb"] if c in df_nacimientos.columns]
    if vars_num:
        df_nacimientos.select(vars_num).describe().show()

    # ============================================================
    # PREGUNTA 1: Evolución por Año y Departamento
    # ============================================================
    print("\n--- P1: Evolución de Nacimientos por Año y Departamento ---")
    p1_df = (
        df_nacimientos
        .join(df_deptos, on="cod_dpto", how="left")
        .groupBy(F.col("anio"), F.coalesce(F.col("nombre"), F.col("cod_dpto")).alias("departamento"))
        .agg(F.count(F.lit(1)).alias("total_nacimientos"))
        .orderBy("anio", F.desc("total_nacimientos"))
    )
    p1_df.show(15, truncate=False)

    # ============================================================
    # PREGUNTA 2: Estacionalidad de Nacimientos por Mes
    # ============================================================
    print("\n--- P2: Estacionalidad de Nacimientos por Mes ---")
    meses_dict = {
        1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
        5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
        9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
    }
    mapping_mes = F.create_map([F.lit(x) for i in meses_dict.items() for x in i])

    p2_df = (
        df_nacimientos
        .filter(F.col("mes").isNotNull())
        .groupBy("mes")
        .agg(
            F.count(F.lit(1)).alias("total_nacimientos"),
            F.countDistinct("anio").alias("anios_evaluados")
        )
        .withColumn("nombre_mes", mapping_mes[F.col("mes")])
        .withColumn("promedio_anual", F.round(F.col("total_nacimientos") / F.col("anios_evaluados"), 0))
        .orderBy(F.desc("total_nacimientos"))
    )
    p2_df.show(12, truncate=False)

    # ============================================================
    # PREGUNTA 3: Proporción de Madres Adolescentes por Departamento
    # ============================================================
    print("\n--- P3: Tasa de Madres Adolescentes (10-19 años) por Departamento ---")
    p3_df = (
        df_nacimientos
        .filter(F.col("edad_madre").isNotNull())
        .join(df_deptos, on="cod_dpto", how="left")
        .groupBy(F.coalesce(F.col("nombre"), F.col("cod_dpto")).alias("departamento"))
        .agg(
            F.count(F.lit(1)).alias("total_nacimientos"),
            F.sum(F.when(F.col("edad_madre").isin(["1", "2"]), 1).otherwise(0)).alias("nacimientos_adolescentes")
        )
        .withColumn(
            "pct_madres_adolescentes",
            F.round(100.0 * F.col("nacimientos_adolescentes") / F.col("total_nacimientos"), 2)
        )
        .orderBy(F.desc("pct_madres_adolescentes"))
    )
    p3_df.show(15, truncate=False)

    # ============================================================
    # PREGUNTA 4: Nivel Educativo vs Controles y Tipo de Parto
    # ============================================================
    print("\n--- P4: Nivel Educativo de la Madre vs Controles y Cesáreas ---")
    cat_edu = df_catalogo.filter(F.col("variable") == "NIV_EDUM")

    p4_df = (
        df_nacimientos
        .filter(F.col("niv_edum").isNotNull())
        .withColumn("cod_edu_pad", F.lpad(F.col("niv_edum"), 2, "0"))
        .join(cat_edu, F.col("cod_edu_pad") == F.col("codigo"), "left")
        .groupBy(F.coalesce(F.col("descripcion"), F.lit("Otro / No especificado")).alias("nivel_educativo"))
        .agg(
            F.count(F.lit(1)).alias("total_nacimientos"),
            F.round(F.avg("numconsul"), 2).alias("promedio_controles"),
            F.round(100.0 * F.sum(F.when(F.col("tipo_parto") == "2", 1).otherwise(0)) / F.count(F.lit(1)), 2).alias("pct_cesareas"),
            F.round(100.0 * F.sum(F.when(F.col("tipo_parto") == "1", 1).otherwise(0)) / F.count(F.lit(1)), 2).alias("pct_espontaneos")
        )
        .orderBy(F.desc("promedio_controles"))
    )
    p4_df.show(15, truncate=False)

    # ============================================================
    # PREGUNTA 5: Comparativa Internacional (Banco Mundial)
    # ============================================================
    print("\n--- P5: Comparativa Internacional de Tasa Bruta de Natalidad ---")
    p5_df = (
        df_wb
        .filter(F.col("indicador") == "WB_WDI_SP_DYN_CBRT_IN")
        .select(
            F.col("pais"),
            F.col("anio"),
            F.round(F.col("valor"), 2).alias("tasa_bruta_natalidad")
        )
        .orderBy(F.desc("anio"), F.desc("tasa_bruta_natalidad"))
    )
    p5_df.show(15, truncate=False)

    # ============================================================
    # GUARDAR RESULTADOS EN ZONA REFINED (S3)
    # ============================================================
    print("\n=== Escribiendo DataFrames analíticos a la zona REFINED (S3) ===")
    p1_df.coalesce(1).write.mode("overwrite").parquet(f"{base_s3}/refined/p1_evolucion_departamentos/")
    p2_df.coalesce(1).write.mode("overwrite").parquet(f"{base_s3}/refined/p2_estacionalidad_meses/")
    p3_df.coalesce(1).write.mode("overwrite").parquet(f"{base_s3}/refined/p3_madres_adolescentes/")
    p4_df.coalesce(1).write.mode("overwrite").parquet(f"{base_s3}/refined/p4_educacion_parto/")
    p5_df.coalesce(1).write.mode("overwrite").parquet(f"{base_s3}/refined/p5_comparativa_wb/")

    print("=== Proceso finalizado exitosamente. Zona refined actualizada ===")

if __name__ == "__main__":
    main()
