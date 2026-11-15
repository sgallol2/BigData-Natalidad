# Punto 5: Catalogación de Tablas SQL con AWS Glue y EMR/Hive

Este módulo contiene la definición y registro de las tablas de datos en el **AWS Glue Data Catalog**, permitiendo que los datos procesados en formato Parquet en la zona `trusted` de Amazon S3 sean consultados mediante SQL estándar en **Amazon Athena**, **Apache Hive (EMR)** y **SparkSQL**.

---

## 1. Conceptos para la Sustentación

- **¿Qué es AWS Glue Data Catalog?**  
  Es un repositorio centralizado de metadatos (un *Hive Metastore* gestionado en la nube). Guarda la definición de las bases de datos, esquemas de tablas, nombres y tipos de columnas, formatos de compresión (Snappy/Parquet) y ubicaciones físicas en S3.
- **¿Por qué es necesario catalogar?**  
  S3 es un almacenamiento de objetos sin conocimiento estructural. Al catalogar, motores analíticos como Athena o Hive pueden interpretar los archivos Parquet como si fueran tablas SQL tradicionales sin mover los datos de lugar (*Serverless Analytics*).
- **Particionamiento y Partition Pruning:**  
  La tabla de nacimientos está particionada por `anio`. Esto permite que una consulta como `WHERE anio = 2024` solo escanee la carpeta `anio=2024/`, reduciendo drásticamente el costo y el tiempo de respuesta.

---

## 2. Métodos de Catalogación Implementados

### Método A: Mediante Sentencias DDL SQL (Amazon Athena / Hive)
Se crearon sentencias DDL explícitas para garantizar tipos de datos exactos y evitar errores de inferencia automática:

1. **`01_crear_database.sql`**: Crea la base de datos `natalidad_db` en Glue/Athena apuntando a `s3://<BUCKET>/trusted/`.
2. **`02_catalogar_nacimientos.sql`**: Crea la tabla `trusted_nacimientos` particionada por `anio INT` y ejecuta `MSCK REPAIR TABLE` para registrar las particiones 2021 a 2025.
3. **`03_catalogar_world_bank.sql`**: Crea la tabla `trusted_world_bank` para las tasas e indicadores internacionales.
4. **`04_catalogar_dimensiones_rds.sql`**: Crea las tablas dimensionales de soporte (`dim_departamento`, `dim_municipio`, `dim_catalogo`) para cruces geográficos y decodificación de variables.

### Método B: Mediante AWS Glue Crawler (Consola AWS)
Un **Glue Crawler** se conecta a Amazon S3, lee los primeros registros de los archivos Parquet, deduce el esquema y escribe la metadata directamente en el Data Catalog:
1. En la consola de **AWS Glue**, ir a **Crawlers** -> **Create crawler**.
2. Nombre: `bigdata-p2-trusted-crawler`.
3. Origen de datos (Data source): `s3://<BUCKET>/trusted/`.
4. Rol IAM: Seleccionar `LabRole`.
5. Base de datos de destino: `natalidad_db`.
6. Programación: *On demand* y dar clic en **Run crawler**.

---

## 3. Verificación de Tablas Catalogadas

Para verificar en el Query Editor de Athena:

```sql
SHOW TABLES IN natalidad_db;
```

Resultado esperado:
- `dim_catalogo`
- `dim_departamento`
- `dim_municipio`
- `trusted_nacimientos`
- `trusted_world_bank`

Verificar particiones de nacimientos:
```sql
SHOW PARTITIONS natalidad_db.trusted_nacimientos;
```
Debe listar:
- `anio=2021`
- `anio=2022`
- `anio=2023`
- `anio=2024`
- `anio=2025`
