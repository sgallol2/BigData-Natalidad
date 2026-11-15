# Punto 7: Consultas y Procesamiento Analítico Descriptivo con Apache Spark en Python (PySpark)

Este módulo contiene el desarrollo del procesamiento analítico distribuido utilizando la API de **DataFrames de Apache Spark (PySpark)** sobre los datos de la zona `trusted` almacenados en Amazon S3.

---

## 1. Contenido del Módulo

- **[`analisis_descriptivo_pyspark.py`](analisis_descriptivo_pyspark.py)**: Script autocontenido de PySpark. Carga los datasets Parquet de S3, ejecuta el análisis descriptivo y escribe los resultados agregados en la zona `s3://<BUCKET>/refined/`.
- **[`analisis_descriptivo.ipynb`](analisis_descriptivo.ipynb)**: Jupyter Notebook interactivo con celdas de Markdown y código paso a paso para demostración en **EMR Notebooks** o **JupyterLab**.

---

## 2. Operaciones Distribuidas y Optimización en Spark

Para sustentar ante el profesor cómo se diseñó el código:

1. **Lectura directa de Parquet particionado:**
   ```python
   df_nacimientos = spark.read.parquet(f"s3://{BUCKET}/trusted/nacimientos/")
   ```
   Spark automáticamente reconoce la columna de partición `anio` a partir de la estructura de carpetas en S3.

2. **Optimización con Caching / Broadcast Join:**
   ```python
   df_deptos.cache()
   df_catalogo.cache()
   ```
   Las dimensiones de departamentos y catálogos son tablas pequeñas (< 100 KB). Al hacerles `.cache()`, Spark puede enviarlas a cada *Executor* mediante un **Broadcast Hash Join**, evitando el costoso *Shuffle* de los 2.6 millones de registros de nacimientos por la red.

3. **Cálculo de agregaciones complejas sin SQL:**
   Se utilizaron funciones nativas de Spark (`pyspark.sql.functions`):
   - `F.count()`, `F.avg()`, `F.round()`, `F.sum()`.
   - `F.when().otherwise()` para cálculos condicionales (como la tasa de adolescentes o cesáreas).
   - `.coalesce(1)` al escribir en la zona `refined` para consolidar cada resultado analítico en un único archivo Parquet limpio para la visualización.

---

## 3. Formas de Ejecución

### Opción A: Mediante spark-submit en el clúster EMR
```bash
spark-submit --deploy-mode client analisis_descriptivo_pyspark.py
```

### Opción B: AWS Glue Job
Se puede crear un Job en AWS Glue con el script `analisis_descriptivo_pyspark.py`, asignando `LabRole` y pasando el parámetro `--BUCKET`.

### Opción C: EMR Studio / Jupyter Notebook
Abrir `analisis_descriptivo.ipynb` en Jupyter conectado al clúster de Spark y ejecutar celda por celda para demostración interactiva.

---

## 4. Respuestas a Preguntas del Profesor en la Sustentación

- **¿Por qué PySpark en vez de Pandas para este análisis?**  
  Porque el volumen total supera los 2.6 millones de registros. Pandas carga todo en la memoria RAM de un solo nodo (generando posibles *Out Of Memory*), mientras que Spark distribuye las particiones de datos entre los nodos trabajadores (*Workers*) del clúster, aprovechando paralelismo y ejecución perezosa (*Lazy Evaluation*).
- **¿Qué es la zona Refined (Gold)?**  
  Es la capa final del Data Lake. Mientras que `trusted` almacena datos limpios a nivel de transacción o registro individual, `refined` almacena datos agregados y listos para alimentar tableros de control (BI), modelos de analítica o APIs.
