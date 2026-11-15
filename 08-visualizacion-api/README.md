# Punto 8: Visualización de Datos y Exposición con API Gateway y Streamlit

Este módulo implementa la capa final de presentación del proyecto:
1. Una aplicación interactiva moderna en **Streamlit** con gráficos en **Plotly**.
2. Un backend Serverless en **AWS Lambda** expuesto mediante **Amazon API Gateway** para servir las métricas analíticas calculadas por el pipeline de Big Data.

---

## 1. Arquitectura de Visualización y Servicio

```
[ Usuario / Evaluador ]
          │
          ▼
┌───────────────────────────┐
│   Streamlit Web App       │  (Dashboard con gráficos Plotly interactivos)
└─────────────┬─────────────┘
              │ (HTTP GET REST / JSON)
              ▼
┌───────────────────────────┐
│    Amazon API Gateway     │  (Endpoint público administrado con CORS)
└─────────────┬─────────────┘
              │
              ▼
┌───────────────────────────┐
│     AWS Lambda Python     │  (Serverless Compute con LabRole)
└─────────────┬─────────────┘
              │
              ▼
┌───────────────────────────┐
│   Amazon S3 (Refined) /   │  (Zona Refined / Amazon Athena)
│     Athena Catalog        │
└───────────────────────────┘
```

---

## 2. Cómo Ejecutar la Aplicación Streamlit

Para iniciar el dashboard interactivo en tu computador:

```bash
python -m streamlit run 08-visualizacion-api/app.py
```

La aplicación se abrirá automáticamente en tu navegador en:  
`http://localhost:8501`

### Funcionalidades del Dashboard:
- **Pestaña 1 (Evolución):** Gráficos de barras del volumen anual y ranking de los departamentos con mayor cantidad de nacimientos.
- **Pestaña 2 (Estacionalidad):** Curva continua de nacimientos por mes destacando el pico nacional en agosto-septiembre.
- **Pestaña 3 (Madres Adolescentes):** Gráfico horizontal de alerta sociodemográfica por departamento.
- **Pestaña 4 (Educación y Salud):** Gráficos combinados de controles prenatales y distribución porcentual entre cesárea y parto espontáneo.
- **Pestaña 5 (Contexto Internacional):** Comparativa de tasas brutas de natalidad (Banco Mundial) y validación cruzada DANE vs World Bank.

---

## 3. Despliegue de AWS Lambda y API Gateway (Opcional en Consola)

### Paso 1: Crear la Función Lambda
1. En la consola de AWS, ir a **AWS Lambda** -> **Create function**.
2. Nombre: `bigdata-p2-api-metricas`.
3. Runtime: **Python 3.11** o superior.
4. Rol de ejecución: Seleccionar rol existente -> `LabRole`.
5. Copiar el código de [`lambda_function.py`](lambda_function.py) en el editor y hacer clic en **Deploy**.

### Paso 2: Crear el API Gateway
1. En la consola de AWS, ir a **API Gateway** -> **Create API**.
2. Seleccionar **HTTP API** -> **Build**.
3. En integración, elegir **Lambda**, seleccionar la función `bigdata-p2-api-metricas`.
4. Nombre del API: `bigdata-p2-gateway`.
5. En rutas, configurar `GET /metricas`.
6. Finalizar y copiar la **Invoke URL** generada (ej. `https://abc123xyz.execute-api.us-east-1.amazonaws.com`).

### Paso 3: Conectar Streamlit al API Gateway
1. En la aplicación Streamlit, en la barra lateral izquierda:
2. Cambiar de *"Datos Procesados del Data Lake"* a *"Consumir desde Amazon API Gateway"*.
3. Pegar la **Invoke URL** y presionar Enter. La aplicación consumirá los datos dinámicamente desde la nube.

---

## 4. Argumentos para la Sustentación Oral

- **¿Por qué desacoplar el frontend (Streamlit) con API Gateway y Lambda?**  
  Porque permite una arquitectura desacoplada (*Serverless*). La aplicación cliente no necesita credenciales de base de datos ni acceso directo a S3; consulta una API REST segura que escala automáticamente y solo genera costo por petición atendida.
- **¿Por qué la visualización no lee directamente los 400 MB de CSVs en tiempo real?**  
  Porque una interfaz gráfica interactiva requiere tiempos de respuesta inferiores a 200 ms. Al precalcular las métricas en PySpark/Athena y almacenarlas en la zona `refined`, la interfaz carga de forma instantánea sin saturar la red ni procesar terabytes en vivo.
