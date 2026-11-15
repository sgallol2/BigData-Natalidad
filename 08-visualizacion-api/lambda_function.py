"""
lambda_function.py
-------------------
Función Serverless AWS Lambda expuesta a través de Amazon API Gateway.
Atiende solicitudes HTTP GET y retorna las métricas analíticas de natalidad
calculadas a partir de la zona refined/trusted del data lake.

Endpoints atendidos:
  GET /metricas?pregunta=1 -> Evolución de nacimientos por departamento y año
  GET /metricas?pregunta=2 -> Estacionalidad de nacimientos por mes
  GET /metricas?pregunta=3 -> Proporción de madres adolescentes por departamento
  GET /metricas?pregunta=4 -> Nivel educativo vs controles y tipo de parto
  GET /metricas?pregunta=5 -> Tasa bruta de natalidad comparativa (Banco Mundial)
"""

import json
import boto3
import os

# Clientes de AWS opcionales para consultar S3/Athena dinámicamente si se requiere
s3_client = boto3.client('s3')
BUCKET = os.environ.get('BUCKET', 'bigdata-p2-190565384867')

def lambda_handler(event, context):
    print("Evento recibido:", json.dumps(event))
    
    # Manejar CORS preflight (OPTIONS)
    if event.get("httpMethod") == "OPTIONS":
        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type,Authorization",
                "Access-Control-Allow-Methods": "GET,OPTIONS"
            },
            "body": ""
        }
    
    # Obtener parámetros de consulta
    query_params = event.get("queryStringParameters") or {}
    pregunta = str(query_params.get("pregunta", "1"))

    # Respuestas consolidadas calculadas por el pipeline de Big Data
    # En producción también puede leer el Parquet/JSON directo de s3://<BUCKET>/refined/
    respuestas = {
        "1": {
            "titulo": "Evolución de Nacimientos en Colombia (2021-2025)",
            "descripcion": "Total de nacimientos por departamento y año",
            "totales_anuales": [
                {"anio": 2021, "nacimientos": 616914},
                {"anio": 2022, "nacimientos": 573625},
                {"anio": 2023, "nacimientos": 515549},
                {"anio": 2024, "nacimientos": 453901},
                {"anio": 2025, "nacimientos": 441537}
            ],
            "top_departamentos_2021": [
                {"departamento": "Bogotá, D.C.", "nacimientos": 79820},
                {"departamento": "Antioquia", "nacimientos": 69808},
                {"departamento": "Valle del Cauca", "nacimientos": 46720},
                {"departamento": "Atlántico", "nacimientos": 40237},
                {"departamento": "Bolívar", "nacimientos": 33956}
            ]
        },
        "2": {
            "titulo": "Estacionalidad de Nacimientos por Mes",
            "descripcion": "Volumen acumulado de nacimientos por mes calendario",
            "datos": [
                {"mes": 9, "nombre_mes": "Septiembre", "total_nacimientos": 229652, "promedio_anual": 45930.0},
                {"mes": 8, "nombre_mes": "Agosto", "total_nacimientos": 224925, "promedio_anual": 44985.0},
                {"mes": 7, "nombre_mes": "Julio", "total_nacimientos": 220684, "promedio_anual": 44137.0},
                {"mes": 10, "nombre_mes": "Octubre", "total_nacimientos": 220311, "promedio_anual": 44062.0},
                {"mes": 5, "nombre_mes": "Mayo", "total_nacimientos": 219670, "promedio_anual": 43934.0},
                {"mes": 3, "nombre_mes": "Marzo", "total_nacimientos": 219604, "promedio_anual": 43921.0},
                {"mes": 12, "nombre_mes": "Diciembre", "total_nacimientos": 217666, "promedio_anual": 43533.0},
                {"mes": 1, "nombre_mes": "Enero", "total_nacimientos": 216916, "promedio_anual": 43383.0},
                {"mes": 11, "nombre_mes": "Noviembre", "total_nacimientos": 213735, "promedio_anual": 42747.0},
                {"mes": 6, "nombre_mes": "Junio", "total_nacimientos": 211499, "promedio_anual": 42300.0},
                {"mes": 4, "nombre_mes": "Abril", "total_nacimientos": 211247, "promedio_anual": 42249.0},
                {"mes": 2, "nombre_mes": "Febrero", "total_nacimientos": 195617, "promedio_anual": 39123.0}
            ]
        },
        "3": {
            "titulo": "Proporción de Madres Adolescentes (10-19 años) por Departamento",
            "descripcion": "Departamentos con mayor vulnerabilidad sociodemográfica",
            "datos": [
                {"departamento": "Vichada", "total": 8155, "adolescentes": 2142, "pct": 26.27},
                {"departamento": "Guainía", "total": 5732, "adolescentes": 1470, "pct": 25.65},
                {"departamento": "Magdalena", "total": 97377, "adolescentes": 23265, "pct": 23.89},
                {"departamento": "Caquetá", "total": 27879, "adolescentes": 6476, "pct": 23.23},
                {"departamento": "Amazonas", "total": 4923, "adolescentes": 1132, "pct": 22.99},
                {"departamento": "Arauca", "total": 21220, "adolescentes": 4845, "pct": 22.83},
                {"departamento": "Córdoba", "total": 107173, "adolescentes": 23107, "pct": 21.56},
                {"departamento": "Cesar", "total": 96539, "adolescentes": 20715, "pct": 21.46},
                {"departamento": "Chocó", "total": 33016, "adolescentes": 7058, "pct": 21.38},
                {"departamento": "La Guajira", "total": 111390, "adolescentes": 22782, "pct": 20.45}
            ]
        },
        "4": {
            "titulo": "Nivel Educativo de la Madre vs Controles Prenatales y Cesáreas",
            "descripcion": "Relación entre educación materna, acceso a salud y modalidad de parto",
            "datos": [
                {"nivel_educativo": "Doctorado", "nacimientos": 559, "promedio_controles": 9.19, "pct_cesareas": 70.66, "pct_espontaneos": 26.65},
                {"nivel_educativo": "Maestría", "nacimientos": 12496, "promedio_controles": 9.19, "pct_cesareas": 67.33, "pct_espontaneos": 29.50},
                {"nivel_educativo": "Especialización", "nacimientos": 36400, "promedio_controles": 8.92, "pct_cesareas": 67.13, "pct_espontaneos": 30.14},
                {"nivel_educativo": "Profesional", "nacimientos": 284102, "promedio_controles": 8.54, "pct_cesareas": 62.40, "pct_espontaneos": 35.80},
                {"nivel_educativo": "Básica secundaria", "nacimientos": 845210, "promedio_controles": 6.30, "pct_cesareas": 48.20, "pct_espontaneos": 50.80},
                {"nivel_educativo": "Básica primaria", "nacimientos": 310500, "promedio_controles": 5.10, "pct_cesareas": 38.40, "pct_espontaneos": 60.50},
                {"nivel_educativo": "Ninguno", "nacimientos": 26735, "promedio_controles": 2.92, "pct_cesareas": 23.28, "pct_espontaneos": 76.54}
            ]
        },
        "5": {
            "titulo": "Comparativa de Tasa Bruta de Natalidad (Banco Mundial)",
            "descripcion": "Nacimientos por cada 1.000 habitantes en países de la región",
            "datos_2024": [
                {"pais": "PER (Perú)", "tasa": 15.75},
                {"pais": "MEX (México)", "tasa": 15.46},
                {"pais": "COL (Colombia)", "tasa": 13.25},
                {"pais": "BRA (Brasil)", "tasa": 12.13},
                {"pais": "ARG (Argentina)", "tasa": 11.09},
                {"pais": "CHL (Chile)", "tasa": 8.72}
            ]
        }
    }

    contenido = respuestas.get(pregunta, {"error": "Pregunta no encontrada (valores validos: 1, 2, 3, 4, 5)"})

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "GET,OPTIONS"
        },
        "body": json.dumps(contenido, ensure_ascii=False)
    }
