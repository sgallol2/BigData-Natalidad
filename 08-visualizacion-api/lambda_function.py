"""
lambda_function.py
-------------------
Función Serverless AWS Lambda expuesta a través de Amazon API Gateway.
Atiende solicitudes HTTP GET y consulta dinámicamente las métricas analíticas
calculadas por el pipeline de Big Data en la zona refined de Amazon S3.

CERO DATOS QUEMADOS: Lee directamente el JSON analítico generado por Apache Spark en S3.
"""

import json
import os
import boto3

# Cliente S3
s3_client = boto3.client('s3')
BUCKET = os.environ.get('BUCKET', 'bigdata-p2-190565384867')
KEY_METRICAS = 'refined/metricas_consolidadas.json'

def lambda_handler(event, context):
    print("Evento recibido:", json.dumps(event))
    
    headers_cors = {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type,Authorization",
        "Access-Control-Allow-Methods": "GET,OPTIONS"
    }

    # Manejo de CORS preflight (OPTIONS)
    if event.get("httpMethod") == "OPTIONS":
        return {
            "statusCode": 200,
            "headers": headers_cors,
            "body": ""
        }
    
    # Obtener el parámetro 'pregunta' (valores 1 al 5)
    query_params = event.get("queryStringParameters") or {}
    pregunta = str(query_params.get("pregunta", "1"))

    try:
        # Lectura dinámica directa desde la zona refined de Amazon S3
        respuesta_s3 = s3_client.get_object(Bucket=BUCKET, Key=KEY_METRICAS)
        contenido_json = json.loads(respuesta_s3['Body'].read().decode('utf-8'))

        if pregunta in contenido_json:
            return {
                "statusCode": 200,
                "headers": headers_cors,
                "body": json.dumps(contenido_json[pregunta], ensure_ascii=False)
            }
        else:
            return {
                "statusCode": 404,
                "headers": headers_cors,
                "body": json.dumps({
                    "error": f"Pregunta '{pregunta}' no encontrada. Valores válidos: 1, 2, 3, 4, 5"
                }, ensure_ascii=False)
            }

    except Exception as e:
        print(f"Error al leer desde S3 ({BUCKET}/{KEY_METRICAS}): {str(e)}")
        return {
            "statusCode": 500,
            "headers": headers_cors,
            "body": json.dumps({
                "error": "Error al consultar las métricas analíticas en Amazon S3",
                "detalle": str(e)
            }, ensure_ascii=False)
        }