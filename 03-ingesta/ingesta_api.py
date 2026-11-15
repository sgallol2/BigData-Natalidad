#!/usr/bin/env python3
"""Ingesta paginada de indicadores de World Bank Data360 a S3."""

import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3


BASE_URL = "https://data360api.worldbank.org/data360/data"
INDICADORES = ["WB_WDI_SP_DYN_CBRT_IN", "WB_WDI_SP_POP_TOTL"]
DEFAULT_COUNTRIES = ("COL", "BRA", "MEX", "ARG", "CHL", "PER")


def required_env(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Falta la variable de entorno {name}")
    return value


def extract_records(payload):
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        raise RuntimeError("La API devolvió un JSON sin una colección de datos")

    for key in ("data", "Data", "results", "Results", "value", "Value"):
        value = payload.get(key)
        if isinstance(value, list):
            return value

    collections = [value for value in payload.values() if isinstance(value, list)]
    if len(collections) == 1:
        return collections[0]
    raise RuntimeError(
        "No se encontró la colección de registros en la respuesta de Data360"
    )


def request_page(indicator, country, skip):
    query = urllib.parse.urlencode(
        {
            "DATABASE_ID": "WB_WDI",
            "INDICATOR": indicator,
            "REF_AREA": country,
            "skip": skip,
        }
    )
    request = urllib.request.Request(
        f"{BASE_URL}?{query}",
        headers={
            "Accept": "application/json",
            "User-Agent": "bigdata-p2-ingesta/1.0",
        },
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        if response.status != 200:
            raise RuntimeError(
                f"Data360 respondió HTTP {response.status} para {country}, skip={skip}"
            )
        raw_payload = response.read()
    return raw_payload, json.loads(raw_payload)


def upload_page(raw_payload, bucket, indicator, country, page, s3_client):
    key = f"raw/url/{indicator}/REF_AREA={country}/page-{page:06d}.json"
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=raw_payload,
        ContentType="application/json",
    )
    print(f"Cargado s3://{bucket}/{key}")


def detect_year_field(records):
    candidates = {}
    for record in records:
        if not isinstance(record, dict):
            continue
        for field, value in record.items():
            try:
                year = int(str(value)[:4])
            except (TypeError, ValueError):
                continue
            if 1000 <= year <= 9999:
                score = 2 if "TIME" in field.upper() or "YEAR" in field.upper() else 1
                candidates[field] = candidates.get(field, 0) + score
    return max(candidates, key=candidates.get) if candidates else None


def year_from_records(records, year_field):
    if not year_field:
        return []
    years = []
    for record in records:
        try:
            year = int(str(record[year_field])[:4])
        except (KeyError, TypeError, ValueError):
            continue
        if 1000 <= year <= 9999:
            years.append(year)
    return years


def ingest_combination(indicator, country, bucket, s3_client, first_page):
    skip = 0
    page = 0
    total_records = 0
    all_records = []
    raw_payload, payload = first_page

    while True:
        records = extract_records(payload)
        if not records:
            break
        upload_page(raw_payload, bucket, indicator, country, page, s3_client)
        total_records += len(records)
        all_records.extend(records)
        page += 1
        skip += len(records)
        raw_payload, payload = request_page(indicator, country, skip)

    year_field = detect_year_field(all_records)
    years = year_from_records(all_records, year_field)
    last_year = max(years) if years else "N/D"
    print(
        f"{indicator} {country}: {page} página(s), "
        f"{total_records} registro(s), último año={last_year}"
    )


def main():
    bucket = required_env("BUCKET")
    countries = tuple(
        country.strip().upper()
        for country in os.environ.get(
            "WB_COUNTRIES", ",".join(DEFAULT_COUNTRIES)
        ).split(",")
        if country.strip()
    )
    if not countries:
        raise RuntimeError("WB_COUNTRIES no contiene países")

    ingested_at = datetime.now(timezone.utc).isoformat()
    print(f"Iniciando ingesta {ingested_at}; indicadores={INDICADORES}")

    preflight = {}
    failed = []
    for indicator in INDICADORES:
        try:
            first_page = request_page(indicator, countries[0], 0)
            if not extract_records(first_page[1]):
                raise RuntimeError(
                    f"la prueba no devolvió datos para {countries[0]}"
                )
            preflight[indicator] = first_page
            print(f"Prueba OK: {indicator} con {countries[0]}")
        except Exception as error:
            print(
                f"Prueba FALLIDA: {indicator} con {countries[0]}: {error}",
                file=sys.stderr,
            )
            failed.extend((indicator, country) for country in countries)

    s3_client = boto3.client("s3")
    for indicator in INDICADORES:
        if indicator not in preflight:
            continue
        for country in countries:
            try:
                first_page = (
                    preflight[indicator]
                    if country == countries[0]
                    else request_page(indicator, country, 0)
                )
                if not extract_records(first_page[1]):
                    raise RuntimeError("la respuesta no contiene datos")
                ingest_combination(
                    indicator, country, bucket, s3_client, first_page
                )
            except Exception as error:
                print(
                    f"FALLÓ {indicator} {country}: {error}",
                    file=sys.stderr,
                )
                failed.append((indicator, country))

    if failed:
        print("Combinaciones fallidas:", file=sys.stderr)
        for indicator, country in failed:
            print(f"- {indicator} {country}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
