#!/usr/bin/env python3
"""Ingesta paginada del indicador de natalidad de World Bank Data360 a S3."""

import json
import os
import subprocess
import tempfile
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


BASE_URL = "https://data360api.worldbank.org/data360/data"
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
        return json.load(response)


def upload_page(payload, bucket, indicator, country, page, directory):
    file_path = Path(directory) / f"page-{page:06d}.json"
    file_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    key = f"raw/url/{indicator}/REF_AREA={country}/page-{page:06d}.json"
    subprocess.run(
        ["aws", "s3", "cp", str(file_path), f"s3://{bucket}/{key}"],
        check=True,
    )
    print(f"Cargado s3://{bucket}/{key}")


def main():
    bucket = required_env("BUCKET")
    indicator = os.environ.get("WB_INDICATOR", "WB_WDI_SP_DYN_CBRT_IN")
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
    print(f"Iniciando ingesta {ingested_at}; indicador={indicator}")

    with tempfile.TemporaryDirectory(prefix="wb-data360-") as directory:
        for country in countries:
            skip = 0
            page = 0
            while True:
                payload = request_page(indicator, country, skip)
                records = extract_records(payload)
                if not records:
                    break

                upload_page(
                    payload, bucket, indicator, country, page, directory
                )
                page += 1
                skip += len(records)

            print(f"{country}: {page} página(s) cargada(s)")


if __name__ == "__main__":
    main()
