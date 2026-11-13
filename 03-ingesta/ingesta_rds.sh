#!/usr/bin/env bash
set -Eeuo pipefail

: "${BUCKET:?Defina BUCKET}"
: "${DB_HOST:?Defina DB_HOST}"
: "${DB_USER:?Defina DB_USER}"

DB_NAME="${DB_NAME:-natalidad}"
DB_PORT="${DB_PORT:-3306}"
DB_PASSWORD="${DB_PASSWORD:-${DB_PASS:-}}"
if [[ -z "$DB_PASSWORD" ]]; then
  read -r -s -p "Contraseña de RDS: " DB_PASSWORD
  echo
fi

command -v mysql >/dev/null 2>&1 || {
  echo "No se encontró mysql en la EC2." >&2
  exit 1
}
command -v aws >/dev/null 2>&1 || {
  echo "No se encontró aws en la EC2." >&2
  exit 1
}

export MYSQL_PWD="$DB_PASSWORD"
temporary_directory="$(mktemp -d)"
trap 'unset MYSQL_PWD; rm -rf "$temporary_directory"' EXIT

tables=(dim_departamento dim_municipio dim_catalogo)
for table in "${tables[@]}"; do
  output="${temporary_directory}/${table}.tsv"
  mysql \
    --host="$DB_HOST" \
    --port="$DB_PORT" \
    --user="$DB_USER" \
    --database="$DB_NAME" \
    --batch \
    --raw \
    --execute="SELECT * FROM \`${table}\`;" > "$output"

  aws s3 cp "$output" \
    "s3://${BUCKET}/raw/rds/${table}.tsv"
  echo "Cargada tabla ${table}"
done
