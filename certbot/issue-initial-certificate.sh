#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_ROOT=$(CDPATH= cd -- "${SCRIPT_DIR}/.." && pwd)

cd "${PROJECT_ROOT}"

if [ ! -f .env ]; then
    echo ".env が見つかりません。.env.example をコピーして作成してください。"
    exit 1
fi

set -a
. ./.env
set +a

: "${DOMAIN:?DOMAIN を .env に設定してください。}"
: "${CERTBOT_EMAIL:?CERTBOT_EMAIL を .env に設定してください。}"

docker-compose up -d web db nginx certbot

docker-compose run --rm --entrypoint certbot certbot certonly \
    --webroot \
    -w /var/www/certbot \
    -d "${DOMAIN}" \
    --email "${CERTBOT_EMAIL}" \
    --agree-tos \
    --no-eff-email

docker-compose exec nginx sh -lc '/docker-entrypoint.d/40-configure-https.sh && nginx -t && nginx -s reload'
