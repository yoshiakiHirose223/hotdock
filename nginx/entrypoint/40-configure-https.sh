#!/bin/sh
set -eu

SERVER_NAME="${DOMAIN:-_}"
APP_UPSTREAM="${NGINX_APP_UPSTREAM:-http://web:8000}"
CERT_ROOT="/etc/letsencrypt/live"
CERT_DIR="${CERT_ROOT}/${DOMAIN:-_}"
TEMPLATE_DIR="/etc/nginx/conf-templates"
OUTPUT_PATH="/etc/nginx/conf.d/default.conf"

render_config() {
    template_path="$1"
    cert_fullchain="${2:-}"
    cert_privkey="${3:-}"

    sed \
        -e "s|__SERVER_NAME__|${SERVER_NAME}|g" \
        -e "s|__APP_UPSTREAM__|${APP_UPSTREAM}|g" \
        -e "s|__CERT_FULLCHAIN__|${cert_fullchain}|g" \
        -e "s|__CERT_PRIVKEY__|${cert_privkey}|g" \
        "${template_path}" > "${OUTPUT_PATH}"
}

if [ -n "${DOMAIN:-}" ] \
    && [ -f "${CERT_DIR}/fullchain.pem" ] \
    && [ -f "${CERT_DIR}/privkey.pem" ]; then
    echo "Using HTTPS nginx config for ${SERVER_NAME}"
    render_config \
        "${TEMPLATE_DIR}/https-ready.conf.template" \
        "${CERT_DIR}/fullchain.pem" \
        "${CERT_DIR}/privkey.pem"
else
    echo "Using HTTP-only nginx config. Certificate files not found for DOMAIN=${DOMAIN:-unset}."
    render_config "${TEMPLATE_DIR}/http-only.conf.template"
fi
