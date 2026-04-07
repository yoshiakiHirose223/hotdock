#!/bin/sh
set -eu

DOMAIN="${DOMAIN:-}"
CERT_WATCH_INTERVAL="${NGINX_CERT_WATCH_INTERVAL:-15}"
CERT_DIR="/etc/letsencrypt/live/${DOMAIN}"
CONFIG_SCRIPT="/docker-entrypoint.d/40-configure-https.sh"

if [ -z "${DOMAIN}" ]; then
    echo "Certificate watcher disabled. DOMAIN is not set."
    exit 0
fi

certificate_fingerprint() {
    if [ -f "${CERT_DIR}/fullchain.pem" ] && [ -f "${CERT_DIR}/privkey.pem" ]; then
        cksum "${CERT_DIR}/fullchain.pem" "${CERT_DIR}/privkey.pem" 2>/dev/null | cksum | awk '{print $1}'
    else
        printf 'missing'
    fi
}

last_fingerprint="$(certificate_fingerprint)"

(
    while :; do
        current_fingerprint="$(certificate_fingerprint)"

        if [ "${current_fingerprint}" != "${last_fingerprint}" ]; then
            echo "Certificate change detected for ${DOMAIN}. Regenerating nginx config."
            "${CONFIG_SCRIPT}"

            if nginx -t; then
                nginx -s reload
                last_fingerprint="${current_fingerprint}"
            else
                echo "Skipping nginx reload because config test failed."
            fi
        fi

        sleep "${CERT_WATCH_INTERVAL}" & wait $!
    done
) &
