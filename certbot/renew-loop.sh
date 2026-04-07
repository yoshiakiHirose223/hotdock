#!/bin/sh
set -eu

trap 'exit 0' TERM INT

while :; do
    certbot renew --webroot -w /var/www/certbot --quiet || true
    sleep 12h & wait $!
done
