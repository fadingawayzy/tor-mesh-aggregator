#!/bin/bash
set -euo pipefail

TOR_HOST="${TOR_HOST:-172.28.0.10}"
TOR_PORT="${TOR_PORT:-9050}"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [privoxy] $*"
}

log "Configuring forward to ${TOR_HOST}:${TOR_PORT}"

sed \
    -e "s|__TOR1_HOST__|${TOR_HOST}|g" \
    -e "s|__TOR1_PORT__|${TOR_PORT}|g" \
    /etc/privoxy/config > /tmp/privoxy_active.conf

log "Starting Privoxy for ${TOR_HOST}:${TOR_PORT}..."
exec privoxy --no-daemon /tmp/privoxy_active.conf
