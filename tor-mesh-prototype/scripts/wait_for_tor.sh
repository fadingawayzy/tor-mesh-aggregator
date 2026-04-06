#!/bin/bash

SOCKS_HOST="${1:-127.0.0.1}"
SOCKS_PORT="${2:-9050}"
TIMEOUT="${3:-120}"

log() { echo "[$(date '+%H:%M:%S')] [wait_for_tor] $*"; }

elapsed=0
while [ $elapsed -lt "$TIMEOUT" ]; do
    if curl \
        --socks5-hostname "${SOCKS_HOST}:${SOCKS_PORT}" \
        --silent --fail --max-time 10 \
        "https://check.torproject.org/api/ip" \
        | grep -q '"IsTor":true' 2>/dev/null
    then
        log "Tor ready on ${SOCKS_HOST}:${SOCKS_PORT} (${elapsed}s)"
        exit 0
    fi
    log "Waiting for Tor... ${elapsed}/${TIMEOUT}s"
    sleep 5
    elapsed=$((elapsed + 5))
done

log "ERROR: Tor not ready after ${TIMEOUT}s"
exit 1
