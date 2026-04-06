#!/bin/bash
set -euo pipefail

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [circuit-mgr] $*"; }

wait_for_control_port() {
    local host="$1"
    local port="$2"
    local node_id="$3"
    local attempts=0

    log "Waiting for ${node_id} Control Port at ${host}:${port}..."
    while ! nc -z -w 3 "${host}" "${port}" > /dev/null 2>&1; do
        attempts=$((attempts + 1))
        if [ $attempts -ge 40 ]; then
            log "WARNING: ${node_id} still unreachable after ${attempts} attempts, starting anyway"
            return 0
        fi
        sleep 3
    done
    log "${node_id} Control Port is up (attempt ${attempts})"
}

TOR1_HOST="${TOR1_HOST:-172.28.0.10}"
TOR1_CONTROL_PORT="${TOR1_CONTROL_PORT:-9051}"
TOR2_HOST="${TOR2_HOST:-172.28.0.11}"
TOR2_CONTROL_PORT="${TOR2_CONTROL_PORT:-9053}"

wait_for_control_port "${TOR1_HOST}" "${TOR1_CONTROL_PORT}" "tor1"
wait_for_control_port "${TOR2_HOST}" "${TOR2_CONTROL_PORT}" "tor2"

log "Starting Circuit Manager..."
exec python3 /app/manager.py
