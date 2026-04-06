#!/bin/bash
set -euo pipefail

NODE_ID="${NODE_ID:-2}"
SOCKS_PORT="${SOCKS_PORT:-9052}"
CONTROL_PORT="${CONTROL_PORT:-9053}"

TORRC_TEMPLATE="/etc/tor/runtime/torrc.template"
TORRC_ACTIVE="/tmp/torrc"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [tor-node-${NODE_ID}] $*"
}

patch_torrc() {
    log "Patching torrc..."

    if [ -z "${TOR_CONTROL_PASSWORD_HASH:-}" ]; then
        if [ -z "${TOR_CONTROL_PASSWORD:-}" ]; then
            log "ERROR: set TOR_CONTROL_PASSWORD or TOR_CONTROL_PASSWORD_HASH in .env"
            exit 1
        fi
        log "Generating hash from TOR_CONTROL_PASSWORD..."
        TOR_CONTROL_PASSWORD_HASH=$(
            tor --hash-password "${TOR_CONTROL_PASSWORD}" 2>/dev/null | tail -1
        )
        log "Hash generated: ${TOR_CONTROL_PASSWORD_HASH}"
    fi

    sed \
        "s|HashedControlPassword PLACEHOLDER|HashedControlPassword ${TOR_CONTROL_PASSWORD_HASH}|g" \
        "${TORRC_TEMPLATE}" > "${TORRC_ACTIVE}"

    if grep -q "PLACEHOLDER" "${TORRC_ACTIVE}"; then
        log "ERROR: PLACEHOLDER not replaced"
        exit 1
    fi

    log "torrc written to ${TORRC_ACTIVE}"
}

validate_config() {
    log "Validating torrc..."
    if ! tor --verify-config -f "${TORRC_ACTIVE}" 2>&1; then
        log "ERROR: torrc invalid — contents:"
        cat "${TORRC_ACTIVE}"
        exit 1
    fi
    log "torrc OK"
}

fix_permissions() {
    chmod 700 /var/lib/tor         2>/dev/null || true
    chmod 700 /var/run/tor         2>/dev/null || true
    chmod 700 /var/run/tor/cookies 2>/dev/null || true
    log "Permissions OK"
}

run_tor() {
    local attempt=0
    local delay=5

    while true; do
        attempt=$((attempt + 1))
        log "Starting Tor (attempt ${attempt})..."

        tor -f "${TORRC_ACTIVE}"
        exit_code=$?
        log "Tor exited (code=${exit_code})"

        [ $exit_code -eq 0 ] && { log "Clean exit"; break; }

        log "Restarting in ${delay}s..."
        sleep "${delay}"
        delay=$(( delay < 60 ? delay * 2 : 60 ))
    done
}

handle_term() {
    log "SIGTERM — stopping Tor"
    pkill -SIGTERM tor 2>/dev/null || true
    sleep 2
    exit 0
}
trap handle_term SIGTERM SIGINT

log "=== Tor Node ${NODE_ID} starting ==="
log "SOCKS=${SOCKS_PORT} CONTROL=${CONTROL_PORT} USER=$(id -un)"

patch_torrc
validate_config
fix_permissions
run_tor
