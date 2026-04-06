#!/bin/bash
set -euo pipefail

SOCKS_PORT="${SOCKS_PORT:-9050}"
TIMEOUT=15
ONION_URL="http://duckduckgogg42xjoc72x3sjiusooug5fqpi3ptcbrmakjxuxnjq.onion"

fail() { echo "[FAIL] $*" >&2; exit 1; }
pass() { echo "[OK] $*"; }

# 1. Проверяем, что локальный SOCKS порт поднят и слушает
nc -z -w 5 127.0.0.1 "${SOCKS_PORT}" > /dev/null 2>&1 \
    || fail "SOCKS port ${SOCKS_PORT} not open"
pass "SOCKS port ${SOCKS_PORT} open"

# 2. Проверяем реальный выход в Darknet (DuckDuckGo Onion)
response=$(
    curl \
        --socks5-hostname "127.0.0.1:${SOCKS_PORT}" \
        --silent \
        --max-time "${TIMEOUT}" \
        "${ONION_URL}" \
    2>/dev/null
) || fail "Cannot reach Onion network via SOCKS ${SOCKS_PORT}"

if echo "${response}" | grep -iq "DuckDuckGo"; then
    pass "Connected through Tor network (Onion reachable)"
else
    fail "Onion routing failed! Unexpected response."
fi

exit 0