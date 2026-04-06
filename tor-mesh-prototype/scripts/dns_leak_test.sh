#!/bin/bash
set -euo pipefail

PROXY_HOST="${1:-127.0.0.1}"
PROXY_PORT="${2:-8118}"
TIMEOUT=30

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

ok()   { echo -e "${GREEN}[OK]${NC}   $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
fail() { echo -e "${RED}[FAIL]${NC} $*"; FAILED=1; }

FAILED=0

echo "══════════════════════════════════════════"
echo " DNS Leak Test | proxy=${PROXY_HOST}:${PROXY_PORT}"
echo "══════════════════════════════════════════"

echo ""
echo "── 1. Текущий реальный IP ──"
REAL_IP=$(curl --silent --max-time 10 "https://api.ipify.org") || REAL_IP="unknown"
echo "   Реальный IP: ${REAL_IP}"

echo ""
echo "── 2. IP через Tor (через Privoxy HTTP proxy) ──"
TOR_IP=$(
    curl \
        --proxy "http://${PROXY_HOST}:${PROXY_PORT}" \
        --silent --fail --max-time "${TIMEOUT}" \
        "https://api.ipify.org" \
    2>/dev/null
) || TOR_IP="FAILED"

if [ "${TOR_IP}" = "FAILED" ]; then
    fail "Cannot get IP through Tor proxy"
elif [ "${TOR_IP}" = "${REAL_IP}" ]; then
    fail "Tor IP == Real IP (${REAL_IP}) — трафик идёт не через Tor!"
else
    ok "Tor IP: ${TOR_IP} (отличается от ${REAL_IP})"
fi

echo ""
echo "── 3. Проверка через check.torproject.org ──"
CHECK=$(
    curl \
        --proxy "http://${PROXY_HOST}:${PROXY_PORT}" \
        --silent --fail --max-time "${TIMEOUT}" \
        "https://check.torproject.org/api/ip" \
    2>/dev/null
) || CHECK="{}"

IS_TOR=$(echo "${CHECK}" | python3 -c "
import sys, json
try:
    print('yes' if json.load(sys.stdin).get('IsTor') else 'no')
except Exception:
    print('unknown')
")

case "${IS_TOR}" in
    yes)     ok "Подтверждено: трафик идёт через Tor" ;;
    no)      fail "Трафик НЕ через Tor!" ;;
    unknown) warn "Не удалось проверить (check.torproject.org недоступен)" ;;
esac

echo ""
echo "── 4. DNS leak ──"

if nslookup "torproject.org" > /dev/null 2>&1; then
    warn "Прямой DNS работает (проверьте изоляцию сети контейнера)"
else
    ok "Прямой DNS заблокирован (ожидаемо для изолированной сети)"
fi

ONION_RESULT=$(
    curl \
        --proxy "http://${PROXY_HOST}:${PROXY_PORT}" \
        --silent \
        --write-out "%{http_code}" \
        --output /dev/null \
        --max-time "${TIMEOUT}" \
        "http://duckduckgogg42xjoc72x3sjasowoarfbgcmvfimaftt6twagswzczad.onion" \
    2>/dev/null
) || ONION_RESULT="0"

if [ "${ONION_RESULT}" = "200" ] || [ "${ONION_RESULT}" = "301" ] || \
   [ "${ONION_RESULT}" = "302" ]; then
    ok ".onion доступен через Tor (HTTP ${ONION_RESULT})"
else
    warn ".onion вернул ${ONION_RESULT} (возможно временно недоступен)"
fi

echo ""
echo "── 5. IPv6 leak ──"
IPV6=$(
    curl --silent --max-time 5 --ipv6 "https://api6.ipify.org" \
    2>/dev/null
) || IPV6=""

if [ -n "${IPV6}" ]; then
    warn "IPv6 активен: ${IPV6} — убедитесь что это Tor exit node"
else
    ok "IPv6 недоступен (утечки нет)"
fi

echo ""
echo "══════════════════════════════════════════"
if [ "${FAILED}" -eq 0 ]; then
    echo -e "${GREEN}Тест пройден — утечек не обнаружено${NC}"
else
    echo -e "${RED}Тест НЕ пройден — обнаружены проблемы${NC}"
fi
echo "══════════════════════════════════════════"
exit "${FAILED}"
