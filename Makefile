.PHONY: help build up down restart logs status \
        test-dns test-health rotate clean logs-aggregator

CYAN  := \033[0;36m
NC    := \033[0m

# Прокси для сборки опционален. 
# Чтобы собрать с VPN: make build PROXY=http://127.0.0.1:10808
PROXY ?= ""

# Удобный алиас для compose, чтобы не писать путь каждый раз
DC := docker compose -f tor-mesh-prototype/docker-compose.yml

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	| awk 'BEGIN {FS = ":.*?## "}; {printf "$(CYAN)%-18s$(NC) %s\n", $$1, $$2}'

build: ## Собрать все образы (передай PROXY=... если нужен VPN)
	docker build \
		--network host \
		--no-cache \
		--build-arg http_proxy=$(PROXY) \
		--build-arg https_proxy=$(PROXY) \
		--build-arg HTTP_PROXY=$(PROXY) \
		--build-arg HTTPS_PROXY=$(PROXY) \
		-f tor-mesh-prototype/tor1/Dockerfile \
		tor-mesh-prototype
	docker build \
		--network host \
		--no-cache \
		--build-arg http_proxy=$(PROXY) \
		--build-arg https_proxy=$(PROXY) \
		--build-arg HTTP_PROXY=$(PROXY) \
		--build-arg HTTPS_PROXY=$(PROXY) \
		-f tor-mesh-prototype/tor2/Dockerfile \
		tor-mesh-prototype
	$(DC) build --no-cache privoxy1 privoxy2 circuit-manager aggregator

up: ## Запустить mesh и aggregator
	$(DC) up -d
	@echo "Ожидаем bootstrap Tor (healthchecks запущены, это займет около минуты)..."
	@$(MAKE) status

down: ## Остановить контейнеры
	$(DC) down --remove-orphans

restart: ## Перезапустить все сервисы
	$(DC) restart

clean: ## Остановить + удалить volumes (и БД агрегатора)
	$(DC) down -v --remove-orphans

logs: ## Логи всех сервисов
	$(DC) logs -f --tail=50

logs-tor1: ## Логи tor1
	$(DC) logs -f --tail=50 tor1

logs-tor2: ## Логи tor2
	$(DC) logs -f --tail=50 tor2

logs-privoxy1: ## Логи Privoxy 1
	$(DC) logs -f --tail=50 privoxy1

logs-privoxy2: ## Логи Privoxy 2
	$(DC) logs -f --tail=50 privoxy2

logs-manager: ## Логи Circuit Manager
	$(DC) logs -f --tail=50 circuit-manager

logs-aggregator: ## Логи Aggregator (Парсер)
	$(DC) logs -f --tail=50 aggregator

status: ## Статус контейнеров + Circuit Manager API
	@echo "── Docker status ──"
	@$(DC) ps
	@echo ""
	@echo "── Circuit Manager ──"
	@curl -s http://127.0.0.1:9999/api/status | python3 -m json.tool 2>/dev/null || \
	    echo "(Circuit Manager ещё не готов)"

test-dns: ## DNS leak test через оба Privoxy
	@echo "── Тестируем Ноду 1 (8118) ──"
	@bash tor-mesh-prototype/scripts/dns_leak_test.sh 127.0.0.1 8118
	@echo "── Тестируем Ноду 2 (8119) ──"
	@bash tor-mesh-prototype/scripts/dns_leak_test.sh 127.0.0.1 8119

test-health: ## Health check Tor контейнеров
	@echo "── tor1 ──"
	@$(DC) exec tor1 /usr/local/bin/healthcheck_tor.sh || true
	@echo ""
	@echo "── tor2 ──"
	@$(DC) exec tor2 /usr/local/bin/healthcheck_tor.sh || true

rotate: ## Немедленная ротация всех цепей
	@curl -s -X POST http://127.0.0.1:9999/api/rotate | python3 -m json.tool

rotate-tor1: ## Ротация только tor1
	@curl -s -X POST http://127.0.0.1:9999/api/rotate \
	    -H "Content-Type: application/json" \
	    -d '{"node_id":"tor1"}' | python3 -m json.tool

rotate-tor2: ## Ротация только tor2
	@curl -s -X POST http://127.0.0.1:9999/api/rotate \
	    -H "Content-Type: application/json" \
	    -d '{"node_id":"tor2"}' | python3 -m json.tool

break: ##[DEMO] Сломать случайную ссылку для проверки Self-Healing
	@$(DC) exec aggregator python break_link.py

dashboard: ## Запустить Live Dashboard агрегатора
	@$(DC) exec aggregator python dashboard.py

show-data: ## Показать последние извлеченные данные (результат парсинга)
	@$(DC) exec aggregator python show_data.py

# Значение по умолчанию, если не указать LIMIT
LIMIT ?= 20

inspect-db: ## Показать ссылки из БД (make inspect-db LIMIT=50 или 0 для всех)
	@$(DC) exec aggregator python inspect_db.py $(LIMIT)
