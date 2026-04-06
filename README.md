# tor-mesh-aggregator

Отказоустойчивая система для автономного мониторинга .onion форумов. 

* **Tor Mesh & Circuit Manager:** Пул из независимых Tor-контейнеров с Privoxy.
* **Самовосстановление:** Фоновый процесс каждые 10 минут простукивает базу собранных ссылок. Если `.onion` или клирнет-зеркало не отвечает 3 раза подряд, ссылка помечается как `DEAD`, паук идет перепаршивать актуальную витрину форума.
* **Single-Writer DB:** База данных не падает в `database is locked` при высоких нагрузках. Все операции записи асинхронно складываются в очередь и прокидываются в SQLite через режим WAL. Дашборд можно смотреть прямо во время работы воркеров.
* **Smart-Router & Deep Crawl:** По умолчанию система работает как "радар" — быстро собирает только новые темы и актуальные ссылки (режим витрины). Если бизнесу понадобится выкачать тексты самих постов, достаточно включить один рубильник `ENABLE_DEEP_CRAWL` в конфиге, и парсер пойдет вглубь.

## Стек
* **Python 3.12** (asyncio, aiosqlite)
* **curl_cffi / parsel** 
* **Tor / Privoxy** 
* **Rich** 
* **Docker / Make** 

## Установка и запуск

## 1. Настройка сети Tor (Мосты)
Настройте мост webtunnel.
1. Получите актуальные мосты через официального Telegram-бота @GetBridgesBot или на сайте bridges.torproject.org.
2. Внесите строки мостов в tor-mesh-prototype/tor1/torrc и tor-mesh-prototype/tor2/torrc в строки 35 и 36
   Формат: Bridge webtunnel 192.0.2.1:443 [FINGERPRINT] url=https://...
3. Сгенерируйте хеш пароля для управления Tor:
   docker compose exec tor1 tor --hash-password "changeme"
4. Внесите пароль и хеш в переменные TOR_CONTROL_PASSWORD и TOR_CONTROL_PASSWORD_HASH в .env

## 2. Сборка и управление (Make)
Управление жизненным циклом осуществляется через утилиту make.

* make build — Сборка всех Docker-образов (без кэша). Для сборки через VPN: make build PROXY=http://127.0.0.1:10808
* make up — Запуск инфраструктуры (Tor, Privoxy, Manager, Aggregator) в фоне.
* make down — Остановка всех контейнеров.
* make clean — Полная очистка: остановка контейнеров и удаление volumes (БД сбрасывается).
* make restart — Перезапуск всех сервисов.

## 3. Ротация IP-адресов
* make rotate — Принудительная ротация цепей (смена IP) для всех Tor-нод.
* make rotate-tor1 — Ротация цепи только для ноды 1.
* make rotate-tor2 — Ротация цепи только для ноды 2.

## 4. Мониторинг и логи
* make dashboard — Запуск Live TUI-дашборда с метриками агрегатора.
* make status — Проверка состояния контейнеров и API Circuit Manager.
* make logs — Объединенный поток логов всех сервисов.
* make logs-aggregator — Логи воркеров сбора данных.
* make logs-manager — Логи Circuit Manager.
* make logs-tor1 / make logs-tor2 — Логи процессов Tor.

## 5. Проверка данных и тестирование
* make inspect-db LIMIT=20 — Вывод таблицы собранных ссылок и тем (Discovery). Вместо 20 можно указать любое число, 0 — вывести всю базу.
* make break — Искусственная поломка случайной ссылки в БД для демонстрации процесса Self-Healing. Результат можно увидеть в логах `make logs-aggregator`,    либо на дашборде `make dashboard`, либо через изменение статуса в выгрузке таблицы `make inspect-db LIMIT=10`  
* make test-health — Выполнение Health Check внутри Tor-контейнеров.
* make test-dns — Проверка на отсутствие утечек DNS (DNS leak test) через Privoxy.

## 6. Настройка авторизации (Cookies)
Для доступа парсера к скрытым разделам требуется передать сессионные куки в .env.
1. Авторизуйтесь на целевом форуме через браузер (желательно Tor).
2. Откройте панель разработчика (F12) -> Application / Storage -> Cookies.
3. Скопируйте значения кук xf_user и xf_session.
4. Вставьте их в .env:
   DUBLIKAT_COOKIE_USER=180089%2C5QhQN...
   DUBLIKAT_COOKIE_SESSION=CkLDUF6SsO8...
(Примечание: Куки устаревают. При потере доступа обновите их в .env и сделайте make restart. При авторизации прожимайте галочку "Запомнить меня" в форме входа, чтобы токен был валиден после закрытия браузера и/или страницы.
Если вы вышли из аккаунта, то значения кук надо получить заново).

## 7. Добавление форумов
Поддерживается возможность работы по нескольким форумам. Для добавления форумов, просто добавьте форум заполнив placeholders в forums.json, данные для них возьмите с форума, который вам нужен и внесите, за исключением переменных окружения, начинающихся с `ENV_`, которые надо прописывать в .env. Другие форумы добавляются аналогично.


--------------------------------------------------

## Альтернативное управление (Чистый Docker)
Если вы используете Windows без WSL или утилита make недоступна, используйте прямые команды Docker. 
(Примечание: указывайте полный путь tor-mesh-prototype/docker-compose.yml, если находитесь на уровень выше).

Сборка и запуск:
* Сборка: docker compose -f tor-mesh-prototype/docker-compose.yml build --no-cache
* Запуск: docker compose -f tor-mesh-prototype/docker-compose.yml up -d
* Остановка: docker compose -f tor-mesh-prototype/docker-compose.yml down
* Полная очистка: docker compose -f tor-mesh-prototype/docker-compose.yml down -v --remove-orphans

Мониторинг:
* Статус контейнеров: docker compose -f tor-mesh-prototype/docker-compose.yml ps
* Логи агрегатора: docker compose -f tor-mesh-prototype/docker-compose.yml logs -f --tail=50 aggregator
* Логи менеджера: docker compose -f tor-mesh-prototype/docker-compose.yml logs -f --tail=50 circuit-manager

Управление и тестирование:
* Запуск Дашборда: docker exec -it forum_aggregator python dashboard.py
* Просмотр БД (Discovery): docker exec -it forum_aggregator python inspect_db.py 20
* Тест Self-Healing: docker exec -it forum_aggregator python break_link.py
* Ротация IP: curl -s -X POST http://127.0.0.1:9999/api/rotate
