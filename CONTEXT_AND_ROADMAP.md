# 🧭 КОНТЕКСТ, АРХИТЕКТУРА И ROADMAP ПРОЕКТА
> **Файл экстренного восстановления контекста для ИИ-агента и разработчика.**  
> Если сессия прервалась, контекст сброшен или ты забыл детали — **прочти этот файл целиком**. Здесь зафиксированы все решения, нюансы, архитектура и план действий.

---

## 📌 1. Быстрый срез проекта (TL;DR)

* **Локальный путь**: `D:\GitHub\OBILeadLagCollector`
* **GitHub аккаунт**: **`grishaSuperBoy`**
  * SSH-алиас: `gh-grisha`
  * Email: `333852748+grishaSuperBoy@users.noreply.github.com`
  * Правило безопасности: **НИКОГДА не коммитить секреты и `.env`**; пушить только под указанным аккаунтом.
* **Менеджер пакетов**: **`Poetry`** (виртуальное окружение, `pyproject.toml`, `package-mode = false`).
* **Целевая платформа деплоя**: **Render.com (Free Tier Web Service)**.
* **Целевая база данных**: **MongoDB Atlas Free Tier (M0, 512 MB)**.
* **Суть стратегии**: Сбор высокочастотных рыночных данных по **Order Book Imbalance (OBI) Lead-Lag** арбитражу на **альткоинах со слабыми маркет-мейкерами**.

---

## 🎯 2. В чём главная идея и почему именно так?

### ❌ Почему НЕ мегакапы (BTC, ETH, SOL):
На BTC и ETH стоят институциональные HFT-маркетмейкеры (Wintermute, Jump, GSR, Jane Street) с прямым оптоволокном и колокацией в датацентрах. Они выравнивают стаканы за доли миллисекунды. Спреды там микроскопические ($0.5$ – $2$ bps), их полностью сжирает комиссия биржи.

### ✅ Где лежат деньги (Альткоины со слабыми маркет-мейкерами):
1. **Сонные роботы**: На волатильных альтах средней и низкой ликвидности на ведомых биржах маркет-мейкеры используют примитивные скрипты, запаздывающие на **$1000$ – $3000$ мс (1–3 секунды)**, либо временно отключаются при резких движениях.
2. **Импульс Binance (Lead)**: Binance Futures — крупнейший пул ликвидности. Когда в его стакане формируется резкий перекос объема ($|OBI| \ge 0.50$, то есть более 75/25 в пользу покупателей или продавцов), цена на Binance мгновенно делает скачок.
3. **Разрыв (Dislocation)**: На ведомой бирже стакан стоит на месте. Разрыв цен раздувается до **$15$ – $150$ bps ($0.15\%$ – $1.50\%$)**!
4. **Снайперский Maker-вход**: Мы выставляем виртуальный лимитный ордер (Post-Only) на ведомой площадке в лучший Bid (при росте) или лучший Ask (при падении). Пока цена ведомой биржи догоняет Binance, наш ордер наливают, и мы забираем профит с нулевой или отрицательной комиссией мейкера.

---

## 🏛️ 3. Матрица участников (10 бирж)

* **LEAD (Ведущая)**:
  * **Binance Futures** (`binance`) — эталон цены и генератор OBI-импульсов (`depth5@100ms`).
* **LAG DEX (Ведомые децентрализованные перп-биржи)**:
  * **Aster DEX** (`asterdex`) — Binance-совместимый протокол WS `depth5@100ms`.
  * **Hyperliquid** (`hyperliquid`) — топовый CLOB DEX, L1 WebSocket `l2Book`.
  * **dYdX v4** (`dydx`) — Cosmos AppChain, WebSocket `v4_orderbook` (из-за децентрализованного консенсуса нод отстаёт сильнее всех, идеальная цель!).
* **LAG CEX (Ведомые централизованные биржи из проекта)**:
  * **Bybit** (`bybit`) — linear WS `orderbook.50`.
  * **OKX** (`okx`) — public WS `books5`.
  * **Bitget** (`bitget`) — futures WS `books5`.
  * **MEXC** (`mexc`) — contract WS `sub.depth.full` (один из самых медленных ММ в крипте).
  * **Gate.io** (`gateio`) — futures WS `futures.order_book`.
  * **BingX** (`bingx`) — swap WS.
* **Таргетные монеты**: Корзина из 36+ волатильных альтов (`ENA`, `WLD`, `SUI`, `NEAR`, `INJ`, `AAVE`, `APT`, `ARB`, `TIA`, `SEI`, `PEPE`, `FET`, `RENDER`, `LDO`, `FIL`, `TRUMP` и др.).

---

## 🛠️ 4. Что уже сделано и проверено (Completed Work)

1. **Создан и настроен репозиторий**:
   * Папка: `D:\GitHub\OBILeadLagCollector`.
   * Git инициализирован с локальной привязкой к `grishaSuperBoy`.
   * `.gitignore` надежно блокирует `.env`, логи, системный мусор и секретный документ `QUANT_ALPHA_STRATEGIES.md`.
2. **Настроена сборка через Poetry**:
   * `pyproject.toml` (`package-mode = false`, зависимости: `fastapi`, `uvicorn`, `websockets`, `pymongo[srv]`, `python-dotenv`, `pytest`).
   * Сгенерирован `poetry.lock`.
   * Установлены все пакеты в изолированное виртуальное окружение.
3. **Реализованы все модули системы (11 квантовых стратегий)**:
   * [models.py](file:///d:/GitHub/OBILeadLagCollector/models.py) — компактные датаклассы со `__slots__`: `OrderBookDepth5`, `OBIDislocationEvent`, `LiquiditySnapshot10m`, `PaperTrade`, `LiquidationEvent`, `CVDMetric`, `FundingScheduleEvent`, `LiquidityVoidMetric`, `TimeAnomalyMetric`, `ScrubberCycleMetric`.
   * [stream.py](file:///d:/GitHub/OBILeadLagCollector/stream.py) — асинхронные сокеты с авто-реконнектом для всех 10 бирж, а также стримы `!forceOrder@arr` (ликвидации), `!markPrice@arr@1s` (фандинг) и `@aggTrade` (CVD/VPIN).
   * [engine.py](file:///d:/GitHub/OBILeadLagCollector/engine.py) — квантовое ядро:
     * Расчет OBI и дислокаций $\ge 15$ bps, эмуляция Maker Limit.
     * `_evaluate_liquidity_void`: поиск пустых стаканов для «Тазиков» (Стратегия 10).
     * `_record_clock_tick`: профайлер минутных меток часа для «Тайм-ботов» (Стратегия 8).
     * `_record_scrubber_tick`: трекер циклических переворотов для «Ёршиков» (Стратегия 7).
     * 5-секундный CVD и VPIN Toxic Flow ($>80\%$ перекос напора).
     * Отслеживание часа Ч по фандингу ($T-5m$).
   * [mongo_worker.py](file:///d:/GitHub/OBILeadLagCollector/mongo_worker.py) — неблокирующий фоновый воркер батч-сброса в 5 коллекций MongoDB Atlas раз в 10 минут.
   * [server.py](file:///d:/GitHub/OBILeadLagCollector/server.py) — FastAPI сервис с эндпоинтами (`/health`, `/api/snapshot`, `/api/rankings`, `/api/dislocations`, `/api/trades`, `/api/liquidations`, `/api/funding`, `/api/voids`, `/api/time_bots`, `/api/scrubbers`, `/api/flush`) и расширенным тёмным дашбордом.
   * [main.py](file:///d:/GitHub/OBILeadLagCollector/main.py) — точка входа для Poetry и Render.
   * [render.yaml](file:///d:/GitHub/OBILeadLagCollector/render.yaml) — конфигурация для развертывания на Render в один клик.
   * [QUANT_ALPHA_STRATEGIES.md](file:///d:/GitHub/OBILeadLagCollector/QUANT_ALPHA_STRATEGIES.md) (в `.gitignore`) — исчерпывающая спецификация всех 11 стратегий в двунаправленном исполнении (Long & Short).
   * [tests/test_collector.py](file:///d:/GitHub/OBILeadLagCollector/tests/test_collector.py) — **10 юнит-тестов** (все 10 проходят успешно за 0.004с).
4. **Проведены реальные тесты и замеры**:
   * **Тесты**: 10 из 10 успешно (`OK`, 0.004s).
   * **Пропускная способность CPU**: **97 170 сообщений/сек** на одном ядре.
   * **Память RAM**: Пиковое выделение под динамические структуры — **`0.04 МБ` (40 КБ)**. Весь процесс весит ~55–65 МБ (при лимите Render в 512 МБ).
   * **MongoDB Atlas M0**: Соединение проверено вживую (`Atlas Connection Status: True`).

---

## ⚠️ 5. Важнейшие технические нюансы (Не забывать!)

### Нюанс 1: Защита от засыпания Render Free (Spin-Down)
* Бесплатный Render усыпляет контейнер через 15 минут без входящего HTTP-трафика.
* **Как решено**:
  В сервисе есть эндпоинт `GET /health`. На него натравливается бесплатный внешний пингер ([cron-job.org](https://cron-job.org) или UptimeRobot) с интервалом **10 минут**. Тогда сервис живет 24/7.

### Нюанс 2: Диск на Render эфемерный
* Нельзя сохранять данные в локальные SQLite или файлы — при перезапуске контейнера они сотрутся.
* **Как решено**:
  Все данные сбрасываются в облачную MongoDB Atlas. На локальном диске ничего важного не хранится.

### Нюанс 3: Лимит MongoDB Atlas Free (512 МБ)
* Если писать каждый тик стакана — база забьется за пару часов.
* **Как решено**:
  Мы пишем не тики, а **события дислокаций**, **эмуляцию сделок**, **крупные ликвидации**, **аномальный фандинг** и **10-минутные агрегаты ликвидности**. Это генерирует **~65–85 МБ в сутки**, чего хватает на **7–10 дней непрерывного сбора**.

### Нюанс 4: Безопасность переменных окружения и альфа-документов
* В репозиторий коммитится только `.env.example`.
* Сам `.env` с боевым паролем к MongoDB Atlas и секретный документ `QUANT_ALPHA_STRATEGIES.md` строго внесены в `.gitignore`.
* В Render URL базы задаётся в настройках сервиса в разделе **Environment Variables**.

---

## 📋 6. Что мы будем делать дальше (Immediate Next Steps)

1. **Создание удалённого репозитория на GitHub под `grishaSuperBoy`**:
   * Запросить у пользователя подтверждение: создаём репозиторий `OBILeadLagCollector` на GitHub под аккаунтом `grishaSuperBoy` через SSH-алиас `gh-grisha`?
   * Закоммитить чистый код (без `.env` и секретных документов) и запушить ветку `main`.
2. **Развертывание на Render.com**:
   * Подключить репозиторий в Render (New Web Service &rarr; `grishaSuperBoy/OBILeadLagCollector`).
   * Указать Build Command: `pip install poetry && poetry install --only main`.
   * Указать Start Command: `poetry run python main.py`.
   * Вбить переменную `MONGO_DB_URL` в Environment Variables на Render.
3. **Включение 24/7 мониторинга**:
   * Настроить пинг раз в 10 минут на `https://<render-url>/health` через cron-job.org.
4. **Сбор первых 24–48 часов данных**:
   * Сбор дислокаций OBI, пустых стаканов («тазики»), тайм-ботов по минутам и циклов «ёршиков» по 36 альтам на 10 биржах.

---

## 🚀 7. Что мы хотим сделать глобально (Конечная цель)

1. **Квантовый анализ базы маркет-мейкеров (Data Analysis)**:
   * Выгрузка из MongoDB в датафрейм (Polars/Pandas).
   * Построение рейтинга: **какие пары "Монета $\times$ Биржа" дают максимальное запаздывание, стабильные тайм-боты и пустые стаканы**.
   * Составление вайтлиста самых «медленных» и прибыльных связок.
2. **Переход к реальному исполнению (Live Trading Engine)**:
   * Подключение API-ключей к биржам из вайтлиста (Aster DEX, Hyperliquid, MEXC, Bitget, BingX).
   * Выставление реальных **Post-Only Maker лимитов** и сеток «тазиков» с жестким риск-менеджментом.
   * Эксплуатация неэффективностей слабого ММ на автомате.

