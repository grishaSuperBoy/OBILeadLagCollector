# ⚡ OBI Lead-Lag & Weak Market Maker Collector

> **Высокопроизводительный асинхронный сканер дислокаций книги заявок (Order Book Imbalance) и сборщик данных по слабым маркет-мейкерам на альтах.**  
> Разработан с жёстким соблюдением лимитов бесплатного тарифа **Render.com Free Tier (RAM < 80 MB, 0.1 vCPU)** и автоматической выгрузкой в облачную **MongoDB Atlas M0 (512 MB Free)**.

---

## 🎯 Философия и идея стратегии

На мегакапах (**BTC, ETH, SOL**) арбитраж съедают институциональные HFT-маркетмейкеры (Wintermute, Jump, GSR), синхронизирующие стаканы за микросекунды.

**Настоящая прибыль лежит в альткоинах со слабыми маркет-мейкерами:**
1. На альтах со средней и низкой ликвидностью на ведомых биржах роботы маркет-мейкеров часто работают на примитивных скриптах с задержкой в **1–3 секунды** или вовсе отсутствуют.
2. Когда на **Binance Futures** (ведущей бирже мира) происходит импульс в стакане ($|OBI| \ge 0.50$, перекос объемов покупателей/продавцов более 75/25), на ведомой бирже стакан стоит на месте.
3. Разрыв цены (Lead-Lag Displacement) раздувается до **$15$ – $150$ bps ($0.15\%$ – $1.50\%$)**.
4. Сервис ловит это запаздывание, ставит виртуальный **Maker Post-Only** лимит на ведомой бирже на микро-откате и фиксирует профит при выравнивании стакана.

---

## 🏛️ Архитектура бирж

| Роль | Площадка | Тип | Протокол подключения |
| :--- | :--- | :--- | :--- |
| **LEAD (Ведущая)** | **Binance Futures** | CEX | WebSocket `depth5@100ms` (Multi-stream) |
| **LAG (Ведомая)** | **Aster DEX** | Perp DEX | WebSocket Binance-protocol `depth5@100ms` |
| **LAG (Ведомая)** | **Hyperliquid** | Perp DEX | Native L1 WebSocket `l2Book` (CLOB) |
| **LAG (Ведомая)** | **dYdX v4** | Perp DEX | Cosmos Indexer WS `v4_orderbook` |
| **LAG (Ведомая)** | **Bybit** | CEX | Linear WS `orderbook.50` |
| **LAG (Ведомая)** | **OKX** | CEX | Public WS `books5` |
| **LAG (Ведомая)** | **Bitget** | CEX | Futures WS `books5` |
| **LAG (Ведомая)** | **MEXC** | CEX | Contract WS `sub.depth.full` |
| **LAG (Ведомая)** | **Gate.io** | CEX | Futures WS `futures.order_book` |

---

## 💾 Сбор данных в MongoDB Atlas (Раз в 10 минут)

Сервис не забивает базу терабайтами холостого шума. Каждые 10 минут воркер сбрасывает компактные батчи в 3 коллекции:

1. **`obi_dislocations`** — зафиксированные импульсы: время, монета, сторона, OBI бинанса, отстающая биржа, величина лага в bps, рекомендованная цена лимита.
2. **`liquidity_rollups_10m`** — срез «живучести» маркет-мейкера: средний спред за 10 минут, плотность стакана (Bid/Ask Depth в $\$$), количество тиков активности робота.
3. **`paper_trades`** — журнал эмуляции сделок: факт налива Maker-ордера (Fill Rate), проскальзывание, причина закрытия (Take-Profit, Stop-Loss, TimeStop), чистый PnL с комиссией.

> **Объём данных:** ~**50–70 МБ в сутки**. Бесплатного хранилища MongoDB Atlas M0 (512 МБ) хватает на **7–10 суток непрерывного сбора**.

---

## 🚀 Деплой на Render.com (Free Tier)

### 1. Подготовка
Репозиторий настроен на аккаунт GitHub `grishaSuperBoy`.

В Render нажми: **New + &rarr; Blueprint** (или **Web Service**) и выбери репозиторий.
Render автоматически подхватит файл `render.yaml` со всеми настройками!

### 2. Настройки (автоматически из render.yaml)
* **Region**: `Frankfurt (EU Central)` *(критично для прямого доступа к API Binance, Bybit, OKX без геоблокировок США)*
* **Environment**: `Python 3` (3.11.9)
* **Build Command**: `pip install -r requirements.txt`
* **Start Command**: `python main.py`
* **Plan**: `Free`
* **Health Check Path**: `/health`

### 3. Переменные окружения (Environment Variables) в Render
Добавь в разделе **Environment**:
```env
MONGO_DB_URL=mongodb+srv://<username>:<password>@cluster0.wgp2hmv.mongodb.net/obi_lead_lag?retryWrites=true&w=majority
ENVIRONMENT=production
FLUSH_INTERVAL_SEC=600
MIN_LEAD_LAG_BPS=15.0
OBI_THRESHOLD=0.50
```

### 4. Защита от засыпания (24/7 Uptime)
Бесплатный Render уходит в сон через 15 минут без HTTP-трафика.
Зайди на бесплатный сервис [cron-job.org](https://cron-job.org) или [uptimerobot.com](https://uptimerobot.com) и поставь пинг каждые **10 минут** на адрес:
```
https://<твой-сервис>.onrender.com/health
```
Сервис будет стабильно работать и собирать базу круглосуточно.

---

## 🛠️ Локальный запуск через Poetry

```bash
# 1. Установка зависимостей
poetry install

# 2. Запуск тестов
poetry run python -m unittest discover tests

# 3. Запуск сервиса
poetry run python main.py
```
После старта веб-интерфейс доступен по адресу: `http://localhost:8080`.

---

## 📡 REST API Эндпоинты

* **`GET /`** — Встроенный сверхлегкий тёмный дашборд со сводкой в реальном времени.
* **`GET /health`** — Статус здоровья сервиса, аптайм, проверка связи с каждой биржей и MongoDB.
* **`GET /api/snapshot`** — Полный JSON-снимок состояния (стаканы, активные ордера, статистика).
* **`GET /api/rankings`** — **Рейтинг бирж по отставанию** (где маркет-мейкер тормозит сильнее всего).
* **`GET /api/dislocations?limit=50`** — История OBI-импульсов и расхождений.
* **`GET /api/trades?limit=50`** — История исполнения виртуальных Maker-ордеров.
* **`POST /api/flush`** — Принудительный сброс буферов в MongoDB вне 10-минутного расписания.

---

## 🔒 Безопасность
* Файл `.env` с паролями и токенами базы внесён в `.gitignore` и **никогда не попадает в публичный репозиторий**.
* Все объекты используют `__slots__` — отсутствие циклических ссылок и минимальная сборка мусора (GC).
