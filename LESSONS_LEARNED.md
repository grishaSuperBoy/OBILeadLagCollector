# 🧠 База Знаний и Извлеченные Уроки (Lessons Learned)

Этот документ содержит специфичные особенности (quirks), баги, грабли и решения, полученные "кровью и токенами". 
**ВНИМАНИЕ АГЕНТУ:** API вендоров постоянно меняются. Используй этот файл как отправную точку и для быстрого распознавания симптомов, но **ВСЕГДА** делай поправку на то, что доки могли обновиться. Сверяй старый опыт с актуальной документацией.

---

## 1. Hyperliquid: Внезапные обрывы WebSocket (no close frame)

**Симптомы:** 
Сразу после успешного коннекта (Connected! Subscribing to l2Book...) вебсокет падает с ошибкой WS error: no close frame received or sent или 1006. Реконнекты не помогают, цикл повторяется бесконечно.

**Корень проблемы:**
Hyperliquid работает как очень жесткий шлюз. Если отправить подписку на **несуществующую или неподдерживаемую монету** (например, старый тикер, которого нет на бирже, или 1000PEPE вместо kPEPE), сервер не присылает JSON с ошибкой. Он **моментально и без предупреждения закрывает TCP-соединение**.

**Решение (Паттерн):**
Никогда не подписываться на хардкодный список монет вслепую. Всегда сначала запрашивать актуальный universe через REST API и делать строгое пересечение (intersection).

**Пример кода (Python):**
```python
import urllib.request
import json

# 1. Запрашиваем актуальные монеты
req = urllib.request.Request(
    'https://api.hyperliquid.xyz/info', 
    data=json.dumps({'type': 'meta'}).encode('utf-8'),
    headers={'Content-Type': 'application/json'}
)
with urllib.request.urlopen(req, timeout=5) as response:
    resp = json.loads(response.read().decode('utf-8'))
    valid_hl_coins = {a['name'] for a in resp['universe']}

# 2. Фильтруем нашу корзину перед отправкой подписки
for coin in my_basket:
    # Учет префиксов (Hyperliquid использует 'k' вместо '1000' для мем-коинов)
    if coin.startswith("1000"):
        hl_coin = "k" + coin[4:]
        if hl_coin in valid_hl_coins:
            coin = hl_coin
            
    if coin not in valid_hl_coins:
        continue # Игнорируем, иначе биржа порвет соединение!
        
    # Отправляем подписку...
```

## 2. Hyperliquid: Требования к Ping-Pong
**Симптомы:** Отвал по таймауту через 60 секунд.
**Корень проблемы:** Hyperliquid не отвечает на стандартные TCP Ping-фреймы, которые шлет библиотека `websockets` (аргументы `ping_interval` / `ping_timeout`).
**Решение:** Отключать встроенные пинги (`ping_interval=None, ping_timeout=None`) и слать мануальный пинг уровня приложения (`await ws.send(json.dumps({"method": "ping"}))`) каждые 20 секунд в фоновом таске.

---

## 3. Binance Futures: Заблуждение о `market/ws` и правильный стриминг Depth5
**Симптомы:**
Соединение с `wss://fstream.binance.com/market/ws` успешно устанавливается, на `{"method": "SUBSCRIBE", "params": ["btcusdt@depth5@100ms"]}` приходит подтверждение `{"result": null, "id": 1}`, но **никаких апдейтов стакана не приходит вообще**. В результате OBI-движок считает, что стакан пуст, и не находит ни одной дислокации.
**Корень проблемы:**
Эндпоинт `market/ws` на Binance Futures предназначен только для агрегированных рыночных потоков (`!forceOrder@arr`, `!markPrice@arr@1s`, `@ticker`). Подписки на стаканы `@depth5@100ms` через `SUBSCRIBE` на этом эндпоинте игнорируются!
**Решение:**
Использовать мультиплексированный эндпоинт Binance:
`wss://fstream.binance.com/stream?streams=btcusdt@depth5@100ms/ethusdt@depth5@100ms/...`
- Не требует отправки JSON `SUBSCRIBE`.
- Моментально начинает гнать 100ms стаканы.
- Возвращает структуру `{"stream": "...", "data": {"s": "BTCUSDT", "b": [...], "a": [...]}}`.
- Шардируется пачками по 50 монет на соединение.

---

## 4. Crypto WebSockets: Ошибка `1011 keepalive ping timeout`
**Симптомы:**
Сокет подключается, работает ровно 20-40 секунд, затем падает с:
`sent 1011 (internal error) keepalive ping timeout; no close frame received.`
**Корень проблемы:**
Библиотека Python `websockets` по умолчанию имеет `ping_interval=20, ping_timeout=20`. Она шлет управляющие фреймы RFC 6455 Ping (opcode 0x9). Почти ни одна криптобиржа (AsterDEX, OKX, BingX, MEXC, Gate.io, dYdX) **не отвечает** на фреймовые пинги RFC 6455 — они ожидают JSON-сообщения (`{"op":"ping"}` или `{"channel":"futures.ping"}` или `"ping"`). Не дождавшись фреймового Pong, клиент сам разрывает соединение с ошибкой 1011.
**Решение:**
Всегда указывать `ping_interval=None, ping_timeout=None` в `websockets.connect()`! 
А следить за зависанием соединения через watchdog:
`msg = await asyncio.wait_for(ws.recv(), timeout=20.0)`
(при потоке стаканов 50 альткоинов сообщения приходят десятки раз в секунду).

---

## 5. Windows: Зависшие дочерние процессы Python и порт 8080 (`[Errno 10048]`)
**Симптомы:**
Убийство родительского таска оставляет процесс `uvicorn` висящим в фоне. Перезапуск падает с ошибкой `[Errno 10048] error while attempting to bind on address ('0.0.0.0', 8080): only one usage of each socket address is normally permitted`.
**Решение:**
Перед запуском сервиса всегда проверять и очищать порт:
```powershell
Get-NetTCPConnection -LocalPort 8080 -ErrorAction SilentlyContinue | ForEach-Object { Stop-Process -Id $_.OwningProcess -Force }
```

---

## 6. Asyncio: Синхронные блокирующие вызовы в конструкторах стримов (Event Loop Freeze)
**Симптомы:**
Сразу после старта сервера все 33 вебсокет-соединения массово отваливаются с пустыми ошибками или таймаутами через 10–12 секунд.
**Корень проблемы:**
Вызов синхронного сетевого метода `urllib.request.urlopen` внутри `__init__` класса стрима (например, получение universe Hyperliquid). При шардинге на 6 инстансов 6 синхронных HTTP-запросов заморозили event loop на 8 секунд. Все открывающиеся сокеты пропустили сетевые тики и были сброшены серверами по таймауту.
**Решение:**
В асинхронных приложениях **категорически запрещены** синхронные блокирующие вызовы (`urllib`, `requests`, `time.sleep`). Вся предзагрузка справочников должна выполняться один раз асинхронно через `aiohttp`/`httpx` на уровне менеджера потоков до старта сокетов.

---

## 7. Lead-Lag: Слишком жесткий порог покоя стакана на ведомых DEX (Stale Threshold)
**Симптомы:**
Стаканы со всех бирж поступают, OBI на Binance регулярно превышает $\pm 0.50$, но дислокации с DEX (Hyperliquid, AsterDEX, dYdX) не фиксируются (`dislocations = 0`).
**Корень проблемы:**
В движке стояла проверка `if abs(now - lag_snap.ts) > 2.5: continue`. На Binance стаканы обновляются каждые 100 мс, но на DEX на неликвидных альтах котировки маркет-мейкера могут спокойно стоять неподвижно 4–8 секунд, пока нет сделок. Движок отбрасывал эти стаканы как «устаревшие», хотя именно в этой неподвижности и заключалась альфа!
**Решение:**
Для ведомых DEX-площадок порог покоя стакана расширен до 10.0–15.0 секунд при условии, что сам вебсокет жив и получает heartbeat/апдейты по другим парам.

---

## 8. Windows PowerShell: Ошибка кодировки `UnicodeEncodeError` при выводе эмодзи
**Симптомы:**
Скрипты и тесты падают с `UnicodeEncodeError: 'charmap' codec can't encode character ...: character maps to <undefined>`.
**Корень проблемы:**
Стандартная консоль Windows PowerShell работает в кодировке `cp1252` или `cp866`, которая не поддерживает символы Unicode Emoji (🛡️, ⏱️, 🚨).
**Решение:**
1. Всегда добавлять в заголовки Python-скриптов:
```python
import sys
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
```
2. В системных логах, тестах и скриптах использовать чистые текстовые ASCII-теги: `[PASS]`, `[FAIL]`, `[ALERT]` вместо эмодзи.


