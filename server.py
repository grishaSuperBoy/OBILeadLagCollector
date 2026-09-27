"""
Lightweight FastAPI Server for Render.com Free Tier.
Binds to $PORT (0.0.0.0), serves real-time dark dashboard, and passes /health checks.
"""

import sys
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
from contextlib import asynccontextmanager
import logging
import os
import time
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse

import config
from engine import OBIEngine
from mongo_worker import MongoBatchWorker
from stream import StreamManager

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
from logger_buffer import log_handler
logging.getLogger().addHandler(log_handler)
log = logging.getLogger("server")

# Global component instances
engine: Optional[OBIEngine] = None
stream_manager: Optional[StreamManager] = None
mongo_worker: Optional[MongoBatchWorker] = None
start_time = time.time()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global engine, stream_manager, mongo_worker, start_time
    start_time = time.time()
    log.info(f"Initializing OBI Lead-Lag Collector on {len(config.SYMBOLS)} altcoins...")

    # 1. Initialize Engine
    engine = OBIEngine(
        lead_exchange=config.LEAD_EXCHANGE,
        lag_exchanges=config.LAG_EXCHANGES,
        symbols=config.SYMBOLS,
        obi_threshold=config.OBI_THRESHOLD,
        min_lead_lag_bps=config.MIN_LEAD_LAG_BPS,
        tp_bps=config.TP_BPS,
        sl_bps=config.SL_BPS,
        order_timeout_sec=config.ORDER_TIMEOUT_SEC,
        max_hold_sec=config.MAX_HOLD_SEC,
    )

    # 2. Initialize WebSocket Streams (Orderbooks + Liquidations + Funding + CVD AggTrades)
    stream_manager = StreamManager(
        lead_exchange=config.LEAD_EXCHANGE,
        lag_exchanges=config.LAG_EXCHANGES,
        symbols=config.SYMBOLS,
        callback=engine.on_depth_update,
        on_liquidation=engine.on_liquidation,
        on_funding=engine.on_funding_update,
        on_agg_trade=engine.on_agg_trade,
    )
    await stream_manager.start_all()

    # 3. Initialize MongoDB Batch Worker (10-minute bulk flush)
    mongo_worker = MongoBatchWorker(
        engine=engine,
        mongo_url=config.MONGO_DB_URL,
        flush_interval_sec=config.FLUSH_INTERVAL_SEC,
    )
    await mongo_worker.start()

    log.info(f"OBI Lead-Lag Service fully online! Ready on port {config.PORT}")
    yield

    # Shutdown
    log.info("Shutting down OBI Lead-Lag Service...")
    if stream_manager:
        await stream_manager.stop_all()
    if mongo_worker:
        await mongo_worker.stop()


app = FastAPI(
    title="OBI Lead-Lag & Weak MM Collector",
    description="High-Frequency Lead-Lag Dislocation Scanner & MongoDB Atlas Ingestion Engine",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health():
    """Render.com Keep-Alive and Health Check endpoint."""
    uptime = time.time() - start_time
    streams_st = stream_manager.get_status() if stream_manager else {}
    mongo_st = mongo_worker.get_status() if mongo_worker else {}

    lead_ok = streams_st.get(config.LEAD_EXCHANGE, {}).get("connected", False)
    connected_lags = sum(1 for ex, st in streams_st.items() if ex != config.LEAD_EXCHANGE and st.get("connected"))

    return JSONResponse({
        "status": "healthy" if lead_ok else "reconnecting",
        "uptime_sec": round(uptime, 1),
        "lead_exchange": config.LEAD_EXCHANGE,
        "lead_connected": lead_ok,
        "connected_lag_venues": f"{connected_lags}/{len(config.LAG_EXCHANGES)}",
        "symbols_tracked": len(config.SYMBOLS),
        "mongodb_connected": mongo_st.get("connected", False),
    })


@app.get("/api/snapshot")
async def api_snapshot():
    """Returns comprehensive JSON state snapshot for UI and remote consumers."""
    if not engine:
        return JSONResponse({"error": "Engine not initialized"}, status_code=503)

    snap = engine.get_snapshot()
    snap["uptime_sec"] = round(time.time() - start_time, 1)
    snap["stream_status"] = stream_manager.get_status() if stream_manager else {}
    snap["mongo_status"] = mongo_worker.get_status() if mongo_worker else {}
    return JSONResponse(snap)


@app.get("/api/dislocations")
async def api_dislocations(limit: int = Query(50, ge=1, le=200)):
    """Returns recent OBI dislocation events."""
    if not engine:
        return JSONResponse([])
    snap = engine.get_snapshot()
    return JSONResponse(snap.get("latest_dislocations", [])[:limit])


@app.get("/api/trades")
async def api_trades(limit: int = Query(50, ge=1, le=200)):
    """Returns recent paper trades."""
    if not engine:
        return JSONResponse([])
    snap = engine.get_snapshot()
    return JSONResponse(snap.get("latest_closed_trades", [])[:limit])


@app.get("/api/rankings")
async def api_rankings():
    """Returns ranking of weakest / slowest market makers across lag exchanges."""
    if not engine:
        return JSONResponse([])
    snap = engine.get_snapshot()
    return JSONResponse(snap.get("exchange_rankings", []))


@app.get("/api/liquidations")
async def api_liquidations(limit: int = Query(30, ge=1, le=100)):
    """Returns real-time Binance liquidation cascade events for front-running."""
    if not engine:
        return JSONResponse([])
    snap = engine.get_snapshot()
    return JSONResponse(snap.get("latest_liquidations", [])[:limit])


@app.get("/api/funding")
async def api_funding():
    """Returns extreme funding schedules and countdowns for Strategy 4."""
    if not engine:
        return JSONResponse([])
    snap = engine.get_snapshot()
    return JSONResponse(snap.get("extreme_funding_schedules", []))


@app.get("/api/voids")
async def api_voids():
    """Returns liquidity void candidates for Strategy 10 ('Тазики' / Ловля прострелов)."""
    if not engine:
        return JSONResponse([])
    snap = engine.get_snapshot()
    return JSONResponse(snap.get("liquidity_voids", []))


@app.get("/api/time_bots")
async def api_time_bots():
    """Returns scheduled minute-of-hour clock anomalies for Strategy 8 (Тайм-боты)."""
    if not engine:
        return JSONResponse([])
    snap = engine.get_snapshot()
    return JSONResponse(snap.get("time_anomalies", []))


@app.get("/api/scrubbers")
async def api_scrubbers():
    """Returns active cyclical MM scrubber algorithms for Strategy 7 ('Ёршики')."""
    if not engine:
        return JSONResponse([])
    snap = engine.get_snapshot()
    return JSONResponse(snap.get("active_scrubbers", []))


@app.post("/api/flush")
async def api_flush():
    """Manually triggers an immediate MongoDB Atlas batch flush."""
    if not mongo_worker:
        return JSONResponse({"error": "Mongo worker not active"}, status_code=503)
    res = await mongo_worker.flush_now()
    return JSONResponse(res)


@app.get("/api/logs")
async def api_logs(
    level: Optional[str] = Query(None, description="Minimum level filter: INFO, WARNING, ERROR"),
    logger: Optional[str] = Query(None, description="Logger name filter (e.g. stream, engine, mongo)"),
    limit: int = Query(100, ge=1, le=500),
):
    """Returns recent log messages from memory ring-buffer."""
    entries = log_handler.get_logs(level=level, logger_name=logger, limit=limit)
    return JSONResponse({"count": len(entries), "logs": entries})


@app.get("/api/errors")
async def api_errors(limit: int = Query(100, ge=1, le=500)):
    """Convenience endpoint returning only WARNING, ERROR, and CRITICAL logs."""
    errors = log_handler.get_errors(limit=limit)
    return JSONResponse({"count": len(errors), "errors": errors})


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Ultra-lightweight dark-mode real-time dashboard."""
    html_content = """<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>OBI Lead-Lag & Weak Market Maker Scanner</title>
  <style>
    :root {
      --bg: #090d16;
      --card: #131b2e;
      --border: #202b42;
      --text: #e6edf3;
      --muted: #8b949e;
      --green: #2ea043;
      --red: #f85149;
      --accent: #58a6ff;
      --yellow: #d29922;
      --purple: #bc8cff;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
    body { background: var(--bg); color: var(--text); padding: 20px; font-size: 13px; }
    .header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid var(--border); padding-bottom: 16px; margin-bottom: 20px; }
    .title { font-size: 20px; font-weight: 700; display: flex; align-items: center; gap: 10px; }
    .badge { padding: 4px 10px; border-radius: 12px; font-size: 11px; font-weight: 700; text-transform: uppercase; }
    .badge.green { background: rgba(46,160,67,0.15); color: var(--green); border: 1px solid var(--green); }
    .badge.blue { background: rgba(88,166,255,0.15); color: var(--accent); border: 1px solid var(--accent); }
    .badge.yellow { background: rgba(210,153,34,0.15); color: var(--yellow); border: 1px solid var(--yellow); }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 14px; margin-bottom: 24px; }
    .card { background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 14px; }
    .card .label { font-size: 11px; text-transform: uppercase; color: var(--muted); margin-bottom: 6px; }
    .card .val { font-size: 22px; font-weight: 700; }
    .section-title { font-size: 15px; font-weight: 600; margin: 20px 0 10px; display: flex; align-items: center; gap: 8px; }
    table { width: 100%; border-collapse: collapse; background: var(--card); border-radius: 8px; overflow: hidden; border: 1px solid var(--border); margin-bottom: 20px; }
    th, td { padding: 9px 12px; text-align: left; border-bottom: 1px solid var(--border); font-size: 12px; }
    th { background: #1a233a; color: var(--muted); font-weight: 600; }
    .tag-buy { color: var(--green); font-weight: bold; }
    .tag-sell { color: var(--red); font-weight: bold; }
    .btn { background: var(--accent); color: #000; border: none; padding: 6px 14px; border-radius: 6px; font-size: 12px; font-weight: 600; cursor: pointer; }
    .btn:hover { opacity: 0.9; }
  </style>
</head>
<body>
  <div class="header">
    <div>
      <div class="title">
        <span>⚡ OBI Lead-Lag & Weak Market Maker Scanner</span>
        <span class="badge blue">Render Free Tier Ready</span>
      </div>
      <p style="color: var(--muted); margin-top: 4px;">
        Lead: <strong style="color:var(--accent);">Binance Futures</strong> &rarr; 
        Lag Venues: <strong style="color:var(--yellow);">Aster DEX, Hyperliquid, dYdX, Bybit, OKX, Bitget, MEXC, Gate</strong> • 
        Target: <strong style="color:#fff;">Altcoins (Weak MMs)</strong>
      </p>
    </div>
    <div style="display:flex; gap:10px; align-items:center;">
      <button class="btn" onclick="triggerFlush()">Sync to Mongo</button>
      <span class="badge green" id="statusBadge">● LIVE WS</span>
    </div>
  </div>

  <div class="grid">
    <div class="card">
      <div class="label">Поймано дислокаций</div>
      <div class="val" id="statDislocations" style="color:var(--accent);">0</div>
    </div>
    <div class="card">
      <div class="label">Сделок Maker (Fill Rate)</div>
      <div class="val" id="statTrades">0</div>
    </div>
    <div class="card">
      <div class="label">Win Rate сделок</div>
      <div class="val" id="statWinRate" style="color:var(--green);">0%</div>
    </div>
    <div class="card">
      <div class="label">Net PnL (Simulated)</div>
      <div class="val" id="statPnL">$0.00</div>
    </div>
    <div class="card">
      <div class="label">MongoDB Sync (10m)</div>
      <div class="val" id="statMongo" style="font-size:14px; color:var(--yellow); padding-top:6px;">Connected</div>
    </div>
  </div>

  <!-- Exchange Latency Rankings -->
  <div class="section-title">🏆 Рейтинг отставания бирж (Где маркет-мейкер тормозит сильнее всего)</div>
  <table>
    <thead><tr>
      <th>Биржа (Lag Venue)</th><th>Зафиксировано лагов</th><th>Средний разрыв (Lag bps)</th>
      <th>Исполнено Maker</th><th>Win Rate %</th><th>Net PnL ($)</th><th>Статус робота ММ</th>
    </tr></thead>
    <tbody id="rankingsBody"><tr><td colspan="7" style="color:var(--muted); text-align:center;">Ожидание накопления данных...</td></tr></tbody>
  </table>

  <!-- Recent Dislocation Events -->
  <div class="section-title">⚡ Свежие импульсы OBI и лаги цен (Binance Lead &rarr; Lagging Venue)</div>
  <table>
    <thead><tr>
      <th>Время</th><th>Альткоин</th><th>Направление</th><th>Binance OBI</th>
      <th>Отстающая биржа</th><th>Разрыв спреда (Lag bps)</th><th>Maker Limit в стакан</th>
    </tr></thead>
    <tbody id="dislocationsBody"><tr><td colspan="7" style="color:var(--muted); text-align:center;">Поиск дислокаций...</td></tr></tbody>
  </table>

  <!-- Live Trades -->
  <div class="section-title">📊 Эмуляция входов Maker Limit на откате отстающей биржи</div>
  <table>
    <thead><tr>
      <th>Монета</th><th>Биржа</th><th>Сторона</th><th>Лимит</th><th>Статус</th><th>Выход</th><th>Причина</th><th>Net PnL</th>
    </tr></thead>
    <tbody id="tradesBody"><tr><td colspan="8" style="color:var(--muted); text-align:center;">Нет открытых ордеров</td></tr></tbody>
  </table>

  <!-- Liquidity Voids (Тазики) -->
  <div class="section-title">🏺 Тазики & Дыры в стакане (Стратегия 10: Ловля прострелов на пустых стаканах)</div>
  <table>
    <thead><tr>
      <th>Монета</th><th>Биржа</th><th>Mid Price</th><th>Глубина Bid ($)</th><th>Глубина Ask ($)</th>
      <th>Тазик LONG (Bid -4%)</th><th>Тазик SHORT (Ask +4%)</th><th>Статус стакана</th>
    </tr></thead>
    <tbody id="voidsBody"><tr><td colspan="8" style="color:var(--muted); text-align:center;">Сканирование плотности стаканов...</td></tr></tbody>
  </table>

  <!-- Scheduled Time Bots -->
  <div class="section-title">⏱️ Тайм-боты (Стратегия 8: Роботы по минутам часа :17, :34, :43)</div>
  <table>
    <thead><tr>
      <th>Монета</th><th>Минута часа</th><th>Срабатываний</th><th>Средний импульс</th>
      <th>Сторона</th><th>Достоверность</th><th>Статус расписания</th>
    </tr></thead>
    <tbody id="timeBotsBody"><tr><td colspan="7" style="color:var(--muted); text-align:center;">Профайлинг минутного расписания...</td></tr></tbody>
  </table>

  <!-- Scrubber Algos (Ёршики) -->
  <div class="section-title">🪥 Ёршики (Стратегия 7: Циклические колебания тупого маркет-мейкера)</div>
  <table>
    <thead><tr>
      <th>Монета</th><th>Биржа</th><th>Период цикла (сек)</th><th>Амплитуда</th>
      <th>Текущая фаза</th><th>Смен направления</th><th>Рекомендация</th>
    </tr></thead>
    <tbody id="scrubbersBody"><tr><td colspan="7" style="color:var(--muted); text-align:center;">Поиск циклических ММ...</td></tr></tbody>
  </table>

  <!-- In-Memory System Logs & Errors Console -->
  <div class="section-title">
    <span>📋 Системный лог и ошибки в оперативной памяти (In-Memory Buffer)</span>
    <span style="font-size:11px; color:var(--muted); font-weight:normal; margin-left:10px;">
      [Эндпоинты: <a href="/api/logs" target="_blank" style="color:var(--accent);">/api/logs</a> • <a href="/api/errors" target="_blank" style="color:var(--red);">/api/errors</a>]
    </span>
  </div>
  <div style="background:var(--card); border:1px solid var(--border); border-radius:8px; padding:12px; max-height:220px; overflow-y:auto; font-family:monospace; font-size:11px; line-height:1.5; margin-bottom:24px;" id="logsConsole">
    <div style="color:var(--muted);">Загрузка логов...</div>
  </div>

  <script>
    async function updateData() {
      try {
        const res = await fetch('/api/snapshot');
        if (!res.ok) return;
        const d = await res.json();

        // Top Stats
        document.getElementById('statDislocations').innerText = d.stats.total_dislocations;
        document.getElementById('statTrades').innerText = d.stats.total_trades;
        document.getElementById('statWinRate').innerText = d.stats.win_rate_pct + '%';
        const pnl = d.stats.total_net_pnl_usd;
        document.getElementById('statPnL').innerText = (pnl >= 0 ? '+' : '') + pnl + ' $';
        document.getElementById('statPnL').style.color = pnl >= 0 ? 'var(--green)' : 'var(--red)';

        if (d.mongo_status && d.mongo_status.connected) {
          document.getElementById('statMongo').innerText = 'Atlas M0: ' + d.mongo_status.total_flushed_dislocations + ' events';
        }

        // 1. Rankings Table
        const rkBody = document.getElementById('rankingsBody');
        if (d.exchange_rankings && d.exchange_rankings.length) {
          rkBody.innerHTML = d.exchange_rankings.map(r => `
            <tr>
              <td><strong>${r.exchange.toUpperCase()}</strong></td>
              <td>${r.dislocations_detected}</td>
              <td style="color:var(--yellow); font-weight:bold;">${r.avg_lag_bps > 0 ? '+' : ''}${r.avg_lag_bps} bps</td>
              <td>${r.trades_filled}</td>
              <td style="color:${r.win_rate >= 50 ? 'var(--green)' : 'var(--red)'};">${r.win_rate}%</td>
              <td style="color:${r.net_pnl_usd >= 0 ? 'var(--green)' : 'var(--red)'}; font-weight:bold;">${r.net_pnl_usd >= 0 ? '+' : ''}${r.net_pnl_usd} $</td>
              <td><span class="badge ${r.avg_lag_bps >= 20 ? 'yellow' : 'blue'}">${r.avg_lag_bps >= 20 ? 'Слабый ММ (Лагает)' : 'Стандартный'}</span></td>
            </tr>
          `).join('');
        }

        // 2. Dislocations Table
        const disBody = document.getElementById('dislocationsBody');
        if (d.latest_dislocations && d.latest_dislocations.length) {
          disBody.innerHTML = d.latest_dislocations.slice(0, 15).map(e => `
            <tr>
              <td style="color:var(--muted);">${new Date(e.ts * 1000).toLocaleTimeString()}</td>
              <td><strong>${e.symbol}</strong></td>
              <td class="${e.side === 'BUY' ? 'tag-buy' : 'tag-sell'}">${e.side}</td>
              <td>${e.lead_obi > 0 ? '+' : ''}${e.lead_obi}</td>
              <td><span class="badge blue">${e.lag_exchange.toUpperCase()}</span></td>
              <td style="color:var(--yellow); font-weight:bold;">${e.lag_bps > 0 ? '+' : ''}${e.lag_bps} bps</td>
              <td style="color:var(--text); font-family:monospace;">${e.maker_limit_price}</td>
            </tr>
          `).join('');
        }

        // 3. Trades Table
        const trBody = document.getElementById('tradesBody');
        const allTrades = [...(d.active_orders || []), ...(d.latest_closed_trades || [])];
        if (allTrades.length) {
          trBody.innerHTML = allTrades.slice(0, 15).map(t => `
            <tr>
              <td><strong>${t.symbol}</strong></td>
              <td>${t.exchange.toUpperCase()}</td>
              <td class="${t.side === 'BUY' ? 'tag-buy' : 'tag-sell'}">${t.side}</td>
              <td style="font-family:monospace;">${t.limit_price}</td>
              <td><span class="badge ${t.status === 'FILLED' ? 'green' : (t.status === 'PENDING' ? 'yellow' : 'blue')}">${t.status}</span></td>
              <td style="font-family:monospace;">${t.exit_price || '-'}</td>
              <td>${t.exit_reason || '-'}</td>
              <td style="color:${(t.net_pnl_usd || 0) >= 0 ? 'var(--green)' : 'var(--red)'}; font-weight:bold;">
                ${t.net_pnl_usd !== undefined ? (t.net_pnl_usd >= 0 ? '+' : '') + t.net_pnl_usd + ' $' : '-'}
              </td>
            </tr>
          `).join('');
        }

        // 4. Liquidity Voids Table (Тазики)
        const vBody = document.getElementById('voidsBody');
        if (d.liquidity_voids && d.liquidity_voids.length) {
          vBody.innerHTML = d.liquidity_voids.slice(0, 10).map(v => `
            <tr>
              <td><strong>${v.symbol}</strong></td>
              <td><span class="badge blue">${v.exchange.toUpperCase()}</span></td>
              <td style="font-family:monospace;">${v.mid_price}</td>
              <td style="color:var(--yellow); font-weight:bold;">$${v.depth_bid_usd}</td>
              <td style="color:var(--yellow); font-weight:bold;">$${v.depth_ask_usd}</td>
              <td style="color:var(--green); font-family:monospace; font-weight:bold;">${v.trough_bid_limit}</td>
              <td style="color:var(--red); font-family:monospace; font-weight:bold;">${v.trough_ask_limit}</td>
              <td><span class="badge yellow">🕳️ Дыра (Тазик активен)</span></td>
            </tr>
          `).join('');
        }

        // 5. Time Bots Table
        const tbBody = document.getElementById('timeBotsBody');
        if (d.time_anomalies && d.time_anomalies.length) {
          tbBody.innerHTML = d.time_anomalies.slice(0, 10).map(tb => `
            <tr>
              <td><strong>${tb.symbol}</strong></td>
              <td style="color:var(--accent); font-weight:bold; font-size:14px;">:${tb.minute_of_hour < 10 ? '0' : ''}${tb.minute_of_hour}</td>
              <td>${tb.occurrences}x</td>
              <td style="color:var(--yellow);">${tb.avg_dislocation_bps} bps</td>
              <td class="${tb.dominant_side === 'BUY' ? 'tag-buy' : (tb.dominant_side === 'SELL' ? 'tag-sell' : '')}">${tb.dominant_side}</td>
              <td><span class="badge green">${Math.round(tb.confidence_score * 100)}%</span></td>
              <td><span class="badge blue">Срабатывание по расписанию</span></td>
            </tr>
          `).join('');
        }

        // 6. Scrubbers Table (Ёршики)
        const scBody = document.getElementById('scrubbersBody');
        if (d.active_scrubbers && d.active_scrubbers.length) {
          scBody.innerHTML = d.active_scrubbers.slice(0, 10).map(sc => `
            <tr>
              <td><strong>${sc.symbol}</strong></td>
              <td><span class="badge blue">${sc.exchange.toUpperCase()}</span></td>
              <td>${sc.cycle_period_sec} с</td>
              <td style="color:var(--yellow);">${sc.amplitude_bps} bps</td>
              <td><span class="badge ${sc.current_phase === 'BUY_PHASE' ? 'green' : 'yellow'}">${sc.current_phase}</span></td>
              <td>${sc.flips_count} переворотов</td>
              <td style="color:var(--accent); font-weight:bold;">${sc.current_phase === 'BUY_PHASE' ? 'Готовить SHORT на пике' : 'Готовить LONG на дне'}</td>
            </tr>
          `).join('');
        }

        // 7. System Logs Console
        try {
          const lRes = await fetch('/api/logs?limit=30');
          if (lRes.ok) {
            const lData = await lRes.json();
            if (lData.logs && lData.logs.length) {
              const logsBox = document.getElementById('logsConsole');
              logsBox.innerHTML = lData.logs.map(l => {
                let color = 'var(--text)';
                if (l.level === 'WARNING') color = 'var(--yellow)';
                else if (l.level === 'ERROR' || l.level === 'CRITICAL') color = 'var(--red)';
                else if (l.level === 'INFO') color = '#9ecbff';
                return `<div style="border-bottom:1px solid rgba(255,255,255,0.05); padding:2px 0;">
                  <span style="color:var(--muted);">${new Date(l.created * 1000).toLocaleTimeString()}</span>
                  <span style="color:${color}; font-weight:bold; margin:0 6px;">[${l.level}]</span>
                  <span style="color:var(--accent); margin-right:6px;">${l.logger}:</span>
                  <span>${l.message.replace(/</g, '&lt;')}</span>
                </div>`;
              }).join('');
            }
          }
        } catch (e) {}

      } catch (err) {
        console.error(err);
      }
    }

    async function triggerFlush() {
      const btn = event.target;
      btn.innerText = 'Syncing...';
      try {
        const res = await fetch('/api/flush', { method: 'POST' });
        const d = await res.json();
        alert('MongoDB Sync: ' + JSON.stringify(d));
      } finally {
        btn.innerText = 'Sync to Mongo';
        updateData();
      }
    }

    setInterval(updateData, 1000);
    updateData();
  </script>
</body>
</html>
    """
    return HTMLResponse(content=html_content)
