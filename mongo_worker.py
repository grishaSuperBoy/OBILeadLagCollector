"""
Async MongoDB Atlas (M0 Free Tier) Background Sync Worker.
Flushes batches every 10 minutes with zero event-loop blocking.
"""

import asyncio
import logging
import time
from typing import Any, Dict, Optional
from pymongo import MongoClient
from pymongo.errors import PyMongoError

import config
from engine import OBIEngine

log = logging.getLogger("mongo_worker")


class MongoBatchWorker:
    def __init__(self, engine: OBIEngine, mongo_url: Optional[str] = None, flush_interval_sec: int = 600):
        self.engine = engine
        self.mongo_url = mongo_url or config.MONGO_DB_URL
        self.flush_interval_sec = flush_interval_sec
        self.db_name = config.MONGO_DB_NAME

        self._client: Optional[MongoClient] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None

        self.last_flush_ts = 0.0
        self.last_flush_count = 0
        self.last_error = ""
        self.total_flushed_dislocations = 0
        self.total_flushed_liquidity = 0
        self.total_flushed_trades = 0

    def connect(self) -> bool:
        if not self.mongo_url:
            log.warning("[MongoDB] MONGO_DB_URL not configured. Running in memory-only mode.")
            return False

        try:
            self._client = MongoClient(
                self.mongo_url,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
                appname="OBILeadLagCollector",
            )
            # Verify connection with ping
            self._client.admin.command("ping")
            log.info(f"[MongoDB] Successfully connected to MongoDB Atlas (Database: {self.db_name})")
            return True
        except Exception as e:
            self.last_error = str(e)
            log.warning(f"[MongoDB] Connection failed: {e}. Worker will retry later.")
            self._client = None
            return False

    async def start(self):
        self._running = True
        self.connect()
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self):
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        # Final flush on shutdown
        await self.flush_now()

        if self._client:
            self._client.close()
            self._client = None

    async def _run_loop(self):
        while self._running:
            try:
                await asyncio.sleep(self.flush_interval_sec)
                await self.flush_now()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"[MongoDB Worker Error] {e}")
                await asyncio.sleep(30.0)

    async def flush_now(self) -> Dict[str, Any]:
        """Performs immediate batch flush of all unflushed engine buffers."""
        t0 = time.time()
        dislocations, liquidity, trades, liquidations, funding_anomalies = self.engine.extract_and_reset_unflushed()
        total_items = len(dislocations) + len(liquidity) + len(trades) + len(liquidations) + len(funding_anomalies)

        if total_items == 0:
            return {"status": "skipped", "count": 0, "message": "No new data to flush"}

        if not self._client:
            if not self.connect():
                return {"status": "error", "error": "MongoDB not connected", "unsynced_items": total_items}

        db = self._client[self.db_name]

        # Run non-blocking in executor
        loop = asyncio.get_running_loop()
        try:
            def _insert():
                res = {}
                if dislocations:
                    db["obi_dislocations"].insert_many(dislocations)
                    res["dislocations"] = len(dislocations)
                if liquidity:
                    db["liquidity_rollups_10m"].insert_many(liquidity)
                    res["liquidity"] = len(liquidity)
                if trades:
                    db["paper_trades"].insert_many(trades)
                    res["trades"] = len(trades)
                if liquidations:
                    db["market_liquidations"].insert_many(liquidations)
                    res["liquidations"] = len(liquidations)
                if funding_anomalies:
                    db["funding_anomalies"].insert_many(funding_anomalies)
                    res["funding_anomalies"] = len(funding_anomalies)

                # Update collector heartbeat
                db["collector_heartbeats"].update_one(
                    {"service": "obi_lead_lag"},
                    {
                        "$set": {
                            "last_heartbeat": time.time(),
                            "service": "obi_lead_lag",
                            "dislocations_count": self.total_flushed_dislocations + len(dislocations),
                            "trades_count": self.total_flushed_trades + len(trades),
                            "liquidations_count": len(liquidations),
                        }
                    },
                    upsert=True
                )
                return res

            flush_res = await loop.run_in_executor(None, _insert)

            elapsed_ms = round((time.time() - t0) * 1000, 1)
            self.last_flush_ts = time.time()
            self.last_flush_count = total_items
            self.last_error = ""

            self.total_flushed_dislocations += len(dislocations)
            self.total_flushed_liquidity += len(liquidity)
            self.total_flushed_trades += len(trades)


            log.info(
                f"[MongoDB] Batch flushed: {len(dislocations)} dislocations, "
                f"{len(liquidity)} liquidity rollups, {len(trades)} trades in {elapsed_ms}ms"
            )
            return {"status": "success", "elapsed_ms": elapsed_ms, **flush_res}

        except Exception as e:
            self.last_error = str(e)
            log.error(f"[MongoDB Flush Failed] {e}")
            return {"status": "failed", "error": str(e)}

    def get_status(self) -> Dict[str, Any]:
        return {
            "connected": self._client is not None,
            "flush_interval_sec": self.flush_interval_sec,
            "last_flush_ts": self.last_flush_ts,
            "last_flush_count": self.last_flush_count,
            "total_flushed_dislocations": self.total_flushed_dislocations,
            "total_flushed_liquidity": self.total_flushed_liquidity,
            "total_flushed_trades": self.total_flushed_trades,
            "last_error": self.last_error,
        }
