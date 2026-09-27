"""
Entrypoint for Render.com Free Web Service and local execution.
Usage:
    poetry run python main.py
    # or
    python main.py
"""

import os
import uvicorn
import config


def run():
    port = int(os.environ.get("PORT", config.PORT))
    print(f"============================================================")
    print(f"🚀 OBI Lead-Lag & Weak Market Maker Collector Starting...")
    print(f"📡 Lead Exchange : {config.LEAD_EXCHANGE.upper()}")
    print(f"⏳ Lag Venues    : {', '.join(e.upper() for e in config.LAG_EXCHANGES)}")
    print(f"🎯 Target Alts   : {len(config.SYMBOLS)} volatile altcoins")
    print(f"💾 MongoDB Atlas : {'Configured' if config.MONGO_DB_URL else 'Not set (Memory only)'}")
    print(f"🌐 Server Binding: 0.0.0.0:{port}")
    print(f"============================================================")

    uvicorn.run(
        "server:app",
        host="0.0.0.0",
        port=port,
        log_level="info",
        access_log=False,  # Minimizes CPU overhead on Render Free Tier
    )


if __name__ == "__main__":
    run()
