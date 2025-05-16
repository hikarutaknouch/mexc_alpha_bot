"""
MEXC Spot: 出来高 Top10 をロング → 8–12 h 後にクローズ
DRY_RUN=1 なら発注せずログだけ吐く
"""

import os
import asyncio
import logging
from datetime import datetime, timedelta, timezone

import ccxt.async_support as ccxt
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

from config import choose_hold_hours
from db import log_trade, due_trades, mark_closed

# ── 環境変数 ───────────────────────────
load_dotenv()
DRY_RUN = os.getenv("DRY_RUN", "1") == "1"

# ── ccxt クライアント ───────────────────
ex = ccxt.mexc({
    "apiKey": os.getenv("MEXC_KEY"),
    "secret": os.getenv("MEXC_SECRET"),
    "enableRateLimit": True,
    "timeout": 15000,                 # ms
})
ex.options["fetchCurrencies"] = False   # capital/config/getall タイムアウト回避

UTC = timezone.utc
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s: %(message)s")

# ── コア関数 ────────────────────────────
async def top10_symbols() -> list[str]:
    """24 h 出来高 Top10（USDT ペア）を返す"""
    raw = await ex.fetch_tickers()
    usdt = {k: v for k, v in raw.items() if k.endswith('/USDT')}
    ranked = sorted(usdt.items(),
                    key=lambda kv: kv[1]['quoteVolume'],
                    reverse=True)
    return [sym.replace('/', '') for sym, _ in ranked[:10]]

async def enter_positions() -> None:
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    symbols = await top10_symbols()

    if DRY_RUN:
        stake = 0.0
    else:
        bal = await ex.fetch_balance()          # ← 要「アカウント詳細を見る」権限
        stake = bal['USDT']['free'] * 0.1       # 口座の 10 %

    hold_h = choose_hold_hours()
    exit_at = now + timedelta(hours=hold_h)

    for sym in symbols:
        if DRY_RUN:
            logging.info(f"[DRY] BUY {sym}")
        else:
            await ex.create_order(sym, 'market', 'buy', stake)
        log_trade(sym, 'BUY', stake, exit_at)

    logging.info("Entered %d symbols, hold %dh", len(symbols), hold_h)

async def exit_due() -> None:
    now = datetime.now(UTC)
    for tr in due_trades(now):
        if DRY_RUN:
            logging.info(f"[DRY] SELL {tr.symbol}")
        else:
            await ex.create_order(tr.symbol, 'market', 'sell', tr.qty)
        mark_closed(tr)
        logging.info("Closed %s", tr.symbol)

# ── スケジューラ ────────────────────────
def main() -> None:
    sched = AsyncIOScheduler(timezone=UTC)
    sched.add_job(enter_positions, 'cron', hour=0, minute=1)   # 00:01 UTC
    sched.add_job(exit_due, 'interval', minutes=5)
    sched.start()
    logging.info("Scheduler started (DRY_RUN=%s)", DRY_RUN)
    asyncio.get_event_loop().run_forever()

# ── エントリーポイント ───────────────────
if __name__ == "__main__":
    try:
        main()
    finally:
        asyncio.run(ex.close())          # コネクタリーク防止