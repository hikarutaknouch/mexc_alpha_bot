"""
MEXC Spot: 出来高 Top10 をロング → 8–12 h 後にクローズ
高度な改善が実装された完全版
"""

import os
import asyncio
import logging
import time
import json
import hashlib
import hmac
import requests
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple, Union
from logging.handlers import RotatingFileHandler
import getpass
import math
import random
import websockets
import pandas as pd
import numpy as np

import ccxt.async_support as ccxt
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

from config import (choose_hold_hours, get_stoploss_threshold, 
                   get_check_interval, calculate_position_size, 
                   is_market_safe, evaluate_strategy_performance)
from db import (log_trade, due_trades, mark_closed, log_error, 
               get_open_trades, get_trade_by_symbol, get_soon_expiring_trades,
               get_pnl_stats, log_performance)
from crypto_util import decrypt_and_load_keys, encrypt_sensitive_data
from notification import send_notification

# ── 環境変数 ───────────────────────────
load_dotenv()
DRY_RUN = os.getenv("DRY_RUN", "1") == "1"
MAX_RETRY = int(os.getenv("MAX_RETRY", "3"))
STAKE_PERCENT = float(os.getenv("STAKE_PERCENT", "0.1"))  # 口座の何%を使うか
MAX_STAKE_USDT = float(os.getenv("MAX_STAKE_USDT", "1000"))  # 最大投資額（USDT）
STOP_LOSS_ENABLED = os.getenv("STOP_LOSS_ENABLED", "1") == "1"  # ストップロス有効化
TAKE_PROFIT_ENABLED = os.getenv("TAKE_PROFIT_ENABLED", "0") == "1"  # 利益確定有効化
TAKE_PROFIT_THRESHOLD = float(os.getenv("TAKE_PROFIT_THRESHOLD", "0.1"))  # 10%の利益確定
BASE_CHECK_INTERVAL_MINUTES = int(os.getenv("BASE_CHECK_INTERVAL_MINUTES", "5"))  # 基本チェック間隔（分）
QUICK_CHECK_INTERVAL_MINUTES = int(os.getenv("QUICK_CHECK_INTERVAL_MINUTES", "1"))  # 短縮チェック間隔（分）
TIME_THRESHOLD_HOURS = float(os.getenv("TIME_THRESHOLD_HOURS", "1.0"))  # 決済までの残り時間閾値（時間）
LOG_MAX_BYTES = int(os.getenv("LOG_MAX_BYTES", "5242880"))  # ログファイルの最大サイズ（5MB）
LOG_BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", "3"))  # バックアップファイル数
USE_WEBSOCKET = os.getenv("USE_WEBSOCKET", "0") == "1"  # WebSocketを使用するか
CACHE_TIMEOUT = int(os.getenv("CACHE_TIMEOUT", "300"))  # キャッシュ有効期間（秒）
NOTIFICATION_ENABLED = os.getenv("NOTIFICATION_ENABLED", "0") == "1"  # 通知機能の有効化
MAX_CONCURRENT_SYMBOLS = int(os.getenv("MAX_CONCURRENT_SYMBOLS", "10"))  # 同時購入する最大シンボル数
REQUIRE_MARKET_CHECK = os.getenv("REQUIRE_MARKET_CHECK", "1") == "1"  # 市場安全性チェック

# ── ロギング設定 ───────────────────────
UTC = timezone.utc

# RotatingFileHandlerによるログローテーション設定
log_file_handler = RotatingFileHandler(
    "bot.log", 
    maxBytes=LOG_MAX_BYTES, 
    backupCount=LOG_BACKUP_COUNT,
    encoding='utf-8'
)
log_file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))

logging.basicConfig(
    level=logging.INFO,
    handlers=[log_file_handler, console_handler]
)

logger = logging.getLogger(__name__)

# ── データキャッシュ ─────────────────────
# API呼び出しを減らすためのキャッシュ
cache = {
    "tickers": {"data": None, "timestamp": 0},
    "balances": {"data": None, "timestamp": 0},
    "symbols": {"data": None, "timestamp": 0},
    "prices": {}  # symbol -> {"price": value, "timestamp": time}
}

# ── ccxtクライアント初期化 ────────────────
def initialize_exchange():
    """APIキーを復号してccxtクライアントを初期化"""
    # 暗号化されたAPIキーがある場合は復号
    mexc_key, mexc_secret = decrypt_and_load_keys()
    
    if not mexc_key or not mexc_secret:
        if os.getenv("ENCRYPTED_KEYS") == "1":
            logger.error("APIキーの復号に失敗しました。正しいパスワードを入力してください。")
            return None
        else:
            logger.error("APIキーが設定されていません。")
            return None
    
    # ccxtクライアントの初期化
    ex = ccxt.mexc({
        "apiKey": mexc_key,
        "secret": mexc_secret,
        "enableRateLimit": True,
        "timeout": 15000,  # ms
    })
    ex.options["fetchCurrencies"] = False  # capital/config/getall タイムアウト回避
    
    return ex

# グローバル変数としてexchangeを保持
exchange = None
websocket_connection = None
websocket_symbols = set()
next_check_task = None

# ── ユーティリティ関数 ───────────────────
async def retry_async(func, *args, retries=MAX_RETRY, **kwargs):
    """エラー発生時に指数バックオフで再試行するラッパー関数"""
    last_error = None
    for attempt in range(retries):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            last_error = e
            # 指数バックオフ: 2^attempt秒（1, 2, 4, 8...）+ ランダム要素
            wait_time = 2 ** attempt + random.uniform(0, 0.5)
            logger.warning(f"試行 {attempt+1}/{retries} 失敗: {e}, {wait_time:.2f}秒後に再試行", exc_info=True)
            await asyncio.sleep(wait_time)
    
    # 全ての再試行が失敗
    logger.error(f"全ての再試行が失敗: {last_error}", exc_info=True)
    log_error(str(last_error))
    
    # 通知送信（設定されている場合）
    if NOTIFICATION_ENABLED:
        send_notification(f"API呼び出し失敗: {func.__name__} - {last_error}")
    
    raise last_error

async def get_cached_tickers():
    """キャッシュされたティッカー情報を取得、必要に応じて更新"""
    now = time.time()
    if cache["tickers"]["data"] is None or (now - cache["tickers"]["timestamp"]) > CACHE_TIMEOUT:
        logger.info("ティッカー情報をキャッシュに読み込み中...")
        cache["tickers"]["data"] = await retry_async(exchange.fetch_tickers)
        cache["tickers"]["timestamp"] = now
    return cache["tickers"]["data"]

async def get_cached_balance():
    """キャッシュされた残高情報を取得、必要に応じて更新"""
    now = time.time()
    if cache["balances"]["data"] is None or (now - cache["balances"]["timestamp"]) > CACHE_TIMEOUT:
        logger.info("残高情報をキャッシュに読み込み中...")
        cache["balances"]["data"] = await retry_async(exchange.fetch_balance)
        cache["balances"]["timestamp"] = now
    return cache["balances"]["data"]

async def get_cached_market_price(symbol: str) -> Optional[float]:
    """キャッシュされた価格情報を取得、必要に応じて更新"""
    now = time.time()
    
    # WebSocketが有効かつ接続済みの場合はキャッシュから直接取得
    if USE_WEBSOCKET and symbol in websocket_symbols and symbol in cache["prices"]:
        price_data = cache["prices"].get(symbol)
        if price_data and (now - price_data["timestamp"]) < 10:  # 10秒以内のデータ
            return price_data["price"]
    
    # それ以外はAPIで取得
    if symbol not in cache["prices"] or (now - cache["prices"].get(symbol, {}).get("timestamp", 0)) > 60:
        try:
            ticker = await retry_async(exchange.fetch_ticker, symbol)
            if "last" in ticker and ticker["last"]:
                cache["prices"][symbol] = {"price": ticker["last"], "timestamp": now}
                return ticker["last"]
            return None
        except Exception as e:
            logger.error(f"{symbol} 価格取得エラー: {e}", exc_info=True)
            return None
    
    return cache["prices"].get(symbol, {}).get("price")

def calculate_dynamic_stoploss(entry_price: float, current_price: float, 
                              hours_held: float) -> float:
    """
    保有時間に基づく動的ストップロス計算
    長く保有するほどストップロスを上げる（利益を守る）
    """
    base_threshold = get_stoploss_threshold()
    
    # デフォルトのストップロス価格
    stoploss_price = entry_price * (1 - base_threshold)
    
    # すでに利益が出ている場合は動的ストップロスを計算
    if current_price > entry_price:
        profit_percent = (current_price / entry_price) - 1
        
        # 保有時間による調整係数（最大0.8まで）
        time_factor = min(hours_held / 24, 0.8)  # 24時間で80%まで
        
        # 利益の確保率: 保有時間が長いほど、より多くの利益を確保
        profit_lock_percent = profit_percent * time_factor
        
        # 新しいストップロス価格（少なくとも元のストップロス以上）
        dynamic_stoploss = entry_price * (1 + profit_lock_percent - (base_threshold * (1 - time_factor)))
        stoploss_price = max(stoploss_price, dynamic_stoploss)
    
    return stoploss_price

# ── WebSocket関連 ─────────────────────
async def initialize_websocket():
    """MEXCのWebSocketに接続し、価格更新をサブスクライブ"""
    if not USE_WEBSOCKET:
        return
    
    global websocket_connection
    
    try:
        # MEXC WebSocketエンドポイント
        ws_url = "wss://wbs.mexc.com/ws"
        websocket_connection = await websockets.connect(ws_url)
        logger.info("WebSocket接続完了")
        
        # サブスクリプション開始
        await websocket_connection.send(json.dumps({
            "method": "SUBSCRIPTION",
            "params": ["spot@public.ticker"]
        }))
        
        # WebSocketハンドラーを別タスクで開始
        asyncio.create_task(handle_websocket_messages())
        
    except Exception as e:
        logger.error(f"WebSocket初期化エラー: {e}", exc_info=True)
        websocket_connection = None

async def handle_websocket_messages():
    """WebSocketから受信したメッセージを処理"""
    global websocket_connection
    
    if not websocket_connection:
        return
    
    try:
        while True:
            message = await websocket_connection.recv()
            try:
                data = json.loads(message)
                
                # ティッカーデータ処理
                if "data" in data and "symbol" in data["data"]:
                    symbol = data["data"]["symbol"]
                    last_price = float(data["data"]["lastPrice"])
                    
                    # キャッシュを更新
                    cache["prices"][symbol] = {
                        "price": last_price,
                        "timestamp": time.time()
                    }
                    
                    websocket_symbols.add(symbol)
                    
            except json.JSONDecodeError:
                logger.warning(f"無効なWebSocketメッセージ: {message}")
            
    except websockets.exceptions.ConnectionClosed:
        logger.warning("WebSocket接続が閉じられました。再接続を試みます...")
        websocket_connection = None
        await asyncio.sleep(5)
        await initialize_websocket()
    
    except Exception as e:
        logger.error(f"WebSocketハンドラーエラー: {e}", exc_info=True)
        websocket_connection = None
        await asyncio.sleep(5)
        await initialize_websocket()

async def subscribe_symbols(symbols):
    """WebSocketに新しいシンボルをサブスクライブ"""
    if not USE_WEBSOCKET or not websocket_connection:
        return
    
    try:
        # 各シンボルをサブスクライブ
        for symbol in symbols:
            await websocket_connection.send(json.dumps({
                "method": "SUBSCRIPTION",
                "params": [f"spot@public.ticker.{symbol}"]
            }))
            websocket_symbols.add(symbol)
            
        logger.info(f"{len(symbols)}個のシンボルをWebSocketにサブスクライブしました")
    
    except Exception as e:
        logger.error(f"WebSocketサブスクリプションエラー: {e}", exc_info=True)

# ── コア関数 ────────────────────────────
async def top10_symbols() -> List[str]:
    """24 h 出来高 Top10（USDT ペア）を返す"""
    try:
        raw = await get_cached_tickers()
        usdt = {k: v for k, v in raw.items() if k.endswith('/USDT')}
        ranked = sorted(usdt.items(),
                        key=lambda kv: kv[1]['quoteVolume'],
                        reverse=True)
        
        # シンボル数を制限
        top_n = min(MAX_CONCURRENT_SYMBOLS, 10)
        result = [sym.replace('/', '') for sym, _ in ranked[:top_n]]
        
        logger.info(f"出来高Top{top_n}: {', '.join(result)}")
        return result
    except Exception as e:
        logger.error(f"出来高Top10取得エラー: {e}", exc_info=True)
        log_error(f"出来高Top10取得エラー: {e}")
        # 通知送信（設定されている場合）
        if NOTIFICATION_ENABLED:
            send_notification(f"出来高Top10取得エラー: {e}")
        return []

async def get_market_price(symbol: str) -> Optional[float]:
    """現在の市場価格を取得"""
    return await get_cached_market_price(symbol)

async def enter_positions() -> None:
    """出来高Top10のシンボルをロングポジションで購入"""
    try:
        # 市場状態をチェック（安全でなければ取引しない）
        if REQUIRE_MARKET_CHECK:
            market_safe = await is_market_safe(exchange)
            if not market_safe:
                logger.warning("市場状態が不安定なため、新規ポジションの開始をスキップします")
                if NOTIFICATION_ENABLED:
                    send_notification("⚠️ 市場状態が不安定なため、新規ポジションを見送りました")
                return
        
        now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        symbols = await top10_symbols()
        
        if not symbols:
            logger.error("シンボルを取得できませんでした。取引を中止します。")
            return

        # WebSocketにシンボルをサブスクライブ
        if USE_WEBSOCKET:
            await subscribe_symbols(symbols)

        if DRY_RUN:
            stake_total = 100.0  # ドライランの場合は仮の値
            logger.info(f"[DRY] 合計ステーク: {stake_total} USDT")
        else:
            bal = await get_cached_balance()
            usdt_balance = bal['USDT']['free']
            
            # 投資上限を設定
            stake_total = calculate_position_size(usdt_balance)
            logger.info(f"USDT残高: {usdt_balance}, 使用予定: {stake_total} ({STAKE_PERCENT*100}%), 上限: {MAX_STAKE_USDT} USDT")
        
        # 各シンボルに均等に分配
        stake_per_symbol = stake_total / len(symbols)
        
        hold_h = choose_hold_hours()
        exit_at = now + timedelta(hours=hold_h)
        
        successful_entries = 0
        purchased_symbols = []
        
        for sym in symbols:
            try:
                price = await get_market_price(sym)
                if not price:
                    logger.warning(f"{sym} の価格を取得できないためスキップします")
                    continue
                
                # 実際の購入数量を計算
                quantity = stake_per_symbol / price
                
                # 最小取引金額と量をチェック（1USDTと0.000001以上）
                if stake_per_symbol < 1.0 or quantity < 0.000001:
                    logger.warning(f"{sym} の取引金額または数量が小さすぎるためスキップします: {stake_per_symbol} USDT, {quantity} 単位")
                    continue
                
                # ストップロス価格を計算
                stoploss_threshold = get_stoploss_threshold()
                stoploss_price = price * (1 - stoploss_threshold)
                
                # 利益確定価格を計算（有効な場合）
                take_profit_price = None
                if TAKE_PROFIT_ENABLED:
                    take_profit_price = price * (1 + TAKE_PROFIT_THRESHOLD)
                
                if DRY_RUN:
                    logger.info(f"[DRY] BUY {sym}: {quantity:.8f} @ {price:.8f} USDT (合計 {stake_per_symbol:.2f} USDT, SL: {stoploss_price:.8f})")
                else:
                    # トレード前の最終価格チェック
                    final_price = await get_market_price(sym)
                    if final_price and abs((final_price - price) / price) > 0.01:  # 1%以上の価格変動
                        logger.warning(f"{sym} の価格が急変しました ({price:.8f} -> {final_price:.8f})。注文を見直します。")
                        price = final_price
                        quantity = stake_per_symbol / price
                    
                    order = await retry_async(
                        exchange.create_order, 
                        sym, 'market', 'buy', 
                        None,  # 数量指定でなく
                        None,  # 価格指定でもなく
                        {'cost': stake_per_symbol}  # 金額指定で購入
                    )
                    logger.info(f"注文成功: {order['id']} - {sym} @ {price:.8f}")
                
                # ストップロス価格も含めてトレードをログに記録
                log_trade(
                    sym=sym, 
                    side='BUY', 
                    qty=quantity, 
                    price=price, 
                    amount=stake_per_symbol, 
                    exit_at=exit_at,
                    stoploss_price=stoploss_price,
                    take_profit_price=take_profit_price
                )
                successful_entries += 1
                purchased_symbols.append(sym)
                
                # レート制限に配慮して少し待機
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.error(f"{sym} 購入エラー: {e}", exc_info=True)
                log_error(f"{sym} 購入エラー: {e}")
        
        logger.info(f"取引完了: {successful_entries}/{len(symbols)}シンボルを購入, 保有時間 {hold_h}h")
        
        # 通知送信（設定されている場合）
        if NOTIFICATION_ENABLED and successful_entries > 0:
            send_notification(f"📈 {successful_entries}銘柄を購入: {', '.join(purchased_symbols[:5])}{' など' if len(purchased_symbols) > 5 else ''}")
    
    except Exception as e:
        logger.error(f"取引プロセス全体エラー: {e}", exc_info=True)
        log_error(f"取引プロセス全体エラー: {e}")
        
        # 通知送信（設定されている場合）
        if NOTIFICATION_ENABLED:
            send_notification(f"❌ 取引プロセスエラー: {e}")

async def exit_due() -> None:
    """期限切れのポジションをクローズ"""
    try:
        now = datetime.now(UTC)
        trades = due_trades(now)
        
        if not trades:
            return
            
        logger.info(f"{len(trades)} 件の期限切れトレードを決済します")
        
        # 通知送信（設定されている場合）
        if NOTIFICATION_ENABLED and trades:
            symbols = [tr.symbol for tr in trades]
            send_notification(f"⏰ {len(trades)}銘柄が期限切れ: {', '.join(symbols[:5])}{' など' if len(symbols) > 5 else ''}")
        
        for tr in trades:
            try:
                current_price = await get_market_price(tr.symbol)
                
                if not current_price:
                    logger.warning(f"{tr.symbol} の現在価格を取得できません。後でリトライします。")
                    continue
                
                if DRY_RUN:
                    logger.info(f"[DRY] SELL {tr.symbol} ({tr.qty}) @ {current_price}")
                    # 損益計算（ドライラン用）
                    if tr.price and current_price:
                        pnl = (current_price - tr.price) * tr.qty
                        pnl_percent = ((current_price / tr.price) - 1) * 100
                        logger.info(f"[DRY] PNL: {pnl:.2f} USDT ({pnl_percent:.2f}%)")
                else:
                    order = await retry_async(
                        exchange.create_order, 
                        tr.symbol, 'market', 'sell', 
                        tr.qty
                    )
                    logger.info(f"決済成功: {order['id']} - {tr.symbol} @ {current_price}")
                    
                    # 損益計算
                    if tr.price and current_price:
                        pnl = (current_price - tr.price) * tr.qty
                        pnl_percent = ((current_price / tr.price) - 1) * 100
                        logger.info(f"PNL: {pnl:.2f} USDT ({pnl_percent:.2f}%)")
                        
                        # 通知送信（設定されている場合）
                        if NOTIFICATION_ENABLED:
                            emoji = "🔴" if pnl < 0 else "🟢"
                            send_notification(f"{emoji} {tr.symbol} 決済: {pnl_percent:.2f}% ({pnl:.2f} USDT)")
                    else:
                        pnl = None
                
                # トレードをクローズ済みとしてマーク
                mark_closed(tr, pnl)
                logger.info(f"クローズ完了: {tr.symbol} (保有期間: {(now - tr.created).total_seconds() / 3600:.1f}h)")
                
                # レート制限に配慮して少し待機
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.error(f"{tr.symbol} 決済エラー: {e}", exc_info=True)
                log_error(f"{tr.symbol} 決済エラー: {e}")
    
    except Exception as e:
        logger.error(f"決済プロセス全体エラー: {e}", exc_info=True)
        log_error(f"決済プロセス全体エラー: {e}")

async def check_stoploss_and_take_profit() -> None:
    """オープンポジションのストップロスと利益確定をチェック"""
    if not STOP_LOSS_ENABLED and not TAKE_PROFIT_ENABLED:
        return
        
    try:
        open_trades = get_open_trades()
        if not open_trades:
            return
        
        now = datetime.now(UTC)
        triggered_symbols = []
            
        for tr in open_trades:
            try:
                current_price = await get_market_price(tr.symbol)
                if not current_price:
                    continue
                
                trigger_reason = None
                
                # ストップロスチェック（有効な場合）
                if STOP_LOSS_ENABLED and hasattr(tr, 'stoploss_price') and tr.stoploss_price:
                    # 保有時間に基づく動的ストップロス計算
                    hours_held = (now - tr.created).total_seconds() / 3600
                    dynamic_stoploss = calculate_dynamic_stoploss(
                        float(tr.price), current_price, hours_held
                    )
                    
                    # 現在価格がストップロス価格を下回っているかチェック
                    if current_price <= dynamic_stoploss:
                        trigger_reason = 'stoploss'
                
                # 利益確定チェック（有効な場合）
                if TAKE_PROFIT_ENABLED and hasattr(tr, 'take_profit_price') and tr.take_profit_price:
                    # 現在価格が利益確定価格を上回っているかチェック
                    if current_price >= tr.take_profit_price:
                        trigger_reason = 'take_profit'
                
                # トリガー理由があれば決済
                if trigger_reason:
                    reason_label = "ストップロス" if trigger_reason == 'stoploss' else "利益確定"
                    logger.warning(f"{reason_label}発動: {tr.symbol} 現在価格 {current_price}")
                    triggered_symbols.append(tr.symbol)
                    
                    if DRY_RUN:
                        logger.info(f"[DRY] {reason_label} SELL {tr.symbol} ({tr.qty}) @ {current_price}")
                        # 損益計算（ドライラン用）
                        if tr.price:
                            pnl = (current_price - tr.price) * tr.qty
                            pnl_percent = ((current_price / tr.price) - 1) * 100
                            logger.info(f"[DRY] PNL: {pnl:.2f} USDT ({pnl_percent:.2f}%)")
                    else:
                        order = await retry_async(
                            exchange.create_order, 
                            tr.symbol, 'market', 'sell', 
                            tr.qty
                        )
                        logger.info(f"{reason_label}決済成功: {order['id']} - {tr.symbol} @ {current_price}")
                        
                        # 損益計算
                        if tr.price:
                            pnl = (current_price - tr.price) * tr.qty
                            pnl_percent = ((current_price / tr.price) - 1) * 100
                            logger.info(f"PNL: {pnl:.2f} USDT ({pnl_percent:.2f}%)")
                            
                            # 通知送信（設定されている場合）
                            if NOTIFICATION_ENABLED:
                                emoji = "🔴" if pnl < 0 else "🟢"
                                send_notification(f"{emoji} {tr.symbol} {reason_label}: {pnl_percent:.2f}% ({pnl:.2f} USDT)")
                        else:
                            pnl = None
                    
                    # トレードをクローズ済みとしてマーク
                    mark_closed(tr, pnl)
                
                # レート制限に配慮して少し待機
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.error(f"{tr.symbol} ストップロス/利益確定チェックエラー: {e}", exc_info=True)
                log_error(f"{tr.symbol} ストップロス/利益確定チェックエラー: {e}")
        
        # 複数のトリガーがあった場合のまとめ通知
        if NOTIFICATION_ENABLED and len(triggered_symbols) > 1:
            send_notification(f"⚠️ {len(triggered_symbols)}銘柄がトリガーされました: {', '.join(triggered_symbols[:5])}{' など' if len(triggered_symbols) > 5 else ''}")
    
    except Exception as e:
        logger.error(f"ストップロス/利益確定チェック全体エラー: {e}", exc_info=True)
        log_error(f"ストップロス/利益確定チェック全体エラー: {e}")

async def schedule_dynamic_checks():
    """動的なチェック間隔でポジションをチェック"""
    global next_check_task
    
    try:
        now = datetime.now(UTC)
        open_trades = get_open_trades()
        
        # エラーがない場合にのみパフォーマンスログを取る
        try:
            # 一日に一度パフォーマンスログを記録（0時頃）
            if now.hour == 0 and 0 <= now.minute < 5:
                stats = get_pnl_stats(30)  # 過去30日の統計
                logger.info(f"30日間のパフォーマンス: {stats['total_trades']}取引, 勝率: {stats['win_rate']*100:.2f}%, 合計損益: {stats['total_pnl']:.2f} USDT")
                # DBにパフォーマンスを記録
                log_performance(stats)
                
                # 通知送信（設定されている場合）
                if NOTIFICATION_ENABLED:
                    win_rate = stats['win_rate']*100
                    total_pnl = stats['total_pnl']
                    emoji = "📊"
                    send_notification(f"{emoji} 30日間のパフォーマンス: {stats['total_trades']}取引, 勝率: {win_rate:.2f}%, 合計: {total_pnl:.2f} USDT")
        except Exception as perf_error:
            logger.error(f"パフォーマンスログ記録エラー: {perf_error}", exc_info=True)
        
        if not open_trades:
            # オープンポジションがない場合、基本間隔でチェック
            next_check_minutes = BASE_CHECK_INTERVAL_MINUTES
            logger.debug("オープンポジションなし - 基本間隔でチェック")
        else:
            # 全てのオープンポジションをチェック
            await check_stoploss_and_take_profit()
            
            # 期限切れのポジションをチェック
            await exit_due()
            
            # 次のチェック時間を決定
            closest_exit = min([trade.exit_at for trade in open_trades])
            time_to_exit = (closest_exit - now).total_seconds() / 3600  # 時間単位
            
            # 決済時間に基づいて動的に間隔を設定
            next_check_minutes = get_check_interval(time_to_exit)
            
            if time_to_exit <= TIME_THRESHOLD_HOURS:
                logger.info(f"最も近い決済まで {time_to_exit:.2f}h - {next_check_minutes:.1f}分間隔でチェック")
        
        # 次のチェックをスケジュール
        next_run_time = now + timedelta(minutes=next_check_minutes)
        
        # 既存のタスクをキャンセル（存在する場合）
        if next_check_task and not next_check_task.done():
            next_check_task.cancel()
        
        # 次のタスクをスケジュール
        next_check_task = asyncio.create_task(scheduled_check(next_run_time))
        
    except Exception as e:
        logger.error(f"動的チェックスケジューリングエラー: {e}", exc_info=True)
        log_error(f"動的チェックスケジューリングエラー: {e}")
        # エラーが発生しても一定時間後に再試行
        await asyncio.sleep(BASE_CHECK_INTERVAL_MINUTES * 60)
        await schedule_dynamic_checks()

async def scheduled_check(run_time):
    """指定時間にチェックを実行"""
    try:
        now = datetime.now(UTC)
        wait_seconds = (run_time - now).total_seconds()
        
        if wait_seconds > 0:
            await asyncio.sleep(wait_seconds)
        
        # チェック実行
        await schedule_dynamic_checks()
        
    except asyncio.CancelledError:
        # タスクがキャンセルされた場合（別のスケジュールに置き換えられた）
        pass
    except Exception as e:
        logger.error(f"スケジュールチェックエラー: {e}", exc_info=True)
        # エラーが発生しても次のチェックをスケジュール
        await schedule_dynamic_checks()

# ── 健全性チェック ──────────────────────
async def health_check() -> None:
    """APIと接続状態を確認"""
    try:
        # 簡単な市場データリクエストで接続チェック
        await exchange.fetch_ticker('BTC/USDT')
        
        # キャッシュエントリをチェック
        old_caches = []
        now = time.time()
        for cache_name, cache_entry in cache.items():
            if isinstance(cache_entry, dict) and "timestamp" in cache_entry:
                age = now - cache_entry["timestamp"]
                if age > CACHE_TIMEOUT * 3:  # タイムアウトの3倍以上古いキャッシュ
                    old_caches.append(f"{cache_name} ({int(age/60)}分)")
        
        # 古いキャッシュがあればログに記録
        if old_caches:
            logger.warning(f"古いキャッシュエントリがあります: {', '.join(old_caches)}")
        
        # WebSocket接続チェック
        if USE_WEBSOCKET and not websocket_connection:
            logger.warning("WebSocket接続が閉じられています。再接続を試みます。")
            await initialize_websocket()
        
        logger.info("健全性チェック: OK")
    except Exception as e:
        logger.error(f"健全性チェック失敗: {e}", exc_info=True)
        log_error(f"健全性チェック失敗: {e}")
        
        # 通知送信（設定されている場合）
        if NOTIFICATION_ENABLED:
            send_notification(f"⚠️ 健全性チェック失敗: {e}")

# ── スケジューラ ────────────────────────
def main() -> None:
    """メインエントリーポイント"""
    global exchange
    
    try:
        # スプラッシュメッセージ
        logger.info("====================================")
        logger.info("MEXC出来高トップ10トレードボット 開始")
        logger.info(f"モード: {'テスト（DRY RUN）' if DRY_RUN else '本番取引'}")
        logger.info(f"投資設定: 口座の{STAKE_PERCENT*100}%（最大{MAX_STAKE_USDT} USDT）")
        logger.info("====================================")
        
        # ccxtクライアントの初期化
        exchange = initialize_exchange()
        if not exchange:
            logger.critical("ccxtクライアントの初期化に失敗しました。終了します。")
            return
        
        # WebSocket初期化（設定されている場合）
        if USE_WEBSOCKET:
            asyncio.get_event_loop().create_task(initialize_websocket())
            
        # メインスケジューラ設定
        sched = AsyncIOScheduler(timezone=UTC)
        
        # 毎日00:01 UTCに新規ポジション
        sched.add_job(enter_positions, 'cron', hour=0, minute=1)
        
        # 1時間ごとに健全性チェック
        sched.add_job(health_check, 'interval', hours=1, next_run_time=datetime.now(UTC) + timedelta(minutes=1))
        
        sched.start()
        logger.info(f"メインスケジューラ開始")
        
        # 動的なポジションチェックを開始
        asyncio.get_event_loop().create_task(schedule_dynamic_checks())
        
        # 通知送信（設定されている場合）
        if NOTIFICATION_ENABLED:
            send_notification("🚀 ボット起動: MEXC出来高トップ10トレーダー")
        
        asyncio.get_event_loop().run_forever()
    
    except (KeyboardInterrupt, SystemExit):
        logger.info("ボット終了中...")
        # 通知送信（設定されている場合）
        if NOTIFICATION_ENABLED:
            send_notification("⛔ ボットが手動で停止されました")
    except Exception as e:
        logger.critical(f"致命的エラー: {e}", exc_info=True)
        log_error(f"致命的エラー: {e}")
        # 通知送信（設定されている場合）
        if NOTIFICATION_ENABLED:
            send_notification(f"❌ 致命的エラー: {e}")
    finally:
        if exchange:
            asyncio.run(exchange.close())  # コネクタリーク防止

# ── エントリーポイント ───────────────────
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"起動失敗: {e}", exc_info=True)
        log_error(f"起動失敗: {e}")
        # 通知送信（設定されている場合）
        if NOTIFICATION_ENABLED:
            send_notification(f"❌ 起動失敗: {e}")
    finally:
        # 非同期ループがすでに閉じている場合に備える
        try:
            if exchange:
                asyncio.run(exchange.close())
        except:
            pass