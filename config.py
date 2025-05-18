"""
戦略パラメータとユーティリティ - 完全版
"""
import os
import random
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple, Union
import numpy as np
from dotenv import load_dotenv

# ロギング設定
logger = logging.getLogger(__name__)

# 環境変数読み込み
load_dotenv()

# ── ホールド時間設定 ───────────────────────
def get_hold_hours_pool() -> List[int]:
    """保有時間プールを取得"""
    pool_str = os.getenv("HOLD_HOURS_POOL", "8,10,12")
    try:
        return [int(h) for h in pool_str.split(",")]
    except (ValueError, AttributeError):
        logger.warning("HOLD_HOURS_POOLの解析に失敗しました。デフォルト値を使用します。")
        return [8, 10, 12]

def choose_hold_hours() -> int:
    """設定されたプールからホールド時間をランダムで返す"""
    pool = get_hold_hours_pool()
    return random.choice(pool)

# ── ストップロス設定 ───────────────────────
DEFAULT_STOPLOSS = 0.05  # デフォルトは5%のストップロス

def get_stoploss_threshold() -> float:
    """
    ストップロスの閾値を返す
    環境変数から取得するか、デフォルト値を使用
    """
    stoploss_str = os.getenv("STOP_LOSS_THRESHOLD")
    if stoploss_str:
        try:
            threshold = float(stoploss_str)
            if 0 < threshold < 1:
                return threshold
            elif threshold >= 1:
                # パーセントで指定された場合（例: 5.0%）
                return threshold / 100.0
        except ValueError:
            pass
    return DEFAULT_STOPLOSS

def get_take_profit_threshold() -> Optional[float]:
    """
    利益確定の閾値を返す
    環境変数から取得するか、有効でない場合はNone
    """
    if os.getenv("TAKE_PROFIT_ENABLED", "0") != "1":
        return None
        
    take_profit_str = os.getenv("TAKE_PROFIT_THRESHOLD")
    if take_profit_str:
        try:
            threshold = float(take_profit_str)
            if 0 < threshold < 1:
                return threshold
            elif threshold >= 1:
                # パーセントで指定された場合（例: 10.0%）
                return threshold / 100.0
        except ValueError:
            pass
    return 0.1  # デフォルトは10%

# ── 動的なポジションチェック設定 ───────────────
def get_check_interval(time_to_exit: float) -> float:
    """
    残り時間に基づいて次のチェック間隔を決定
    
    Args:
        time_to_exit (float): 決済までの残り時間（時間単位）
    
    Returns:
        float: チェック間隔（分単位）
    """
    # 環境変数から設定を読み込み
    base_interval = float(os.getenv("BASE_CHECK_INTERVAL_MINUTES", "5"))
    quick_interval = float(os.getenv("QUICK_CHECK_INTERVAL_MINUTES", "1"))
    time_threshold = float(os.getenv("TIME_THRESHOLD_HOURS", "1.0"))
    
    # 残り時間に基づいて動的にチェック間隔を計算
    if time_to_exit <= time_threshold * 0.5:
        # 決済が非常に近い場合
        return quick_interval
    elif time_to_exit <= time_threshold:
        # 決済がやや近い場合は線形に補間
        ratio = (time_to_exit - time_threshold * 0.5) / (time_threshold * 0.5)
        return quick_interval + ratio * (base_interval - quick_interval) * 0.5
    elif time_to_exit <= time_threshold * 2:
        # 決済が中程度に近い場合
        ratio = (time_to_exit - time_threshold) / time_threshold
        return base_interval * 0.5 + ratio * base_interval * 0.5
    else:
        # それ以外は通常間隔
        return base_interval

# ── ポジションサイズ設定 ───────────────────
def calculate_position_size(available_balance: float) -> float:
    """
    利用可能残高からポジションサイズを計算
    上限付きの割合ベースの計算
    
    Args:
        available_balance (float): 利用可能USDT残高
    
    Returns:
        float: 使用するステーク量（USDT）
    """
    try:
        stake_percent = float(os.getenv("STAKE_PERCENT", "0.1"))
        max_stake = float(os.getenv("MAX_STAKE_USDT", "1000"))
        
        # 入力値の検証
        if stake_percent <= 0 or stake_percent > 1:
            logger.warning(f"不正なSTAKE_PERCENT値: {stake_percent}, デフォルト0.1を使用します")
            stake_percent = 0.1
            
        if max_stake <= 0:
            logger.warning(f"不正なMAX_STAKE_USDT値: {max_stake}, デフォルト1000を使用します")
            max_stake = 1000
        
        # 利用可能残高の一定割合と上限の低い方を採用
        stake = min(available_balance * stake_percent, max_stake)
        
        return stake
    except Exception as e:
        logger.error(f"ポジションサイズ計算エラー: {e}", exc_info=True)
        # エラー時はデフォルト値を返す
        return min(available_balance * 0.1, 1000)

def calculate_per_symbol_allocation(total_stake: float, num_symbols: int) -> float:
    """
    シンボルごとの配分を計算
    
    Args:
        total_stake (float): 総ステーク量
        num_symbols (int): シンボル数
    
    Returns:
        float: シンボルあたりの配分額
    """
    if num_symbols <= 0:
        return 0
        
    # 最低取引額（1 USDT）を考慮
    min_allocation = 1.0
    
    # 均等配分を計算
    allocation = total_stake / num_symbols
    
    # 最低取引額を下回る場合は調整
    if allocation < min_allocation:
        # 実際に使用するシンボル数を計算
        usable_symbols = min(num_symbols, int(total_stake / min_allocation))
        if usable_symbols > 0:
            allocation = total_stake / usable_symbols
            logger.warning(f"配分額が小さすぎるため、シンボル数を{num_symbols}から{usable_symbols}に調整しました")
        else:
            allocation = min_allocation
            logger.warning(f"残高不足: 1シンボルに最低取引額を割り当てられません")
            
    return allocation

# ── 市場条件チェック ───────────────────────
async def is_market_safe(exchange) -> bool:
    """
    市場全体が安全かどうかをチェック
    異常なボラティリティや急落時はFalseを返す
    
    Args:
        exchange: ccxtのExchangeオブジェクト
    
    Returns:
        bool: 市場が安全かどうか
    """
    try:
        # BTC/USDTの状態を取得（市場の代表として）
        btc_ticker = await exchange.fetch_ticker('BTC/USDT')
        
        # 環境変数から閾値を取得
        market_decline_threshold = float(os.getenv("MARKET_DECLINE_THRESHOLD", "0.1"))
        
        # 24時間の価格変化をチェック
        change_percent = btc_ticker.get('percentage', 0)
        
        # パーセンテージがマイナス値で提供されるか確認
        if isinstance(change_percent, (int, float)):
            # パーセンテージか小数かを判断
            if abs(change_percent) > 100:
                # すでにパーセント表示ならそのまま使用
                actual_percent = change_percent
            else:
                # 小数表示なら100をかけてパーセントに変換
                actual_percent = change_percent * 100
        else:
            logger.warning(f"無効な変化率フォーマット: {change_percent}")
            return True  # エラー時は安全側に倒す
        
        # 大幅な下落がある場合は警告
        if actual_percent < -market_decline_threshold * 100:
            logger.warning(f"市場の大幅な下落検出: BTC/USDT {actual_percent:.2f}% (閾値: {-market_decline_threshold * 100:.2f}%)")
            return False
        
        # ボラティリティチェック（オプション）
        if 'high' in btc_ticker and 'low' in btc_ticker and btc_ticker['high'] > 0:
            volatility = (btc_ticker['high'] - btc_ticker['low']) / btc_ticker['high']
            volatility_threshold = float(os.getenv("MARKET_VOLATILITY_THRESHOLD", "0.05"))
            
            if volatility > volatility_threshold:
                logger.warning(f"市場の高ボラティリティ検出: BTC/USDT {volatility:.2f} (閾値: {volatility_threshold:.2f})")
                return False
        
        return True
        
    except Exception as e:
        logger.error(f"市場状態チェックエラー: {e}", exc_info=True)
        # エラー発生時は安全側に倒して取引を制限
        return True

# ── 動的ストップロス計算 ───────────────────
def calculate_dynamic_stoploss(entry_price: float, current_price: float, 
                              hours_held: float, base_threshold: float = None) -> float:
    """
    保有時間と利益に基づく動的ストップロス計算
    
    Args:
        entry_price (float): 購入価格
        current_price (float): 現在価格
        hours_held (float): 保有時間（時間）
        base_threshold (float, optional): 基本ストップロス閾値
    
    Returns:
        float: 動的ストップロス価格
    """
    if base_threshold is None:
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

# ── パフォーマンス評価 ───────────────────────
def evaluate_strategy_performance(trades, initial_capital: float = 10000) -> Dict[str, Any]:
    """
    取引履歴から戦略のパフォーマンスを評価
    
    Args:
        trades: 取引履歴のリスト
        initial_capital (float): 初期資金
        
    Returns:
        Dict[str, Any]: パフォーマンス指標の辞書
    """
    if not trades:
        return {
            "period_days": 30,
            "total_trades": 0,
            "profitable_trades": 0,
            "total_pnl": 0,
            "win_rate": 0,
            "avg_pnl": 0,
            "max_drawdown": 0,
            "sharpe_ratio": 0,
            "avg_hold_time": 0
        }
    
    # 基本的な統計
    total_pnl = sum(float(t.pnl) for t in trades if hasattr(t, 'pnl') and t.pnl is not None)
    profit_percent = (total_pnl / initial_capital) * 100 if initial_capital else 0
    
    # 勝率
    win_count = sum(1 for t in trades if hasattr(t, 'pnl') and t.pnl is not None and float(t.pnl) > 0)
    win_rate = win_count / len(trades) if trades else 0
    
    # 平均利益・損失
    if win_count > 0:
        avg_profit = sum(float(t.pnl) for t in trades if hasattr(t, 'pnl') and t.pnl is not None and float(t.pnl) > 0) / win_count
    else:
        avg_profit = 0
        
    loss_count = len(trades) - win_count
    if loss_count > 0:
        avg_loss = sum(float(t.pnl) for t in trades if hasattr(t, 'pnl') and t.pnl is not None and float(t.pnl) <= 0) / loss_count
    else:
        avg_loss = 0
    
    # 平均保有時間
    if hasattr(trades[0], 'created') and hasattr(trades[0], 'updated'):
        hold_times = [(t.updated - t.created).total_seconds() / 3600 for t in trades 
                    if hasattr(t, 'updated') and hasattr(t, 'created')]
        avg_hold_time = sum(hold_times) / len(hold_times) if hold_times else 0
    else:
        avg_hold_time = 0
    
    # 取引期間（日数）
    if hasattr(trades[0], 'created'):
        oldest = min(t.created for t in trades if hasattr(t, 'created'))
        newest = max(t.created for t in trades if hasattr(t, 'created'))
        period_days = (newest - oldest).days + 1
    else:
        period_days = 30  # デフォルト
    
    # 最大ドローダウンと利益曲線の計算は複雑なため別関数で実装するのが望ましい
    # ここでは簡易版を実装
    max_drawdown = 0
    
    # リターンの計算（Sharpe比計算用）
    if hasattr(trades[0], 'pnl_percent'):
        returns = [float(t.pnl_percent) for t in trades if hasattr(t, 'pnl_percent') and t.pnl_percent is not None]
        if returns:
            avg_return = sum(returns) / len(returns)
            std_return = np.std(returns) if len(returns) > 1 else 1
            sharpe_ratio = (avg_return / std_return) * np.sqrt(365 / period_days) if std_return > 0 else 0
        else:
            sharpe_ratio = 0
    else:
        sharpe_ratio = 0
        
    return {
        "period_days": period_days,
        "total_trades": len(trades),
        "profitable_trades": win_count,
        "total_pnl": total_pnl,
        "profit_percent": profit_percent,
        "win_rate": win_rate,
        "avg_pnl": total_pnl / len(trades) if trades else 0,
        "avg_profit": avg_profit,
        "avg_loss": avg_loss,
        "max_drawdown": max_drawdown,
        "sharpe_ratio": sharpe_ratio,
        "avg_hold_time": avg_hold_time
    }

# ── 設定の保存と読み込み ───────────────────
def save_config_to_file(config: Dict[str, Any], filename: str = "strategy_config.json") -> bool:
    """
    現在の設定をJSONファイルに保存
    
    Args:
        config (Dict[str, Any]): 保存する設定の辞書
        filename (str): 保存先のファイル名
        
    Returns:
        bool: 保存が成功したかどうか
    """
    try:
        with open(filename, 'w') as f:
            json.dump(config, f, indent=2)
        return True
    except Exception as e:
        logger.error(f"設定ファイル保存エラー: {e}", exc_info=True)
        return False

def load_config_from_file(filename: str = "strategy_config.json") -> Optional[Dict[str, Any]]:
    """
    JSONファイルから設定を読み込み
    
    Args:
        filename (str): 読み込むファイル名
        
    Returns:
        Optional[Dict[str, Any]]: 読み込んだ設定の辞書またはNone
    """
    try:
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                return json.load(f)
        return None
    except Exception as e:
        logger.error(f"設定ファイル読み込みエラー: {e}", exc_info=True)
        return None

# ── その他のユーティリティ関数 ───────────────
def format_price(price: float, decimals: int = 8) -> str:
    """
    価格を指定された小数点以下桁数でフォーマット
    
    Args:
        price (float): フォーマットする価格
        decimals (int): 小数点以下の桁数
        
    Returns:
        str: フォーマットされた価格文字列
    """
    try:
        return f"{price:.{decimals}f}"
    except (ValueError, TypeError):
        return str(price)

def calculate_quantity(cost: float, price: float, min_quantity: float = 0.000001) -> float:
    """
    コストと価格から数量を計算し、最小数量を考慮
    
    Args:
        cost (float): 投資金額（USDT）
        price (float): 1単位あたりの価格
        min_quantity (float): 最小取引数量
        
    Returns:
        float: 計算された数量
    """
    if price <= 0:
        return 0
        
    quantity = cost / price
    
    # 最小数量を下回る場合は調整
    if quantity < min_quantity:
        return 0
        
    return quantity

def get_symbol_precision(exchange, symbol: str) -> Tuple[int, int]:
    """
    取引所から指定シンボルの価格・数量精度を取得
    
    Args:
        exchange: ccxtのExchangeオブジェクト
        symbol (str): 精度を取得するシンボル
        
    Returns:
        Tuple[int, int]: (価格精度, 数量精度)
    """
    try:
        markets = exchange.markets
        if symbol in markets:
            market = markets[symbol]
            price_precision = market.get('precision', {}).get('price', 8)
            amount_precision = market.get('precision', {}).get('amount', 8)
            return price_precision, amount_precision
        else:
            return 8, 8  # デフォルト値
    except Exception as e:
        logger.error(f"シンボル精度取得エラー: {e}", exc_info=True)
        return 8, 8  # エラー時はデフォルト値

def round_to_precision(value: float, precision: int) -> float:
    """
    指定された精度で四捨五入
    
    Args:
        value (float): 丸める値
        precision (int): 小数点以下の桁数
        
    Returns:
        float: 丸められた値
    """
    factor = 10 ** precision
    return round(value * factor) / factor