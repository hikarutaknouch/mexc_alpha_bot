"""
MEXCボット バックテスト機能
過去の市場データを使用して戦略をテスト
"""

import os
import json
import logging
import asyncio
from datetime import datetime, timedelta, timezone
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from tqdm import tqdm
import ccxt
from dotenv import load_dotenv

from config import get_backtest_params, choose_hold_hours, get_stoploss_threshold
from db import log_backtest_result

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    handlers=[
        logging.FileHandler("backtest.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 環境変数のロード
load_dotenv()

class MEXCBacktester:
    def __init__(self, params=None):
        # バックテストパラメータの設定
        self.params = params or get_backtest_params()
        self.start_date = datetime.strptime(self.params["start_date"], "%Y-%m-%d")
        self.end_date = datetime.strptime(self.params["end_date"], "%Y-%m-%d")
        self.initial_capital = self.params["initial_capital"]
        self.stake_percent = self.params["stake_percent"]
        self.stoploss_threshold = self.params["stoploss_threshold"]
        self.enable_stoploss = self.params["enable_stoploss"]
        self.hold_hours = self.params["hold_hours"]
        
        # MEXCクライアント（履歴データ取得用）
        self.exchange = ccxt.mexc({
            'enableRateLimit': True,
            'timeout': 30000,
        })
        
        # バックテスト結果用の変数
        self.capital = self.initial_capital
        self.positions = {}  # シンボル -> ポジション情報
        self.closed_trades = []
        self.trade_history = []  # 時系列での資金推移
        self.daily_balance = {}  # 日次の資金残高
        
        logger.info(f"バックテスト期間: {self.start_date} から {self.end_date}")
        logger.info(f"初期資金: {self.initial_capital} USDT")
        logger.info(f"投資比率: {self.stake_percent * 100}%")
        logger.info(f"ストップロス: {'有効' if self.enable_stoploss else '無効'} ({self.stoploss_threshold * 100}%)")
    
    async def fetch_historical_data(self, symbol, timeframe='1h', limit=1000):
        """指定されたシンボルの履歴データを取得"""
        try:
            # OHLCV (Open, High, Low, Close, Volume) データを取得
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            
            # Pandas DataFrameに変換
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            
            return df
        except Exception as e:
            logger.error(f"{symbol} の履歴データ取得エラー: {e}")
            return None
    
    async def get_top_symbols(self, date):
        """指定日付の出来高Top10（USDT ペア）を取得
        実際のAPIでは過去の出来高ランキングを取得できないため、
        このバックテストでは簡易的にハードコードされたリストか
        ランダムなUSDTペアを使用"""
        
        # 日付ごとに固定のトップシンボルのマッピング（実際の環境ではより複雑なロジックが必要）
        # ここではサンプルデータを使用
        symbols_by_month = {
            "2024-01": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "XRPUSDT", 
                         "BNBUSDT", "ADAUSDT", "AVAXUSDT", "DOTUSDT", "LTCUSDT"],
            "2024-02": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "XRPUSDT", 
                         "BNBUSDT", "LINKUSDT", "AVAXUSDT", "MATICUSDT", "UNIUSDT"],
            "2024-03": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "XRPUSDT", 
                         "BNBUSDT", "LINKUSDT", "AVAXUSDT", "MATICUSDT", "ARBUSDT"],
            "2024-04": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "XRPUSDT", 
                         "BNBUSDT", "LINKUSDT", "AVAXUSDT", "SHIB", "PEPEUSDT"],
        }
        
        # 日付を年-月の形式に変換
        month_key = date.strftime("%Y-%m")
        
        # マッピングにある場合はそれを返す、なければデフォルトリスト
        if month_key in symbols_by_month:
            return symbols_by_month[month_key]
        else:
            # デフォルトリスト
            return ["BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "XRPUSDT", 
                    "BNBUSDT", "ADAUSDT", "AVAXUSDT", "DOTUSDT", "LTCUSDT"]
    
    async def run_backtest(self):
        """バックテストを実行"""
        logger.info("バックテスト開始...")
        
        # 全データキャッシュ
        data_cache = {}
        
        # シミュレーション日付のループ
        current_date = self.start_date
        portfolio_values = []  # ポートフォリオ価値の履歴
        dates = []  # 日付の履歴
        
        # 進捗バーの設定
        total_days = (self.end_date - self.start_date).days
        
        for _ in tqdm(range(total_days), desc="バックテスト進行中"):
            # その日は取引を行うか？(UTCで00:01に新規ポジション)
            if current_date.hour == 0 and current_date.minute == 1:
                await self.enter_positions(current_date, data_cache)
            
            # オープンポジションのチェック（ストップロス、期限切れ）
            await self.check_positions(current_date, data_cache)
            
            # 資金推移の記録
            portfolio_value = await self.calculate_portfolio_value(current_date, data_cache)
            portfolio_values.append(portfolio_value)
            dates.append(current_date)
            
            # 日次残高の記録
            day_key = current_date.strftime("%Y-%m-%d")
            self.daily_balance[day_key] = portfolio_value
            
            # 時間を進める（1時間ごと）
            current_date += timedelta(hours=1)
            
            # 終了日を超えたら終了
            if current_date > self.end_date:
                break
        
        # 最終ポートフォリオ価値
        final_value = portfolio_values[-1] if portfolio_values else self.initial_capital
        
        # 結果の分析と保存
        results = self.analyze_results(portfolio_values, dates)
        
        return results
    
    async def enter_positions(self, date, data_cache):
        """新規ポジションのエントリー"""
        # トップシンボルを取得
        symbols = await self.get_top_symbols(date)
        
        # 投資可能な資金を計算
        available_capital = self.capital * self.stake_percent
        stake_per_symbol = available_capital / len(symbols)
        
        logger.debug(f"{date}: 新規ポジションエントリー - 利用可能資金: {available_capital:.2f} USDT")
        
        for symbol in symbols:
            # データを取得
            df = await self.get_market_data(symbol, date, data_cache)
            if df is None or df.empty:
                continue
            
            # 現在の価格を取得
            try:
                price_row = df.iloc[df.index.get_indexer([date], method='nearest')[0]]
                price = price_row['close']
            except (IndexError, KeyError):
                logger.warning(f"{date}: {symbol} の価格データが見つかりません")
                continue
            
            # 数量を計算
            quantity = stake_per_symbol / price
            
            # ストップロス価格を計算
            stoploss_price = price * (1 - self.stoploss_threshold)
            
            # ランダムな保有時間を選択
            hold_hours = np.random.choice(self.hold_hours)
            exit_time = date + timedelta(hours=hold_hours)
            
            # ポジションを記録
            self.positions[symbol] = {
                'entry_time': date,
                'exit_time': exit_time,
                'entry_price': price,
                'quantity': quantity,
                'stoploss_price': stoploss_price,
                'stake': stake_per_symbol
            }
            
            # 資金を減らす
            self.capital -= stake_per_symbol
            
            logger.debug(f"{date}: {symbol} を {price:.4f} で {quantity:.6f} 購入、計 {stake_per_symbol:.2f} USDT")
        
    async def check_positions(self, date, data_cache):
        """オープンポジションをチェック（ストップロス、期限切れ）"""
        symbols_to_remove = []
        
        for symbol, position in self.positions.items():
            # 現在の価格データを取得
            df = await self.get_market_data(symbol, date, data_cache)
            if df is None or df.empty:
                continue
            
            try:
                # 最も近い時間のデータを取得
                price_row = df.iloc[df.index.get_indexer([date], method='nearest')[0]]
                current_price = price_row['close']
                
                # ストップロスをチェック
                if self.enable_stoploss and current_price <= position['stoploss_price']:
                    # ストップロスによる決済
                    pnl = (current_price - position['entry_price']) * position['quantity']
                    pnl_percent = ((current_price / position['entry_price']) - 1) * 100
                    
                    logger.debug(f"{date}: {symbol} ストップロス発動 - 価格: {current_price:.4f} <= {position['stoploss_price']:.4f}")
                    logger.debug(f"{date}: {symbol} 損益: {pnl:.2f} USDT ({pnl_percent:.2f}%)")
                    
                    # 決済記録
                    self.closed_trades.append({
                        'symbol': symbol,
                        'entry_time': position['entry_time'],
                        'exit_time': date,
                        'entry_price': position['entry_price'],
                        'exit_price': current_price,
                        'quantity': position['quantity'],
                        'pnl': pnl,
                        'pnl_percent': pnl_percent,
                        'exit_reason': 'stoploss'
                    })
                    
                    # 資金を更新
                    returned_capital = position['stake'] + pnl
                    self.capital += returned_capital
                    
                    # 削除リストに追加
                    symbols_to_remove.append(symbol)
                    
                # 期限切れチェック
                elif date >= position['exit_time']:
                    # 期限切れによる決済
                    pnl = (current_price - position['entry_price']) * position['quantity']
                    pnl_percent = ((current_price / position['entry_price']) - 1) * 100
                    
                    logger.debug(f"{date}: {symbol} 期限切れによる決済 - 保有期間: {(date - position['entry_time']).total_seconds() / 3600:.1f}h")
                    logger.debug(f"{date}: {symbol} 損益: {pnl:.2f} USDT ({pnl_percent:.2f}%)")
                    
                    # 決済記録
                    self.closed_trades.append({
                        'symbol': symbol,
                        'entry_time': position['entry_time'],
                        'exit_time': date,
                        'entry_price': position['entry_price'],
                        'exit_price': current_price,
                        'quantity': position['quantity'],
                        'pnl': pnl,
                        'pnl_percent': pnl_percent,
                        'exit_reason': 'timeexpiry'
                    })
                    
                    # 資金を更新
                    returned_capital = position['stake'] + pnl
                    self.capital += returned_capital
                    
                    # 削除リストに追加
                    symbols_to_remove.append(symbol)
                
            except (IndexError, KeyError) as e:
                logger.warning(f"{date}: {symbol} の価格データ取得エラー: {e}")
        
        # 決済したポジションを削除
        for symbol in symbols_to_remove:
            del self.positions[symbol]
    
    async def get_market_data(self, symbol, date, data_cache):
        """市場データを取得（キャッシュを使用）"""
        # 既にキャッシュにあるかチェック
        if symbol in data_cache:
            return data_cache[symbol]
        
        # なければデータを取得
        df = await self.fetch_historical_data(symbol)
        if df is not None:
            data_cache[symbol] = df
        
        return df
    
    async def calculate_portfolio_value(self, date, data_cache):
        """現在のポートフォリオ価値を計算"""
        # 現金
        total_value = self.capital
        
        # オープンポジションの価値
        for symbol, position in self.positions.items():
            df = await self.get_market_data(symbol, date, data_cache)
            if df is None or df.empty:
                # データがなければ購入時の価値で計算
                total_value += position['stake']
                continue
            
            try:
                # 最も近い時間のデータを取得
                price_row = df.iloc[df.index.get_indexer([date], method='nearest')[0]]
                current_price = price_row['close']
                
                # ポジション価値を計算
                position_value = current_price * position['quantity']
                total_value += position_value
            except (IndexError, KeyError):
                # エラーの場合は購入時の価値で計算
                total_value += position['stake']
        
        return total_value
    
    def analyze_results(self, portfolio_values, dates):
        """バックテスト結果を分析"""
        if not portfolio_values:
            logger.error("バックテスト結果がありません")
            return None
        
        # 最終資金
        final_capital = portfolio_values[-1]
        
        # 総利益と利益率
        profit = final_capital - self.initial_capital
        profit_percent = (profit / self.initial_capital) * 100
        
        # 最大ドローダウンを計算
        max_value = self.initial_capital
        max_drawdown = 0
        for value in portfolio_values:
            max_value = max(max_value, value)
            drawdown = (max_value - value) / max_value * 100
            max_drawdown = max(max_drawdown, drawdown)
        
        # トレード統計
        trade_count = len(self.closed_trades)
        win_count = sum(1 for trade in self.closed_trades if trade['pnl'] > 0)
        loss_count = trade_count - win_count
        win_rate = win_count / trade_count if trade_count > 0 else 0
        
        # 平均利益・損失
        if win_count > 0:
            avg_profit = sum(trade['pnl'] for trade in self.closed_trades if trade['pnl'] > 0) / win_count
        else:
            avg_profit = 0
            
        if loss_count > 0:
            avg_loss = sum(trade['pnl'] for trade in self.closed_trades if trade['pnl'] <= 0) / loss_count
        else:
            avg_loss = 0
        
        # 平均保有時間
        hold_times = [(trade['exit_time'] - trade['entry_time']).total_seconds() / 3600 
                      for trade in self.closed_trades]
        avg_hold_time = sum(hold_times) / len(hold_times) if hold_times else 0
        
        # 結果を辞書に格納
        results = {
            'start_date': self.start_date,
            'end_date': self.end_date,
            'initial_capital': self.initial_capital,
            'final_capital': final_capital,
            'profit': profit,
            'profit_percent': profit_percent,
            'max_drawdown': max_drawdown,
            'max_drawdown_percent': max_drawdown,
            'trade_count': trade_count,
            'win_count': win_count,
            'loss_count': loss_count,
            'win_rate': win_rate,
            'avg_profit': avg_profit,
            'avg_loss': avg_loss,
            'avg_hold_time': avg_hold_time,
            'params': json.dumps(self.params),
            'portfolio_history': list(zip([d.strftime("%Y-%m-%d %H:%M") for d in dates], portfolio_values)),
            'trades': self.closed_trades
        }
        
        # 結果をログに記録
        logger.info(f"バックテスト完了: 初期資金 {self.initial_capital} -> 最終資金 {final_capital:.2f} USDT")
        logger.info(f"利益: {profit:.2f} USDT ({profit_percent:.2f}%)")
        logger.info(f"最大ドローダウン: {max_drawdown:.2f}%")
        logger.info(f"トレード数: {trade_count}, 勝率: {win_rate*100:.2f}%")
        logger.info(f"平均利益: {avg_profit:.2f} USDT, 平均損失: {avg_loss:.2f} USDT")
        logger.info(f"平均保有時間: {avg_hold_time:.2f}h")
        
        # 結果をDBに保存
        db_results = {k: v for k, v in results.items() if k not in ['portfolio_history', 'trades']}
        log_backtest_result(db_results)
        
        # 結果のグラフを生成
        self.plot_results(dates, portfolio_values)
        
        return results
    
    def plot_results(self, dates, portfolio_values):
        """バックテスト結果をグラフ化"""
        plt.figure(figsize=(12, 6))
        plt.plot(dates, portfolio_values)
        plt.title('バックテスト結果: ポートフォリオ価値の推移')
        plt.xlabel('日付')
        plt.ylabel('ポートフォリオ価値 (USDT)')
        plt.grid(True)
        plt.tight_layout()
        plt.savefig('backtest_results.png')
        logger.info("結果グラフを 'backtest_results.png' に保存しました")

async def main():
    """メイン関数"""
    # パラメータを設定
    params = get_backtest_params()
    
    # バックテスターの初期化と実行
    backtester = MEXCBacktester(params)
    results = await backtester.run_backtest()
    
    # 結果の表示
    if results:
        print("\n===== バックテスト結果 =====")
        print(f"期間: {results['start_date']} から {results['end_date']}")
        print(f"初期資金: {results['initial_capital']} USDT")
        print(f"最終資金: {results['final_capital']:.2f} USDT")
        print(f"利益: {results['profit']:.2f} USDT ({results['profit_percent']:.2f}%)")
        print(f"最大ドローダウン: {results['max_drawdown']:.2f}%")
        print(f"トレード数: {results['trade_count']}")
        print(f"勝率: {results['win_rate']*100:.2f}%")
        print(f"平均利益: {results['avg_profit']:.2f} USDT")
        print(f"平均損失: {results['avg_loss']:.2f} USDT")
        print(f"平均保有時間: {results['avg_hold_time']:.2f}h")
        print("===========================")
        
        print("\nグラフを 'backtest_results.png' に保存しました")

if __name__ == "__main__":
    asyncio.run(main())