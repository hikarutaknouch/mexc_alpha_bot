"""
超シンプルなトレードログ ORM（完全改善版）
浮動小数点精度向上、インデックス最適化、パフォーマンスログ
"""
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text, Numeric, Index, Boolean, ForeignKey, inspect
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

engine = create_engine("sqlite:///trades.sqlite", echo=False, future=True)
Session = sessionmaker(bind=engine)
Base = declarative_base()

class Trade(Base):
    __tablename__ = "trades"
    id      = Column(Integer, primary_key=True)
    symbol  = Column(String, index=True)  # インデックス追加
    side    = Column(String)           # BUY / SELL
    qty     = Column(Numeric(18, 8))   # 数量（精度向上）
    price   = Column(Numeric(18, 8))   # 実行価格（精度向上）
    amount  = Column(Numeric(18, 8))   # 取引総額（USDT）（精度向上）
    exit_at = Column(DateTime, index=True)  # 決済予定時刻（インデックス追加）
    closed  = Column(Integer, default=0, index=True)  # 0=オープン, 1=クローズ済（インデックス追加）
    pnl     = Column(Numeric(18, 8), default=0.0)  # 損益（精度向上）
    pnl_percent = Column(Numeric(10, 4))  # 損益率（%）
    stoploss_price = Column(Numeric(18, 8))  # ストップロス価格（精度向上）
    take_profit_price = Column(Numeric(18, 8))  # 利益確定価格
    close_reason = Column(String)      # 決済理由: time_expiry, stoploss, take_profit, manual
    created = Column(DateTime, default=datetime.utcnow)  # 作成時刻
    updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)  # 更新時刻

    # 複合インデックスの追加
    __table_args__ = (
        Index('idx_exit_at_closed', 'exit_at', 'closed'),
        Index('idx_symbol_closed', 'symbol', 'closed'),
    )

class ErrorLog(Base):
    __tablename__ = "errors"
    id      = Column(Integer, primary_key=True)
    message = Column(Text)
    severity = Column(String, default="error")  # error, warning, info
    resolved = Column(Boolean, default=False)   # 解決済みフラグ
    created = Column(DateTime, default=datetime.utcnow, index=True)  # インデックス追加

class BacktestResult(Base):
    __tablename__ = "backtest_results"
    id      = Column(Integer, primary_key=True)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    initial_capital = Column(Numeric(18, 8))  # 精度向上
    final_capital = Column(Numeric(18, 8))    # 精度向上
    profit = Column(Numeric(18, 8))           # 精度向上
    profit_percent = Column(Numeric(10, 4))   # パーセント値
    max_drawdown = Column(Numeric(10, 4))     # パーセント値
    max_drawdown_percent = Column(Numeric(10, 4))  # パーセント値
    trade_count = Column(Integer)
    win_count = Column(Integer)
    loss_count = Column(Integer)
    win_rate = Column(Numeric(10, 4))         # パーセント値
    avg_profit = Column(Numeric(18, 8))       # 精度向上
    avg_loss = Column(Numeric(18, 8))         # 精度向上
    avg_hold_time = Column(Numeric(10, 2))    # 平均保有時間（時間）
    params = Column(Text)                      # JSON形式のパラメータ
    created = Column(DateTime, default=datetime.utcnow, index=True)  # インデックス追加

class PerformanceLog(Base):
    __tablename__ = "performance_logs"
    id = Column(Integer, primary_key=True)
    date = Column(DateTime, default=datetime.utcnow, index=True)
    period_days = Column(Integer)  # 統計期間（日数）
    total_trades = Column(Integer)
    profitable_trades = Column(Integer)
    total_pnl = Column(Numeric(18, 8))
    win_rate = Column(Numeric(10, 4))
    avg_pnl = Column(Numeric(18, 8))
    current_balance = Column(Numeric(18, 8))
    open_positions = Column(Integer)
    notes = Column(Text)

class APIStat(Base):
    __tablename__ = "api_stats"
    id = Column(Integer, primary_key=True)
    date = Column(DateTime, default=datetime.utcnow, index=True)
    endpoint = Column(String, index=True)
    method = Column(String)
    success = Column(Boolean)
    response_time = Column(Float)  # ミリ秒
    error_message = Column(Text)

class WebSocketStat(Base):
    __tablename__ = "websocket_stats"
    id = Column(Integer, primary_key=True)
    date = Column(DateTime, default=datetime.utcnow, index=True)
    event_type = Column(String)  # connect, disconnect, message, error
    duration = Column(Float)     # 接続維持時間（秒）
    message_count = Column(Integer)
    symbols = Column(Text)       # JSON形式のシンボルリスト
    error_message = Column(Text)

# 既存のテーブルが存在する場合のマイグレーション処理
def migrate_database():
    """データベーススキーマの変更を処理"""
    try:
        # データベース接続を取得
        connection = engine.connect()
        
        # 既存のカラムをチェック
        inspector = inspect(engine)
        columns = {col['name'] for col in inspector.get_columns('trades')} if inspector.has_table('trades') else set()
        
        # テーブルが存在しなければ何もしない
        if not inspector.has_table('trades'):
            connection.close()
            return
        
        # 新しいカラムを追加
        for column_name, data_type in [
            ('pnl_percent', 'NUMERIC(10, 4)'),
            ('take_profit_price', 'NUMERIC(18, 8)'),
            ('close_reason', 'VARCHAR')
        ]:
            if column_name not in columns:
                connection.execute(f'ALTER TABLE trades ADD COLUMN {column_name} {data_type}')
                print(f"Added column {column_name} to trades table")
        
        connection.close()
        print("Database migration completed successfully")
        
    except Exception as e:
        print(f"Database migration error: {e}")

# テーブル作成
try:
    Base.metadata.create_all(engine)
    # マイグレーション処理を実行
    migrate_database()
except Exception as e:
    print(f"Database initialization error: {e}")

def log_trade(sym: str, side: str, qty: float, price: float = None, amount: float = None, 
              exit_at=None, stoploss_price: float = None, take_profit_price: float = None) -> None:
    """トレードをログに記録"""
    try:
        with Session() as s:
            s.add(Trade(
                symbol=sym, 
                side=side, 
                qty=qty, 
                price=price,
                amount=amount,
                exit_at=exit_at,
                stoploss_price=stoploss_price,
                take_profit_price=take_profit_price
            ))
            s.commit()
    except Exception as e:
        # セッション内でのエラーをキャッチしてログに記録
        print(f"トレードログ記録エラー: {e}")
        log_error(f"トレードログ記録エラー: {e}")

def due_trades(now):
    """期限切れのトレードを取得"""
    try:
        with Session() as s:
            # 複合インデックスを利用するクエリ
            return s.query(Trade).filter(Trade.exit_at <= now, Trade.closed == 0).all()
    except Exception as e:
        print(f"期限切れトレード取得エラー: {e}")
        log_error(f"期限切れトレード取得エラー: {e}")
        return []

def mark_closed(tr, pnl: float = None, close_reason: str = "time_expiry") -> None:
    """トレードをクローズ済みとしてマーク"""
    try:
        with Session() as s:
            tr.closed = 1
            tr.close_reason = close_reason
            
            if pnl is not None:
                tr.pnl = pnl
                # PnL率も計算して保存
                if tr.price and float(tr.price) > 0:
                    current_price = float(tr.pnl) / float(tr.qty) + float(tr.price)
                    tr.pnl_percent = ((current_price / float(tr.price)) - 1) * 100
            
            tr.updated = datetime.utcnow()
            s.add(tr)
            s.commit()
    except Exception as e:
        print(f"トレードクローズマークエラー: {e}")
        log_error(f"トレードクローズマークエラー: {e}")

def log_error(message: str, severity: str = "error") -> None:
    """エラーをログに記録"""
    try:
        with Session() as s:
            s.add(ErrorLog(message=message, severity=severity))
            s.commit()
    except Exception as e:
        # 最後の手段としてprint
        print(f"エラーログ記録失敗: {e}, 元のエラー: {message}")

def get_open_trades():
    """オープン中のトレードを取得"""
    try:
        with Session() as s:
            # インデックスを利用するクエリ
            return s.query(Trade).filter(Trade.closed == 0).all()
    except Exception as e:
        print(f"オープントレード取得エラー: {e}")
        log_error(f"オープントレード取得エラー: {e}")
        return []

def get_trade_by_symbol(symbol: str):
    """指定シンボルのオープントレードを取得"""
    try:
        with Session() as s:
            # 複合条件でのクエリ（両方インデックス付き）
            return s.query(Trade).filter(Trade.symbol == symbol, Trade.closed == 0).first()
    except Exception as e:
        print(f"{symbol} トレード取得エラー: {e}")
        log_error(f"{symbol} トレード取得エラー: {e}")
        return None

def get_soon_expiring_trades(hours=1):
    """間もなく期限切れになるトレードを取得"""
    try:
        now = datetime.utcnow()
        threshold = now + timedelta(hours=hours)
        with Session() as s:
            return s.query(Trade).filter(
                Trade.exit_at <= threshold,
                Trade.exit_at > now,
                Trade.closed == 0
            ).order_by(Trade.exit_at).all()
    except Exception as e:
        print(f"期限間近トレード取得エラー: {e}")
        log_error(f"期限間近トレード取得エラー: {e}")
        return []

def get_pnl_stats(days=30):
    """損益統計を取得"""
    try:
        from_date = datetime.utcnow() - timedelta(days=days)
        with Session() as s:
            trades = s.query(Trade).filter(
                Trade.created >= from_date,
                Trade.closed == 1,
                Trade.pnl != None
            ).all()
            
            if not trades:
                return {
                    "total_trades": 0,
                    "profitable_trades": 0,
                    "total_pnl": 0,
                    "win_rate": 0,
                    "avg_pnl": 0
                }
            
            profitable = sum(1 for t in trades if float(t.pnl) > 0)
            total_pnl = sum(float(t.pnl) for t in trades)
            
            return {
                "total_trades": len(trades),
                "profitable_trades": profitable,
                "total_pnl": total_pnl,
                "win_rate": profitable / len(trades) if trades else 0,
                "avg_pnl": total_pnl / len(trades) if trades else 0
            }
    except Exception as e:
        print(f"PnL統計取得エラー: {e}")
        log_error(f"PnL統計取得エラー: {e}")
        return {
            "total_trades": 0,
            "profitable_trades": 0,
            "total_pnl": 0,
            "win_rate": 0,
            "avg_pnl": 0,
            "error": str(e)
        }

def log_backtest_result(results):
    """バックテスト結果をDBに記録"""
    try:
        with Session() as s:
            s.add(BacktestResult(**results))
            s.commit()
    except Exception as e:
        print(f"バックテスト結果記録エラー: {e}")
        log_error(f"バックテスト結果記録エラー: {e}")

def get_backtest_results(limit=10):
    """バックテスト結果を取得"""
    try:
        with Session() as s:
            return s.query(BacktestResult).order_by(BacktestResult.created.desc()).limit(limit).all()
    except Exception as e:
        print(f"バックテスト結果取得エラー: {e}")
        log_error(f"バックテスト結果取得エラー: {e}")
        return []

def log_performance(stats, current_balance=None, notes=None):
    """パフォーマンス統計をログに記録"""
    try:
        with Session() as s:
            open_positions = s.query(Trade).filter(Trade.closed == 0).count()
            
            s.add(PerformanceLog(
                period_days=stats.get('period_days', 30),
                total_trades=stats.get('total_trades', 0),
                profitable_trades=stats.get('profitable_trades', 0),
                total_pnl=stats.get('total_pnl', 0),
                win_rate=stats.get('win_rate', 0),
                avg_pnl=stats.get('avg_pnl', 0),
                current_balance=current_balance,
                open_positions=open_positions,
                notes=notes
            ))
            s.commit()
    except Exception as e:
        print(f"パフォーマンスログ記録エラー: {e}")
        log_error(f"パフォーマンスログ記録エラー: {e}")

def log_api_stat(endpoint, method, success, response_time, error_message=None):
    """API統計をログに記録"""
    try:
        with Session() as s:
            s.add(APIStat(
                endpoint=endpoint,
                method=method,
                success=success,
                response_time=response_time,
                error_message=error_message
            ))
            s.commit()
    except Exception as e:
        print(f"API統計記録エラー: {e}")
        log_error(f"API統計記録エラー: {e}")

def log_websocket_stat(event_type, duration=None, message_count=None, symbols=None, error_message=None):
    """WebSocket統計をログに記録"""
    try:
        with Session() as s:
            s.add(WebSocketStat(
                event_type=event_type,
                duration=duration,
                message_count=message_count,
                symbols=symbols,
                error_message=error_message
            ))
            s.commit()
    except Exception as e:
        print(f"WebSocket統計記録エラー: {e}")
        log_error(f"WebSocket統計記録エラー: {e}")

def get_trade_history(days=30, limit=100):
    """取引履歴を取得"""
    try:
        from_date = datetime.utcnow() - timedelta(days=days)
        with Session() as s:
            return s.query(Trade).filter(
                Trade.created >= from_date,
                Trade.closed == 1
            ).order_by(Trade.created.desc()).limit(limit).all()
    except Exception as e:
        print(f"取引履歴取得エラー: {e}")
        log_error(f"取引履歴取得エラー: {e}")
        return []

def get_error_history(days=7, severity="error", limit=100):
    """エラー履歴を取得"""
    try:
        from_date = datetime.utcnow() - timedelta(days=days)
        with Session() as s:
            return s.query(ErrorLog).filter(
                ErrorLog.created >= from_date,
                ErrorLog.severity == severity
            ).order_by(ErrorLog.created.desc()).limit(limit).all()
    except Exception as e:
        print(f"エラー履歴取得エラー: {e}")
        log_error(f"エラー履歴取得エラー: {e}")
        return []

def get_performance_history(days=90):
    """パフォーマンス履歴を取得"""
    try:
        from_date = datetime.utcnow() - timedelta(days=days)
        with Session() as s:
            return s.query(PerformanceLog).filter(
                PerformanceLog.date >= from_date
            ).order_by(PerformanceLog.date).all()
    except Exception as e:
        print(f"パフォーマンス履歴取得エラー: {e}")
        log_error(f"パフォーマンス履歴取得エラー: {e}")
        return []

def vacuum_database():
    """データベースの最適化とバキューム処理"""
    try:
        connection = engine.raw_connection()
        cursor = connection.cursor()
        cursor.execute("VACUUM")
        connection.commit()
        cursor.close()
        connection.close()
        return True
    except Exception as e:
        print(f"データベース最適化エラー: {e}")
        log_error(f"データベース最適化エラー: {e}")
        return False