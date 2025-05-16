"""
超シンプルなトレードログ ORM
"""
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker

engine = create_engine("sqlite:///trades.sqlite", echo=False, future=True)
Session = sessionmaker(bind=engine)
Base = declarative_base()

class Trade(Base):
    __tablename__ = "trades"
    id      = Column(Integer, primary_key=True)
    symbol  = Column(String)
    side    = Column(String)           # BUY / SELL
    qty     = Column(Float)
    exit_at = Column(DateTime)
    closed  = Column(Integer, default=0)
    created = Column(DateTime, default=datetime.utcnow)

Base.metadata.create_all(engine)

def log_trade(sym: str, side: str, qty: float, exit_at=None) -> None:
    with Session() as s:
        s.add(Trade(symbol=sym, side=side, qty=qty, exit_at=exit_at))
        s.commit()

def due_trades(now):
    with Session() as s:
        return s.query(Trade).filter(Trade.exit_at <= now, Trade.closed == 0).all()

def mark_closed(tr) -> None:
    with Session() as s:
        tr.closed = 1
        s.commit()