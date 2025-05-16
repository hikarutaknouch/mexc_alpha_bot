"""
戦略パラメータとユーティリティ
"""
import random

HOLD_HOURS_POOL = [8, 10, 12]

def choose_hold_hours() -> int:
    """8 / 10 / 12 時間のいずれかをランダムで返す"""
    return random.choice(HOLD_HOURS_POOL)