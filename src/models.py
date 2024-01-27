from dataclasses import dataclass
from typing import List, Tuple

@dataclass
class Trade:
    symbol: str
    timestamp: int
    amount: float
    price: float
    is_buy: bool

@dataclass
class OHLCVC:
    symbol: str
    timestamp: int
    high: float
    low: float
    open: float
    close: float
    volume: float
    count: int

@dataclass
class Orderbook:
    symbol: str
    timestamp: int
    asks: List[Tuple[float, float]]
    bids: List[Tuple[float, float]]
