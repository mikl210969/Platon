"""
GB Zone — торговая зона стратегии Golden Bounce.
Логика: ОТСКАК от стены (не пробой). Версия: Кумулятивная дельта + 48ч TTL.
"""

import math
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


class ZoneStatus(Enum):
    CONFIRMED = "CONFIRMED"
    DEPARTED = "DEPARTED"
    RETURNING = "RETURNING"
    SIGNAL_READY = "SIGNAL_READY"
    INVALIDATED = "INVALIDATED"


@dataclass
class GBZone:
    symbol: str
    price: float
    side: str
    created_at: datetime
    atr_at_creation: float

    id: str = field(default_factory=lambda: f"GB_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M')}_{uuid.uuid4().hex[:6]}")
    status: ZoneStatus = ZoneStatus.CONFIRMED

    peak_volume: float = 0.0
    score_multiplier: float = 1.0
    fill_refills: int = 0
    pull_cycles: int = 0
    wall_status: str = "CONFIRMED"
    invalidation_reason: Optional[str] = None

    departure_threshold: float = 0.0
    extreme_price: Optional[float] = None
    high_price: Optional[float] = None
    departure_confirmed_at: Optional[datetime] = None
    
    delta_recent: float = 0.0
    delta_pullback: float = 0.0
    delta_z_score: float = 0.0
    delta_short: float = 0.0
    absorption_score: float = 0.0
    cumulative_delta: float = 0.0
 
    ttl_seconds: float = 172800.0
    return_ttl_seconds: float = 3600.0

    return_window_pct: float = 0.005
    _last_return_check: Optional[datetime] = None

    @property
    def trade_side(self) -> str:
        return "LONG" if self.side == "bid" else "SHORT"

    def calculate_departure_threshold(self, atr_multiplier: float = 1.5) -> float:
        distance = atr_multiplier * self.atr_at_creation
        max_distance_pct = self.price * 0.04
        actual_distance = min(distance, max_distance_pct)

        if self.side == "bid":
            return self.price - actual_distance
        else:
            return self.price + actual_distance

    def check_departure(self, current_price: float, atr_multiplier: float = 1.5) -> bool:
        if self.status != ZoneStatus.CONFIRMED:
            if self.status == ZoneStatus.DEPARTED and self.extreme_price is not None:
                if self.side == "bid" and current_price < self.extreme_price:
                    self.extreme_price = current_price
                elif self.side == "ask" and current_price > self.extreme_price:
                    self.extreme_price = current_price
            return False

        distance = atr_multiplier * self.atr_at_creation
        max_distance_pct = self.price * 0.04
        actual_distance = min(distance, max_distance_pct)

        threshold = self.price - actual_distance if self.side == "bid" else self.price + actual_distance
        passed = False

        if self.side == "bid" and current_price <= threshold:
            passed = True
        elif self.side == "ask" and current_price >= threshold:
            passed = True

        if passed:
            self.departure_threshold = threshold
            self.status = ZoneStatus.DEPARTED
            self.departure_confirmed_at = datetime.now(timezone.utc)
            self.high_price = self.price
            self.extreme_price = current_price

        return passed

    def update_cumulative_delta(self, signed_delta: float):
        self.cumulative_delta += signed_delta

    def update_delta_metrics(self, delta_recent: float, delta_short: float, gross_60: float):
        self.delta_recent = delta_recent
        self.delta_short = delta_short
        self.delta_z_score = (delta_recent / math.sqrt(gross_60)) if gross_60 > 0 else 0.0
        
        if self.high_price is not None and self.extreme_price is not None:
            price_range = abs(self.high_price - self.extreme_price)
            if price_range > 0 and self.atr_at_creation > 0:
                normalized_range = price_range / self.atr_at_creation
                self.absorption_score = abs(self.delta_pullback) / normalized_range

    def check_return(self, current_price: float) -> bool:
        if self.status != ZoneStatus.DEPARTED or self.extreme_price is None:
            return False

        now = datetime.now(timezone.utc)
        if self._last_return_check and (now - self._last_return_check).total_seconds() < 5:
            return False
        self._last_return_check = now

        return_window = self.price * self.return_window_pct

        if self.side == "bid":
            if current_price >= (self.price - return_window):
                self.status = ZoneStatus.RETURNING
                return True
        else:
            if current_price <= (self.price + return_window):
                self.status = ZoneStatus.RETURNING
                return True

        return False

    def is_expired(self, now: Optional[datetime] = None) -> bool:
        now = now or datetime.now(timezone.utc)
        return (now - self.created_at).total_seconds() > self.ttl_seconds

    def is_return_expired(self, now: Optional[datetime] = None) -> bool:
        if self.departure_confirmed_at is None:
            return False
        now = now or datetime.now(timezone.utc)
        return (now - self.departure_confirmed_at).total_seconds() > self.return_ttl_seconds

    def calculate_sl_tp(self, current_entry_price: float) -> Optional[dict]:
        if self.extreme_price is None or self.price is None:
            return None

        departure_abs = abs(self.price - self.extreme_price)
        buffer = self.extreme_price * 0.002 

        if self.side == "bid":
            sl = self.extreme_price - buffer
            tp1 = current_entry_price + (departure_abs * 0.5)
            tp2 = current_entry_price + departure_abs
        else:
            sl = self.extreme_price + buffer
            tp1 = current_entry_price - (departure_abs * 0.5)
            tp2 = current_entry_price - departure_abs

        risk_per_unit = abs(current_entry_price - sl)

        return {
            "entry": current_entry_price,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "risk_per_unit": risk_per_unit,
            "departure_abs": departure_abs
        }

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "symbol": self.symbol,
            "price": self.price,
            "side": self.side,
            "trade_side": self.trade_side,
            "status": self.status.value,
            "peak_volume": self.peak_volume,
            "score_multiplier": self.score_multiplier,
            "atr_at_creation": self.atr_at_creation,
            "departure_threshold": self.departure_threshold,
            "extreme_price": self.extreme_price,
            "high_price": self.high_price,
            "delta_pullback": round(self.delta_pullback, 2),
            "delta_recent": round(self.delta_recent, 2),
            "cumulative_delta": round(self.cumulative_delta, 2),
            "absorption_score": round(self.absorption_score, 2),
            "fill_refills": self.fill_refills,
            "pull_cycles": self.pull_cycles,
            "wall_status": self.wall_status,
            "created_at": self.created_at.isoformat(),
        }