"""
GB Zone — торговая зона стратегии Golden Bounce.
Логика: ОТСКАК от стены (не пробой).
"""

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

    # Данные стены
    peak_volume: float = 0.0
    score_multiplier: float = 1.0
    fill_refills: int = 0
    pull_cycles: int = 0
    wall_status: str = "CONFIRMED"
    invalidation_reason: Optional[str] = None   # ← ДОБАВИТЬ ЭТУ СТРОКУ

    # Отслеживание отката
    departure_threshold: float = 0.0
    extreme_price: Optional[float] = None
    high_price: Optional[float] = None
    departure_confirmed_at: Optional[datetime] = None
    
    # Метрики дельты и абсорбции
    delta_recent: float = 0.0
    delta_pullback: float = 0.0
    delta_z_score: float = 0.0       # ✅ ДОБАВИТЬ ЭТО: Z-скор дельты (60s)
    delta_short: float = 0.0         # ✅ ДОБАВИТЬ ЭТО: Краткосрочная дельта (15s)
    absorption_score: float = 0.0
 
    # Таймауты
    ttl_seconds: float = 7200.0
    return_ttl_seconds: float = 3600.0

    # Гистерезис
    return_window_pct: float = 0.003
    _last_return_check: Optional[datetime] = None

    @property
    def trade_side(self) -> str:
        return "LONG" if self.side == "bid" else "SHORT"

    def calculate_departure_threshold(self, atr_multiplier: float = 1.5) -> float:
        """Рассчитать порог отката: zone_price ± atr_multiplier × ATR."""
        distance = atr_multiplier * self.atr_at_creation
        if self.side == "bid":  # LONG: откат ВВЕРХ от стены поддержки
            return self.price + distance
        else:  # SHORT: откат ВНИЗ от стены сопротивления
            return self.price - distance

    def check_departure(self, current_price: float, atr_multiplier: float = 1.5) -> bool:
        """Проверить, произошел ли откат на 1.5×ATR."""
        if self.status != ZoneStatus.CONFIRMED:
            return False

        threshold = self.calculate_departure_threshold(atr_multiplier)
        passed = False

        if self.side == "bid":  # LONG: цена должна уйти ВВЕРХ выше порога
            if current_price >= threshold:
                passed = True
        else:  # SHORT: цена должна уйти ВНИЗ ниже порога
            if current_price <= threshold:
                passed = True

        if passed:
            self.departure_threshold = threshold
            self.extreme_price = current_price
            self.status = ZoneStatus.DEPARTED
            self.departure_confirmed_at = datetime.now(timezone.utc)
            self.high_price = self.price  # Точка начала отката — это цена самой стены
            return True

        # Обновляем экстремум
        if self.extreme_price is None:
            self.extreme_price = current_price
        else:
            if self.side == "bid" and current_price > self.extreme_price:
                self.extreme_price = current_price
            elif self.side == "ask" and current_price < self.extreme_price:
                self.extreme_price = current_price

        return False

    def update_delta(self, delta_recent: float, current_price: float):
        """Обновить дельту и рассчитать абсорбцию."""
        self.delta_recent = delta_recent
        if self.high_price is not None and self.extreme_price is not None:
            price_range = abs(self.high_price - self.extreme_price)
            if price_range > 0 and self.atr_at_creation > 0:
                normalized_range = price_range / self.atr_at_creation
                self.absorption_score = abs(self.delta_pullback) / normalized_range

    def check_return(self, current_price: float) -> bool:
        """Проверить возврат с гистерезисом."""
        if self.status != ZoneStatus.DEPARTED or self.extreme_price is None:
            return False

        now = datetime.now(timezone.utc)
        if self._last_return_check and (now - self._last_return_check).total_seconds() < 5:
            return False
        self._last_return_check = now

        return_window = self.price * self.return_window_pct

        if self.side == "bid":  # LONG: цена падает обратно ВНИЗ к стене
            if current_price <= (self.price + return_window):
                self.status = ZoneStatus.RETURNING
                return True
        else:  # SHORT: цена растет обратно ВВЕРХ к стене
            if current_price >= (self.price - return_window):
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

    def calculate_sl_tp(self, sl_buffer_mult: float = 1.0, tp1_mult: float = 1.5, tp2_mult: float = 3.0) -> Optional[dict]:
        """SL от цены входа (стены) с буфером ×ATR; TP1/TP2 по кратным риска."""
        if self.atr_at_creation <= 0:
            return None

        risk = sl_buffer_mult * self.atr_at_creation

        if self.side == "bid":  # LONG: стоп ниже стены
            sl = self.price - risk
            tp1 = self.price + (tp1_mult * risk)
            tp2 = self.price + (tp2_mult * risk)
        else:  # SHORT: стоп выше стены
            sl = self.price + risk
            tp1 = self.price - (tp1_mult * risk)
            tp2 = self.price - (tp2_mult * risk)

        return {
            "entry": self.price,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "risk": risk,
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
            "absorption_score": round(self.absorption_score, 2),
            "fill_refills": self.fill_refills,
            "pull_cycles": self.pull_cycles,
            "wall_status": self.wall_status,
            "created_at": self.created_at.isoformat(),
        }