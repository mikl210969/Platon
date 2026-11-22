"""
GB Zone — торговая зона стратегии Golden Bounce.
Логика: ОТСКАК от стены (не пробой). Версия: Кумулятивная дельта + 48ч TTL.
v3.3: Swing SL + early_BE + инструмент-специфичные фильтры
"""

import math
import uuid
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Optional, List, Dict

logger = logging.getLogger("plato.zone")


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
    
    # ✅ Новое поле: время подтверждения возврата (для фильтра return_bars_max)
    return_confirmed_at: Optional[datetime] = None
    
    delta_recent: float = 0.0
    delta_pullback: float = 0.0
    delta_z_score: float = 0.0
    delta_short: float = 0.0
    absorption_score: float = 0.0
    cumulative_delta: float = 0.0
    delta_bounce_snapshot: float = 0.0  # Снимок дельты в момент начала возврата

    ttl_seconds: float = 172800.0
    return_ttl_seconds: float = 3600.0
    return_window_pct: float = 0.005
    _last_return_check: Optional[datetime] = None
    
    bounce_start_time: Optional[datetime] = None
    bounce_confirmed: bool = False
    
    # ✅ Новое поле: история баров для Swing SL
    bar_history: List[Dict] = field(default_factory=list)

    @property
    def trade_side(self) -> str:
        return "LONG" if self.side == "bid" else "SHORT"

    def add_bar(self, bar: Dict):
        """Добавляет новый бар в историю (для Swing SL)."""
        self.bar_history.append(bar)
        # Ограничиваем историю 200 барами (≈16 часов на 5мин)
        if len(self.bar_history) > 200:
            self.bar_history = self.bar_history[-200:]

    def find_swing(self, n: int = 5, window: int = 48, lng: bool = True) -> Optional[float]:
        """
        Находит Swing Low (для LONG) или Swing High (для SHORT) в окне до текущего момента.
        Swing = локальный экстремум с n барами слева и n барами справа, которые выше/ниже.
        """
        if len(self.bar_history) < 2 * n + 1:
            return None
        
        # Ищем Swing только в окне [end - window - n, end - n]
        end = len(self.bar_history)
        search_start = max(n, end - window - n)
        search_end = end - n
        
        if search_end <= search_start:
            return None
        
        swings = []
        for i in range(search_start, search_end):
            if lng:
                # Swing Low: low[i] меньше всех low[i-n..i+n]
                current_low = self.bar_history[i]["low"]
                is_swing = all(
                    current_low <= self.bar_history[j]["low"]
                    for j in range(i - n, i + n + 1)
                    if j != i and 0 <= j < len(self.bar_history)
                )
                if is_swing:
                    swings.append(current_low)
            else:
                # Swing High
                current_high = self.bar_history[i]["high"]
                is_swing = all(
                    current_high >= self.bar_history[j]["high"]
                    for j in range(i - n, i + n + 1)
                    if j != i and 0 <= j < len(self.bar_history)
                )
                if is_swing:
                    swings.append(current_high)
        
        return swings[-1] if swings else None

    def calculate_departure_threshold(self, atr_multiplier: float = 1.5) -> float:
        distance = atr_multiplier * self.atr_at_creation
        max_distance_pct = self.price * 0.04
        actual_distance = min(distance, max_distance_pct)
        return self.price - actual_distance if self.side == "bid" else self.price + actual_distance

    def check_departure(self, current_price: float, atr_multiplier: float = 1.5, min_departure_atr_mult: float = 0.8) -> bool:
        if self.status != ZoneStatus.CONFIRMED:
            return False

        required_distance = min_departure_atr_mult * self.atr_at_creation
        max_distance_pct = self.price * 0.04
        actual_required = min(required_distance, max_distance_pct)

        if self.side == "bid":
            if current_price <= self.price:
                self.bounce_start_time = None
                self.bounce_confirmed = False
                return False
            current_distance = current_price - self.price
            if self.extreme_price is None or current_price > self.extreme_price:
                self.extreme_price = current_price
            if self.bounce_start_time is None:
                self.bounce_start_time = datetime.now(timezone.utc)
        else:
            if current_price >= self.price:
                self.bounce_start_time = None
                self.bounce_confirmed = False
                return False
            current_distance = self.price - current_price
            if self.extreme_price is None or current_price < self.extreme_price:
                self.extreme_price = current_price
            if self.bounce_start_time is None:
                self.bounce_start_time = datetime.now(timezone.utc)

        elapsed = 0.0
        if self.bounce_start_time is not None:
            elapsed = (datetime.now(timezone.utc) - self.bounce_start_time).total_seconds()
            if elapsed >= 600:
                self.bounce_confirmed = True
        
        if current_distance >= actual_required and self.bounce_confirmed:
            self.departure_threshold = current_price
            self.status = ZoneStatus.DEPARTED
            self.departure_confirmed_at = datetime.now(timezone.utc)
            self.high_price = self.price
            logger.info(f"✅ [{self.symbol}] ZONE {self.id} DEPARTED: price={self.price:.2f}, extreme={self.extreme_price:.2f}, dist={current_distance:.2f}, bounce_time={elapsed:.0f}s")
            return True

        return False

    def check_return(self, current_price: float) -> bool:
        if self.status != ZoneStatus.DEPARTED or self.high_price is None:
            return False

        now = datetime.now(timezone.utc)
        if self._last_return_check and (now - self._last_return_check).total_seconds() < 5:
            return False
        self._last_return_check = now

        return_tolerance = self.high_price * 0.001

        if self.side == "bid":
            if (self.high_price - return_tolerance) <= current_price <= (self.high_price + return_tolerance):
                self.status = ZoneStatus.RETURNING
                self.delta_bounce_snapshot = self.cumulative_delta
                self.return_confirmed_at = now  # ✅ Новое: фиксируем время возврата
                logger.info(f"🔄 [{self.symbol}] ZONE {self.id} RETURNING: price={current_price:.2f}, wall={self.high_price:.2f}, delta_snapshot={self.delta_bounce_snapshot:.2f}")
                return True
        else:
            if (self.high_price - return_tolerance) <= current_price <= (self.high_price + return_tolerance):
                self.status = ZoneStatus.RETURNING
                self.delta_bounce_snapshot = self.cumulative_delta
                self.return_confirmed_at = now  # ✅ Новое: фиксируем время возврата
                logger.info(f"🔄 [{self.symbol}] ZONE {self.id} RETURNING: price={current_price:.2f}, wall={self.high_price:.2f}, delta_snapshot={self.delta_bounce_snapshot:.2f}")
                return True

        return False

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

    def is_expired(self, now: Optional[datetime] = None) -> bool:
        now = now or datetime.now(timezone.utc)
        return (now - self.created_at).total_seconds() > self.ttl_seconds

    def is_return_expired(self, now: Optional[datetime] = None) -> bool:
        if self.departure_confirmed_at is None:
            return False
        now = now or datetime.now(timezone.utc)
        return (now - self.departure_confirmed_at).total_seconds() > self.return_ttl_seconds

    def calculate_return_bars(self) -> Optional[int]:
        """Возвращает количество 5-минутных баров между departure и return."""
        if self.departure_confirmed_at is None or self.return_confirmed_at is None:
            return None
        elapsed_sec = (self.return_confirmed_at - self.departure_confirmed_at).total_seconds()
        return int(elapsed_sec / 300)  # 5 мин = 300 сек

    def calculate_return_delta(self) -> float:
        """Возвращает дельту между моментом возврата и моментом отскока."""
        return self.cumulative_delta - self.delta_bounce_snapshot

    def calculate_sl_tp(self, current_entry_price: float, sl_buffer_pct: float = 0.002, 
                        tp1_fraction: float = 0.5, swing_sl_cfg: Optional[dict] = None,
                        atr: float = 0.0) -> Optional[dict]:
        """
        ✅ Swing-based расчёт SL/TP.
        
        Если swing_sl_cfg передан и включен:
            SL = Swing Low/High ± sl_buffer_atr_mult × ATR
            TP1 = entry ± sl_distance × tp1_mult
            TP2 = entry ± sl_distance × tp2_mult
        
        Иначе — fallback на старую логику от стены.
        """
        if swing_sl_cfg and swing_sl_cfg.get("enabled", False) and atr > 0:
            return self._calculate_sl_tp_swing(current_entry_price, swing_sl_cfg, atr)
        
        return self._calculate_sl_tp_legacy(current_entry_price, sl_buffer_pct, tp1_fraction)

    def _calculate_sl_tp_swing(self, current_entry_price: float, cfg: dict, atr: float) -> Optional[dict]:
        """Swing-based расчёт SL/TP."""
        lng = self.side == "bid"
        n = cfg.get("swing_n", 5)
        window = cfg.get("swing_window", 48)
        sl_buffer_atr_mult = cfg.get("sl_buffer_atr_mult", 0.5)
        tp1_mult = cfg.get("tp1_mult", 1.0)
        tp2_mult = cfg.get("tp2_mult", 2.0)
        
        swing = self.find_swing(n, window, lng)
        if swing is None:
            logger.warning(f"⚠️ [{self.symbol}] Swing not found, falling back to legacy SL")
            return self._calculate_sl_tp_legacy(current_entry_price, 0.002, 0.5)
        
        # SL = swing ± sl_buffer_atr_mult * ATR
        if lng:
            sl = swing - sl_buffer_atr_mult * atr
            # Sanity check: SL должен быть ниже входа
            if sl >= current_entry_price:
                logger.warning(f"⚠️ [{self.symbol}] Swing SL >= entry for LONG, invalid")
                return None
            sl_distance = current_entry_price - sl
            tp1 = current_entry_price + sl_distance * tp1_mult
            tp2 = current_entry_price + sl_distance * tp2_mult
        else:
            sl = swing + sl_buffer_atr_mult * atr
            if sl <= current_entry_price:
                logger.warning(f"⚠️ [{self.symbol}] Swing SL <= entry for SHORT, invalid")
                return None
            sl_distance = sl - current_entry_price
            tp1 = current_entry_price - sl_distance * tp1_mult
            tp2 = current_entry_price - sl_distance * tp2_mult
        
        logger.info(f"🎯 [{self.symbol}] SWING SL: swing={swing:.4f}, SL={sl:.4f}, TP1={tp1:.4f}, TP2={tp2:.4f}")
        
        departure_abs = abs(self.price - (self.extreme_price or self.price))
        
        return {
            "entry": current_entry_price,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "risk_per_unit": sl_distance,
            "departure_abs": departure_abs
        }

    def _calculate_sl_tp_legacy(self, current_entry_price: float, sl_buffer_pct: float, 
                                 tp1_fraction: float) -> Optional[dict]:
        """Старая логика расчёта SL/TP (fallback)."""
        if self.high_price is None or self.price is None or self.extreme_price is None:
            return None

        departure_abs = abs(self.price - self.extreme_price)
        buffer = max(self.price * sl_buffer_pct, 0.01)

        if self.side == "bid":
            sl = self.price - buffer
            tp1 = current_entry_price + (departure_abs * tp1_fraction)
            tp2 = current_entry_price + departure_abs
        else:
            sl = self.price + buffer
            tp1 = current_entry_price - (departure_abs * tp1_fraction)
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
            "id": self.id, "symbol": self.symbol, "price": self.price, "side": self.side,
            "trade_side": self.trade_side, "status": self.status.value, "peak_volume": self.peak_volume,
            "score_multiplier": self.score_multiplier, "atr_at_creation": self.atr_at_creation,
            "departure_threshold": self.departure_threshold, "extreme_price": self.extreme_price,
            "high_price": self.high_price, "delta_pullback": round(self.delta_pullback, 2),
            "delta_recent": round(self.delta_recent, 2), "cumulative_delta": round(self.cumulative_delta, 2),
            "absorption_score": round(self.absorption_score, 2), "fill_refills": self.fill_refills,
            "pull_cycles": self.pull_cycles, "wall_status": self.wall_status,
            "created_at": self.created_at.isoformat(),
            "return_bars": self.calculate_return_bars(),
            "return_delta": round(self.calculate_return_delta(), 2),
        }