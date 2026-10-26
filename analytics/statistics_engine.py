"""
Statistics Engine (EMA версия + Медиана объёма стен).
Расчет динамических порогов на основе экспоненциального сглаживания по сессиям.
"""

import logging
import numpy as np
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any
from core.event_bus import EventBus

logger = logging.getLogger("plato.statistics_engine")


@dataclass
class SessionConfig:
    name: str
    start_hour: int
    end_hour: int
    k_wall: float
    k_delta: float
    k_atr: float


@dataclass
class DynamicThresholds:
    wall_threshold: float
    delta_threshold_long: float
    delta_threshold_short: float
    max_departure_pct: float
    current_session: str
    # ✅ НОВОЕ: Медианы для фильтра пуленепробиваемых стен
    wall_median_bid: float
    wall_median_ask: float


class StatisticsEngine:
    def __init__(self, event_bus: EventBus, window_size: int = 100, min_observations: int = 60, timezone_offset_hours: float = 0.0, wall_baseline_bars: int = 28):
        self.bus = event_bus
        self.window_size = window_size
        self.min_obs = min_observations
        self.tz_offset = timedelta(hours=timezone_offset_hours)
        self.wall_baseline_bars = wall_baseline_bars
        
        # EMA коэффициенты
        self.alpha_wall = 0.15
        self.alpha_delta = 0.20
        self.alpha_atr = 0.15
        
        # EMA переменные
        self._ema_wall_mean = 0.0
        self._ema_wall_var = 0.0
        self._ema_delta_mean = 0.0
        self._ema_delta_var = 0.0
        self._ema_atr_mean = 0.0
        
        self._obs_count = 0
        self._is_ready = False

        # ✅ НОВОЕ: История объёмов стен для расчёта медианы
        self.wall_volume_history_bid = deque(maxlen=wall_baseline_bars)
        self.wall_volume_history_ask = deque(maxlen=wall_baseline_bars)
        self.wall_median_bid = 0.0
        self.wall_median_ask = 0.0

        # Сессии в UTC
        self.sessions = [
            SessionConfig("asian_night", 21, 24, k_wall=2.0, k_delta=1.5, k_atr=2.5),
            SessionConfig("asian", 0, 8, k_wall=1.8, k_delta=1.5, k_atr=2.5),
            SessionConfig("european", 8, 13, k_wall=1.3, k_delta=2.0, k_atr=3.0),
            SessionConfig("american", 13, 21, k_wall=1.2, k_delta=2.0, k_atr=3.0),
        ]
        
        self.bus.subscribe("MARKET:SNAPSHOT", self._on_snapshot)
        logger.info(f"📊 StatisticsEngine инициализирован (EMA + Median {wall_baseline_bars} bars).")

    def _get_current_session(self, dt: datetime) -> SessionConfig:
        utc_dt = dt - self.tz_offset
        hour = utc_dt.hour
        for session in self.sessions:
            if session.start_hour <= hour < session.end_hour:
                return session
        return self.sessions[-1]

    def update_wall_median(self, bid_wall: float, ask_wall: float):
        """Обновляет историю и пересчитывает медиану объёма стен."""
        if bid_wall > 0:
            self.wall_volume_history_bid.append(bid_wall)
        if ask_wall > 0:
            self.wall_volume_history_ask.append(ask_wall)
        
        # Считаем медиану, если накопили хотя бы 5 значений
        if len(self.wall_volume_history_bid) >= 5:
            self.wall_median_bid = float(np.median(self.wall_volume_history_bid))
        if len(self.wall_volume_history_ask) >= 5:
            self.wall_median_ask = float(np.median(self.wall_volume_history_ask))

    async def _on_snapshot(self, snapshot: Dict[str, Any]):
        """Обновление EMA и медиан на основе снапшота рынка."""
        if not snapshot:
            return
        
        walls = snapshot.get("walls", {})
        delta = snapshot.get("delta", {})
        atr = snapshot.get("atr", 0.5)
        
        bid_wall = walls.get("bid_v1_max", 0.0)
        ask_wall = walls.get("ask_v1_max", 0.0)
        max_wall = max(bid_wall, ask_wall)
        delta_5m = delta.get("windows", {}).get(300, {}).get("velocity", 0.0)
        
        # ✅ Обновляем медианы
        self.update_wall_median(bid_wall, ask_wall)
        
        # Обновляем EMA
        self.update(datetime.now(timezone.utc), max_wall, delta_5m, atr)
        
        if self._is_ready:
            thresholds = self.get_thresholds(datetime.now(timezone.utc))
            if thresholds:
                await self.bus.publish("ANALYTICS:THRESHOLDS", thresholds)

    def update(self, timestamp: datetime, wall_max: float, delta: float, atr: float):
        """Обновляет EMA переменные новыми данными."""
        self._obs_count += 1
        
        if self._obs_count == 1:
            self._ema_wall_mean = wall_max
            self._ema_wall_var = 0.0
            self._ema_delta_mean = delta
            self._ema_delta_var = 0.0
            self._ema_atr_mean = atr
            return
        
        wall_diff = wall_max - self._ema_wall_mean
        self._ema_wall_mean = self._ema_wall_mean + self.alpha_wall * wall_diff
        self._ema_wall_var = (1 - self.alpha_wall) * (self._ema_wall_var + self.alpha_wall * wall_diff ** 2)
        
        delta_diff = delta - self._ema_delta_mean
        self._ema_delta_mean = self._ema_delta_mean + self.alpha_delta * delta_diff
        self._ema_delta_var = (1 - self.alpha_delta) * (self._ema_delta_var + self.alpha_delta * delta_diff ** 2)
        
        atr_diff = atr - self._ema_atr_mean
        self._ema_atr_mean = self._ema_atr_mean + self.alpha_atr * atr_diff
        
        if self._obs_count >= self.min_obs and not self._is_ready:
            self._is_ready = True
            logger.info(f"✅ StatisticsEngine готов (наблюдений: {self._obs_count})")

    def get_thresholds(self, current_time: datetime) -> Optional[DynamicThresholds]:
        if not self._is_ready:
            return None

        session = self._get_current_session(current_time)
        wall_std = max(self._ema_wall_var ** 0.5, 0.01)
        delta_std = max(self._ema_delta_var ** 0.5, 0.01)

        return DynamicThresholds(
            wall_threshold=self._ema_wall_mean + (session.k_wall * wall_std),
            delta_threshold_long=self._ema_delta_mean - (session.k_delta * delta_std),
            delta_threshold_short=self._ema_delta_mean + (session.k_delta * delta_std),
            max_departure_pct=session.k_atr * self._ema_atr_mean,
            current_session=session.name,
            wall_median_bid=self.wall_median_bid if self.wall_median_bid > 0 else self._ema_wall_mean,
            wall_median_ask=self.wall_median_ask if self.wall_median_ask > 0 else self._ema_wall_mean
        )