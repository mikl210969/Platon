"""
Statistics Engine (EMA версия).
Расчет динамических порогов на основе экспоненциального сглаживания по сессиям.
"""

import logging
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


class StatisticsEngine:
    def __init__(self, event_bus: EventBus, window_size: int = 100, min_observations: int = 60, timezone_offset_hours: float = 0.0):
        self.bus = event_bus
        self.window_size = window_size
        self.min_obs = min_observations
        self.tz_offset = timedelta(hours=timezone_offset_hours)
        
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

        # Сессии в UTC. Адаптация чувствительности под реальную ликвидность рынка.
        self.sessions = [
            # Тихие сессии: повышаем k_wall, чтобы ловить только реальные аномалии, а не шум
            SessionConfig("asian_night", 21, 24, k_wall=2.0, k_delta=1.5, k_atr=2.5),
            SessionConfig("asian", 0, 8, k_wall=1.8, k_delta=1.5, k_atr=2.5),
            
            # Активные сессии: снижаем k_wall, чтобы не пропустить начало движения при росте объемов
            SessionConfig("european", 8, 13, k_wall=1.3, k_delta=2.0, k_atr=3.0),
            SessionConfig("american", 13, 21, k_wall=1.2, k_delta=2.0, k_atr=3.0),
        ]
        
        # Подписка на снапшоты
        self.bus.subscribe("MARKET:SNAPSHOT", self._on_snapshot)
        
        logger.info(f"📊 StatisticsEngine инициализирован (EMA). Alpha: wall={self.alpha_wall}, delta={self.alpha_delta}, atr={self.alpha_atr}")

    def _get_current_session(self, dt: datetime) -> SessionConfig:
        utc_dt = dt - self.tz_offset
        hour = utc_dt.hour
        for session in self.sessions:
            if session.start_hour <= hour < session.end_hour:
                return session
        return self.sessions[-1]

    async def _on_snapshot(self, snapshot: Dict[str, Any]):
        """Обновление EMA на основе снапшота рынка."""
        if not snapshot:
            return
        
        walls = snapshot.get("walls", {})
        delta = snapshot.get("delta", {})
        atr = snapshot.get("atr", 0.5)
        
        # Берем максимум из стен bid/ask
        bid_wall = walls.get("bid_v1_max", 0.0)
        ask_wall = walls.get("ask_v1_max", 0.0)
        max_wall = max(bid_wall, ask_wall)
        
        # Дельта за 5 минут
        delta_5m = delta.get("windows", {}).get(300, {}).get("velocity", 0.0)
        
        # Обновляем EMA
        self.update(datetime.now(timezone.utc), max_wall, delta_5m, atr)
        
        # Если готовы — публикуем пороги
        if self._is_ready:
            thresholds = self.get_thresholds(datetime.now(timezone.utc))
            if thresholds:
                await self.bus.publish("ANALYTICS:THRESHOLDS", thresholds)

    def update(self, timestamp: datetime, wall_max: float, delta: float, atr: float):
        """Обновляет EMA переменные новыми данными."""
        self._obs_count += 1
        
        # Инициализация при первом наблюдении
        if self._obs_count == 1:
            self._ema_wall_mean = wall_max
            self._ema_wall_var = 0.0
            self._ema_delta_mean = delta
            self._ema_delta_var = 0.0
            self._ema_atr_mean = atr
            return
        
        # EMA для mean и variance
        # Wall
        wall_diff = wall_max - self._ema_wall_mean
        self._ema_wall_mean = self._ema_wall_mean + self.alpha_wall * wall_diff
        self._ema_wall_var = (1 - self.alpha_wall) * (self._ema_wall_var + self.alpha_wall * wall_diff ** 2)
        
        # Delta
        delta_diff = delta - self._ema_delta_mean
        self._ema_delta_mean = self._ema_delta_mean + self.alpha_delta * delta_diff
        self._ema_delta_var = (1 - self.alpha_delta) * (self._ema_delta_var + self.alpha_delta * delta_diff ** 2)
        
        # ATR
        atr_diff = atr - self._ema_atr_mean
        self._ema_atr_mean = self._ema_atr_mean + self.alpha_atr * atr_diff
        
        # Готовность
        if self._obs_count >= self.min_obs and not self._is_ready:
            self._is_ready = True
            logger.info(f"✅ StatisticsEngine готов (наблюдений: {self._obs_count})")

    def get_thresholds(self, current_time: datetime) -> Optional[DynamicThresholds]:
        if not self._is_ready:
            return None

        session = self._get_current_session(current_time)
        
        # Защита от нулевой дисперсии
        wall_std = max(self._ema_wall_var ** 0.5, 0.01)
        delta_std = max(self._ema_delta_var ** 0.5, 0.01)

        return DynamicThresholds(
            wall_threshold=self._ema_wall_mean + (session.k_wall * wall_std),
            delta_threshold_long=self._ema_delta_mean - (session.k_delta * delta_std),
            delta_threshold_short=self._ema_delta_mean + (session.k_delta * delta_std),
            max_departure_pct=session.k_atr * self._ema_atr_mean,
            current_session=session.name
        )