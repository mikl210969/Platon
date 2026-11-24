"""
GB Zone — торговая зона стратегии Golden Bounce.
Логика: ОТСКАК от стены (не пробой). Версия: Кумулятивная дельта + 48ч TTL.
v3.4 (Шаг 1-7): снапшот стакана, кластерный SL, встречный кластер, телеметрия autopsy.
"""

import math
import uuid
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Optional, List, Dict, Any

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

    return_confirmed_at: Optional[datetime] = None

    delta_recent: float = 0.0
    delta_pullback: float = 0.0
    delta_z_score: float = 0.0
    delta_short: float = 0.0
    absorption_score: float = 0.0
    cumulative_delta: float = 0.0
    delta_bounce_snapshot: float = 0.0

    ttl_seconds: float = 172800.0
    return_ttl_seconds: float = 3600.0
    return_window_pct: float = 0.005
    _last_return_check: Optional[datetime] = None

    bounce_start_time: Optional[datetime] = None
    bounce_confirmed: bool = False

    bar_history: List[Dict] = field(default_factory=list)

    # ✅ ШАГ 1: Поле для хранения снимка стакана на момент оценки сигнала
    orderbook_snapshot: Optional[Dict[str, Any]] = None

    # ✅ ШАГ 7: Причина последнего отклонения внутри расчёта SL (для телеметрии)
    last_rejection_reason: Optional[str] = None

    @property
    def trade_side(self) -> str:
        return "LONG" if self.side == "bid" else "SHORT"

    def add_bar(self, bar: Dict):
        self.bar_history.append(bar)
        if len(self.bar_history) > 200:
            self.bar_history = self.bar_history[-200:]

    def find_swing(self, n: int = 5, window: int = 48, lng: bool = True) -> Optional[float]:
        if len(self.bar_history) < 2 * n + 1:
            return None

        end = len(self.bar_history)
        search_start = max(n, end - window - n)
        search_end = end - n

        if search_end <= search_start:
            return None

        swings = []
        for i in range(search_start, search_end):
            if lng:
                current_low = self.bar_history[i]["low"]
                is_swing = all(
                    current_low <= self.bar_history[j]["low"]
                    for j in range(i - n, i + n + 1)
                    if j != i and 0 <= j < len(self.bar_history)
                )
                if is_swing:
                    swings.append(current_low)
            else:
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
                self.return_confirmed_at = now
                logger.info(f"🔄 [{self.symbol}] ZONE {self.id} RETURNING: price={current_price:.2f}, wall={self.high_price:.2f}, delta_snapshot={self.delta_bounce_snapshot:.2f}")
                return True
        else:
            if (self.high_price - return_tolerance) <= current_price <= (self.high_price + return_tolerance):
                self.status = ZoneStatus.RETURNING
                self.delta_bounce_snapshot = self.cumulative_delta
                self.return_confirmed_at = now
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
        if self.departure_confirmed_at is None or self.return_confirmed_at is None:
            return None
        elapsed_sec = (self.return_confirmed_at - self.departure_confirmed_at).total_seconds()
        return int(elapsed_sec / 300)

    def calculate_return_delta(self) -> float:
        return self.cumulative_delta - self.delta_bounce_snapshot

    # ✅ ШАГ 1: Метод для сохранения снапшота
    def set_orderbook_snapshot(self, snapshot: Dict[str, Any]):
        """Сохраняет актуальный снимок стакана на момент оценки сигнала."""
        self.orderbook_snapshot = snapshot

    def find_cluster_boundary(self, tick_size: float, cfg: Optional[dict] = None,
                              start_price: Optional[float] = None,
                              scan_side: Optional[str] = None) -> Optional[float]:
        """
        Находит границу кластера ликвидности в стакане.

        Параметры:
            tick_size: шаг цены инструмента
            cfg: конфиг кластерного поиска (cluster_search + min_cluster_volume_usd)
            start_price: цена, от которой начинаем сканировать.
                         Если None — используется self.price (стена).
            scan_side: 'bid' или 'ask' — какую сторону стакана сканировать.
                       Если None — используется self.side (своя сторона, для SL).

        Возвращает цену границы кластера или None.
        """
        if not self.orderbook_snapshot:
            return None

        if cfg is None:
            cfg = {}
        k_median = cfg.get("k_median", 4)
        break_coefficient = cfg.get("break_coefficient", 3.0)
        quick_signal_coefficient = cfg.get("quick_signal_coefficient", 2.0)
        min_liquid_levels = cfg.get("min_liquid_levels", 3)
        min_liquid_width_ticks = cfg.get("min_liquid_width_ticks", 5)
        max_liquid_volume_pct = cfg.get("max_liquid_volume_pct", 0.10)
        min_cluster_volume_usd = cfg.get("min_cluster_volume_usd", 20000)

        data = self.orderbook_snapshot.get("data", {})
        levels = data.get(scan_side if scan_side else self.side, [])

        if not levels:
            return None

        parsed_levels = [(float(p), float(v)) for p, v in levels]

        # Определяем стартовый индекс
        effective_start = start_price if start_price is not None else self.price
        start_idx = 0

        if scan_side == "ask" or (scan_side is None and self.side == "ask"):
            # asks отсортированы по возрастанию: ищем первый уровень >= start_price
            for i, (p, v) in enumerate(parsed_levels):
                if p >= effective_start:
                    start_idx = i
                    break
        else:
            # bids отсортированы по убыванию: ищем первый уровень <= start_price
            for i, (p, v) in enumerate(parsed_levels):
                if p <= effective_start:
                    start_idx = i
                    break

        cluster_volume_usd = 0.0
        prev_volumes = []

        is_liquid_zone = False
        liquid_zone_volume_usd = 0.0
        liquid_levels_count = 0
        first_liquid_price: Optional[float] = None

        for i in range(start_idx, len(parsed_levels)):
            price, vol = parsed_levels[i]
            vol_usd = vol * price

            if not is_liquid_zone:
                cluster_volume_usd += vol_usd
                prev_volumes.append(vol_usd)
                if len(prev_volumes) > k_median:
                    prev_volumes.pop(0)

                if len(prev_volumes) >= k_median:
                    median_vol = sorted(prev_volumes)[len(prev_volumes) // 2]
                    prev_vol_usd = parsed_levels[i-1][1] * parsed_levels[i-1][0] if i > start_idx else median_vol

                    is_quick_break = vol_usd < (prev_vol_usd / quick_signal_coefficient)
                    is_median_break = vol_usd < (median_vol / break_coefficient)

                    if is_quick_break or is_median_break:
                        is_liquid_zone = True
                        liquid_zone_volume_usd = vol_usd
                        liquid_levels_count = 1
                        first_liquid_price = price
            else:
                if first_liquid_price is None:
                    first_liquid_price = price

                liquid_zone_volume_usd += vol_usd
                liquid_levels_count += 1
                current_width_ticks = abs(price - first_liquid_price) / tick_size

                if (liquid_levels_count >= min_liquid_levels and
                    current_width_ticks >= min_liquid_width_ticks and
                    liquid_zone_volume_usd < (cluster_volume_usd * max_liquid_volume_pct)):

                    if cluster_volume_usd >= min_cluster_volume_usd:
                        logger.info(f"🔍 [{self.symbol}] Cluster boundary at {price:.4f}, "
                                    f"vol=${cluster_volume_usd:.0f}, width={current_width_ticks:.1f} ticks")
                        return price
                    else:
                        logger.info(f"⚠️ [{self.symbol}] Cluster invalid (vol=${cluster_volume_usd:.0f} < ${min_cluster_volume_usd})")
                        return None

                if i == len(parsed_levels) - 1:
                    if cluster_volume_usd >= min_cluster_volume_usd:
                        logger.info(f"🔍 [{self.symbol}] Cluster boundary at end of book: {price:.4f}")
                        return price
                    else:
                        return None

        return None

    def _check_opposite_cluster(self, entry_price: float, tp1: float, tp2: float,
                                tick_size: float, cfg: Optional[dict] = None,
                                cluster_sl_cfg: Optional[dict] = None) -> dict:
        """
        Проверяет наличие встречного кластера на пути к TP1/TP2.

        Возвращает dict:
            - found: bool
            - price: Optional[float] — цена ближнего края кластера
            - volume_usd: float
            - tp1_blocked: bool — кластер до TP1 (минус буфер)
            - tp2_shift: Optional[float] — новая цена TP2 (или None)
            - skip_reason: Optional[str] — 'no_snapshot' если снапшота нет
        """
        result = {
            "found": False,
            "price": None,
            "volume_usd": 0.0,
            "tp1_blocked": False,
            "tp2_shift": None,
            "skip_reason": None
        }

        if not self.orderbook_snapshot:
            result["skip_reason"] = "no_snapshot"
            return result

        if cluster_sl_cfg is None:
            cluster_sl_cfg = {}
        tp_buffer_ticks = cluster_sl_cfg.get("tp_buffer_ticks", 3)

        # Определяем сторону сканирования для встречного кластера
        # LONG → ищем в asks (сопротивление), SHORT → в bids (поддержка)
        opposite_side = "ask" if self.side == "bid" else "bid"

        # Стартовая цена: лучшая цена противоположной стороны
        data = self.orderbook_snapshot.get("data", {})
        opposite_levels = data.get(opposite_side, [])
        if not opposite_levels:
            result["skip_reason"] = "no_opposite_levels"
            return result

        best_opposite_price = float(opposite_levels[0][0])

        # Ищем кластер
        cluster_price = self.find_cluster_boundary(
            tick_size, cfg,
            start_price=best_opposite_price,
            scan_side=opposite_side
        )

        if cluster_price is None:
            return result

        result["found"] = True
        result["price"] = cluster_price
        result["volume_usd"] = 0.0  # TODO: точный объём через отдельный проход

        # Определяем буфер в цене
        buffer_price = tp_buffer_ticks * tick_size

        # Проверяем положение кластера относительно TP1 и TP2
        if self.side == "bid":  # LONG: кластер в asks выше entry
            cluster_near_edge = cluster_price - buffer_price  # ближний край кластера минус буфер

            if cluster_near_edge <= tp1:
                # Кластер до TP1 → отклоняем сделку
                result["tp1_blocked"] = True
                logger.info(f"🚫 [{self.symbol}] TP1 blocked by opposite cluster at {cluster_price:.4f}")
            elif cluster_near_edge < tp2:
                # Кластер между TP1 и TP2 → сдвигаем TP2
                result["tp2_shift"] = cluster_near_edge
                logger.info(f"📉 [{self.symbol}] TP2 shifted from {tp2:.4f} to {cluster_near_edge:.4f} "
                            f"(opposite cluster at {cluster_price:.4f})")
            # else: кластер за TP2 → не трогаем
        else:  # SHORT: кластер в bids ниже entry
            cluster_near_edge = cluster_price + buffer_price

            if cluster_near_edge >= tp1:
                result["tp1_blocked"] = True
                logger.info(f"🚫 [{self.symbol}] TP1 blocked by opposite cluster at {cluster_price:.4f}")
            elif cluster_near_edge > tp2:
                result["tp2_shift"] = cluster_near_edge
                logger.info(f"📈 [{self.symbol}] TP2 shifted from {tp2:.4f} to {cluster_near_edge:.4f} "
                            f"(opposite cluster at {cluster_price:.4f})")

        return result

    def _calculate_cluster_sl(self, current_entry_price: float, cluster_boundary_price: float,
                              tick_size: float, atr: float, cfg: Optional[dict] = None,
                              cluster_search_cfg: Optional[dict] = None) -> Optional[dict]:
        """
        Рассчитывает SL на основе границы кластера ликвидности.
        Проверяет встречный кластер и корректирует TP1/TP2.
        """
        # ✅ ШАГ 7: сброс причины отклонения перед расчётом
        self.last_rejection_reason = None

        if cfg is None:
            cfg = {}
        buffer_atr_mult = cfg.get("buffer_atr_mult", 0.1)
        buffer_min_ticks = cfg.get("buffer_min_ticks", 3)
        tp1_mult = cfg.get("tp1_mult", 1.0)
        tp2_mult = cfg.get("tp2_mult", 2.0)

        # Буфер = max(0.1 * ATR, min_ticks * tick_size)
        buffer_price = max(buffer_atr_mult * atr, buffer_min_ticks * tick_size)

        lng = self.side == "bid"
        if lng:
            sl = cluster_boundary_price - buffer_price
            if sl >= current_entry_price:
                self.last_rejection_reason = "cluster_sl_above_entry"
                logger.warning(f"⚠️ [{self.symbol}] Cluster SL >= entry for LONG, invalid")
                return None
            sl_distance = current_entry_price - sl
            tp1 = current_entry_price + sl_distance * tp1_mult
            tp2 = current_entry_price + sl_distance * tp2_mult
        else:
            sl = cluster_boundary_price + buffer_price
            if sl <= current_entry_price:
                self.last_rejection_reason = "cluster_sl_below_entry"
                logger.warning(f"⚠️ [{self.symbol}] Cluster SL <= entry for SHORT, invalid")
                return None
            sl_distance = sl - current_entry_price
            tp1 = current_entry_price - sl_distance * tp1_mult
            tp2 = current_entry_price - sl_distance * tp2_mult

        # Округление SL в сторону увеличения риска
        if lng:
            sl = math.floor(sl / tick_size) * tick_size
        else:
            sl = math.ceil(sl / tick_size) * tick_size

        logger.info(f"🛡️ [{self.symbol}] Cluster SL: {sl:.4f} (boundary={cluster_boundary_price:.4f}, buffer={buffer_price:.4f})")

        # ✅ Проверка встречного кластера
        opposite_result = self._check_opposite_cluster(
            current_entry_price, tp1, tp2, tick_size,
            cfg=cluster_search_cfg, cluster_sl_cfg=cfg
        )

        # Edge case: снапшот недоступен — пропускаем проверку
        if opposite_result.get("skip_reason"):
            logger.info(f"ℹ️ [{self.symbol}] Opposite cluster check skipped: {opposite_result['skip_reason']}")
        else:
            # Edge case: TP1 заблокирован
            if opposite_result["tp1_blocked"]:
                self.last_rejection_reason = "tp1_blocked_by_opposite_cluster"
                logger.warning(f"🚫 [{self.symbol}] Trade rejected: TP1 blocked by opposite cluster")
                return None

            # Сдвиг TP2
            if opposite_result["tp2_shift"] is not None:
                tp2 = opposite_result["tp2_shift"]

                # Edge case: сдвинутый TP2 <= TP1
                if lng and tp2 <= tp1:
                    self.last_rejection_reason = "tp2_shifted_below_tp1"
                    logger.warning(f"🚫 [{self.symbol}] Trade rejected: shifted TP2 ({tp2:.4f}) <= TP1 ({tp1:.4f})")
                    return None
                if not lng and tp2 >= tp1:
                    self.last_rejection_reason = "tp2_shifted_above_tp1"
                    logger.warning(f"🚫 [{self.symbol}] Trade rejected: shifted TP2 ({tp2:.4f}) >= TP1 ({tp1:.4f})")
                    return None

                # Округление TP2 в сторону уменьшения прибыли
                if lng:
                    tp2 = math.floor(tp2 / tick_size) * tick_size
                else:
                    tp2 = math.ceil(tp2 / tick_size) * tick_size

                # Проверка R:R после сдвига (порог 1.5)
                rr_after_shift = abs(tp2 - current_entry_price) / sl_distance
                if rr_after_shift < 1.5:
                    self.last_rejection_reason = "rr_too_low_after_cluster_adjust"
                    logger.warning(f"🚫 [{self.symbol}] Trade rejected: R:R after shift = {rr_after_shift:.2f} < 1.5")
                    return None

                logger.info(f"✅ [{self.symbol}] TP2 shifted to {tp2:.4f}, R:R after shift = {rr_after_shift:.2f}")

        return {
            "entry": current_entry_price,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "risk_per_unit": sl_distance,
            "departure_abs": abs(self.price - (self.extreme_price or self.price)),
            "sl_source": "cluster",
            "cluster_boundary_price": cluster_boundary_price,
            "opposite_cluster_found": opposite_result["found"],
            "opposite_cluster_price": opposite_result["price"],
            "tp2_shifted": opposite_result["tp2_shift"] is not None
        }

    def calculate_sl_tp(self, current_entry_price: float, sl_buffer_pct: float = 0.002,
                        tp1_fraction: float = 0.5, swing_sl_cfg: Optional[dict] = None,
                        atr: float = 0.0, cluster_boundary_price: Optional[float] = None,
                        tick_size: float = 0.01, cluster_sl_cfg: Optional[dict] = None,
                        cluster_search_cfg: Optional[dict] = None) -> Optional[dict]:
        """
        Decision tree для расчёта SL/TP:
        1. Cluster SL (если граница найдена, конфиг передан И enabled=true)
        2. Swing SL (если включён и есть данные)
        3. Legacy SL (fallback)
        """
        cluster_sl_shadow_price: Optional[float] = None

        # Приоритет 1: Кластерный SL
        if (cluster_boundary_price is not None and cluster_sl_cfg
                and cluster_sl_cfg.get("enabled", False)):
            cluster_result = self._calculate_cluster_sl(
                current_entry_price, cluster_boundary_price, tick_size, atr,
                cfg=cluster_sl_cfg, cluster_search_cfg=cluster_search_cfg
            )
            if cluster_result:
                cluster_result["sl_source"] = "cluster"
                return cluster_result

        # Shadow-логирование
        if (cluster_boundary_price is not None and cluster_sl_cfg
                and not cluster_sl_cfg.get("enabled", False)):
            shadow_result = self._calculate_cluster_sl(
                current_entry_price, cluster_boundary_price, tick_size, atr,
                cfg=cluster_sl_cfg, cluster_search_cfg=cluster_search_cfg
            )
            if shadow_result:
                cluster_sl_shadow_price = shadow_result["sl"]
                logger.info(
                    f"👻 [{self.symbol}] SHADOW cluster SL: {cluster_sl_shadow_price:.4f} "
                    f"(boundary={cluster_boundary_price:.4f}) — не используется, идёт Swing"
                )

        # Приоритет 2: Swing SL
        if swing_sl_cfg and swing_sl_cfg.get("enabled", False) and atr > 0:
            swing_result = self._calculate_sl_tp_swing(current_entry_price, swing_sl_cfg, atr)
            if swing_result:
                if cluster_sl_shadow_price is not None:
                    swing_result["cluster_sl_shadow"] = cluster_sl_shadow_price
                swing_result["sl_source"] = "swing"
                swing_result["cluster_boundary_price"] = cluster_boundary_price
                return swing_result

        # Приоритет 3: Legacy
        legacy_result = self._calculate_sl_tp_legacy(current_entry_price, sl_buffer_pct, tp1_fraction)
        if legacy_result:
            legacy_result["sl_source"] = "legacy"
            if cluster_sl_shadow_price is not None:
                legacy_result["cluster_sl_shadow"] = cluster_sl_shadow_price
            legacy_result["cluster_boundary_price"] = cluster_boundary_price
        return legacy_result

    def _calculate_sl_tp_swing(self, current_entry_price: float, cfg: dict, atr: float) -> Optional[dict]:
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

        if lng:
            sl = swing - sl_buffer_atr_mult * atr
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