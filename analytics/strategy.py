"""
Adaptive Zone V1 Strategy.
Поиск аномальных зон (крупные стены) + возврат цены + фильтр дельты.
LONG (стены bid) и SHORT (стены ask).
✅ МУЛЬТИВАЛЮТНОСТЬ: Изоляция зон по символам + symbol во всех логах + троттлинг ZONE_ANALYSIS.
"""
import asyncio
import json
import logging
import sys
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import List, Optional, Dict, Any, TYPE_CHECKING
from pathlib import Path

from core.event_bus import EventBus
from execution.passport_manager import PassportManager, generate_client_order_id

if TYPE_CHECKING:
    from execution.order_manager import OrderManager
    from execution.passport_storage import PassportStorage
    from execution.position_monitor import PositionMonitor

logger = logging.getLogger("plato.strategy")

autopsy_logger = logging.getLogger("plato.strategy_autopsy")
autopsy_logger.setLevel(logging.INFO)
log_path = Path("logs/strategy_autopsy.jsonl")
log_path.parent.mkdir(exist_ok=True)
if not any(isinstance(h, logging.FileHandler) for h in autopsy_logger.handlers):
    file_handler = logging.FileHandler(log_path, encoding='utf-8')
    file_handler.setFormatter(logging.Formatter('%(message)s'))
    autopsy_logger.addHandler(file_handler)


class ZoneState(Enum):
    DETECTED = "DETECTED"
    DEPARTED = "DEPARTED"
    INVALIDATED = "INVALIDATED"
    ACTIVE_TRADE = "ACTIVE_TRADE"


@dataclass
class AnomalyZone:
    id: str
    price: float
    volume: float
    timestamp: str
    state: str = "DETECTED"
    side: str = "LONG"
    min_price_after: Optional[float] = None
    max_price_after: Optional[float] = None
    candles_buffer: List[Dict[str, Any]] = field(default_factory=list)
    rejection_logged: bool = False


class AdaptiveZoneV1:
    def __init__(self, event_bus: EventBus, rest_client=None, order_manager=None, 
                 passport_storage=None, position_monitor=None, shadow_mode: bool = True, 
                 min_departure_pct: float = 0.3, max_departure_pct: float = 4.0,
                 config=None, symbol: str = "SOLUSDT"):
        
        self.bus = event_bus
        self.rest_client = rest_client
        self.order_manager: Optional['OrderManager'] = order_manager
        self.passport_storage: Optional['PassportStorage'] = passport_storage
        self.position_monitor: Optional['PositionMonitor'] = position_monitor
        self.shadow_mode = shadow_mode
        self.min_departure_pct = min_departure_pct
        self.max_departure_pct = max_departure_pct
        self.symbol = symbol
        
        self.config = config or {}
        self.exit_logic = self.config.get("trading.exit_logic", "atr")
        self.order_ttl_seconds = self.config.get("trading.orders.order_ttl_seconds", 120)
        
        logger.info(f" [{self.symbol}] AdaptiveZoneV1: exit_logic = '{self.exit_logic}', order_ttl = {self.order_ttl_seconds}s")
        
        self.active_zones_by_symbol: Dict[str, List[AnomalyZone]] = {self.symbol: []}
        self.current_thresholds: Optional[Any] = None
        self.current_snapshot: Optional[Dict[str, Any]] = None

        self.zones_file = Path(f"logs/active_zones_{self.symbol}.json")
        self.zones_file.parent.mkdir(exist_ok=True)
        self._load_zones()

        self.bus.subscribe("ANALYTICS:THRESHOLDS", self._on_thresholds)
        self.bus.subscribe(f"MARKET:SNAPSHOT:{self.symbol}", self._on_snapshot)
        self.bus.subscribe("ORDER:TIMER_CANCELLED_WITH_ZONE", self._on_timer_cancelled)

        logger.info(f" [{self.symbol}] AdaptiveZoneV1 инициализирована. Загружено {len(self.active_zones_by_symbol[self.symbol])} зон. Теневой режим: {shadow_mode}")

    @property
    def active_zones(self) -> List[AnomalyZone]:
        return self.active_zones_by_symbol.get(self.symbol, [])

    def _has_active_trade(self) -> bool:
        """Проверяет, есть ли активная сделка ТОЛЬКО для текущего символа."""
        if not self.position_monitor:
            return False
            
        for p in self.position_monitor.active_passports.values():
            # ✅ КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Игнорируем паспорта других символов
            if p.symbol != self.symbol:
                continue
                
            try:
                mgr = self.position_monitor._get_manager(p)
                if mgr.is_active():
                    return True
            except Exception:
                if p.status not in ("CLOSED", "ORDER_CANCELLED", "ORDER_REJECTED", "ERROR_STATE", "IDLE"):
                    return True
                    
        return False

    def _load_zones(self):
        if not self.zones_file.exists():
            return
        try:
            with open(self.zones_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for item in data:
                    zone = AnomalyZone(
                        id=item['id'], price=item['price'], volume=item['volume'],
                        timestamp=item['timestamp'], state=item['state'],
                        side=item.get('side', 'LONG'),
                        min_price_after=item.get('min_price_after'),
                        max_price_after=item.get('max_price_after'),
                        candles_buffer=item.get('candles_buffer', [])
                    )
                    self.active_zones_by_symbol[self.symbol].append(zone)
        except Exception as e:
            logger.error(f"[{self.symbol}] Ошибка загрузки зон: {e}")

    def _save_zones(self):
        try:
            data = [asdict(zone) for zone in self.active_zones_by_symbol.get(self.symbol, [])]
            with open(self.zones_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"[{self.symbol}] Ошибка сохранения зон: {e}")

    async def _get_futures_price(self, symbol: str) -> Optional[float]:
        if not self.rest_client:
            logger.warning(f"[{self.symbol}] ⚠️ REST-клиент не передан, проверка спреда пропущена")
            return None
        try:
            ticker = await self.rest_client.get_ticker("linear", symbol)
            if ticker and ticker.get("list"):
                return float(ticker["list"][0]["lastPrice"])
        except Exception as e:
            logger.error(f"[{self.symbol}] ❌ Ошибка получения фьючерсной цены: {e}")
        return None

    async def _on_thresholds(self, thresholds):
        self.current_thresholds = thresholds

    async def _on_snapshot(self, snapshot: Dict[str, Any]):
        self.current_snapshot = snapshot
        if not self.current_thresholds:
            return
        current_price = snapshot.get("current_price", 0.0)
        if current_price == 0:
            return
        walls = snapshot.get("walls", {})
        delta = snapshot.get("delta", {})
        bid_v1_max = walls.get("bid_v1_max", 0.0)
        ask_v1_max = walls.get("ask_v1_max", 0.0)
        delta_5m = delta.get("windows", {}).get(300, {}).get("velocity", 0.0)
        await self._process_zones(current_price, bid_v1_max, ask_v1_max, delta_5m)

    async def _on_timer_cancelled(self, payload: Dict[str, Any]):
        zone_id = payload.get("zone_id")
        if not zone_id:
            return
        zone = next((z for z in self.active_zones if z.id == zone_id), None)
        if not zone or zone.state != "ACTIVE_TRADE":
            return
        current_price = self.current_snapshot.get("current_price", 0.0) if self.current_snapshot else 0.0
        if current_price == 0:
            return
        distance_pct = abs(zone.price - current_price) / zone.price * 100
        reset_threshold_pct = 0.8
        if distance_pct <= reset_threshold_pct:
            zone.state = "DEPARTED"
            logger.info(f"🔄 [{zone_id}] Conditional Reset: ордер отменен, цена в коридоре ({distance_pct:.2f}%). Зона сброшена в DEPARTED.")
            autopsy_logger.info(json.dumps({
                "event": "ZONE_RESET_AFTER_TIMEOUT", "symbol": self.symbol, "zone_id": zone_id,
                "distance_pct": round(distance_pct, 2), "threshold": reset_threshold_pct,
                "current_price": current_price
            }))
        else:
            zone.state = "INVALIDATED"
            logger.info(f"❌ [{zone_id}] Zone Invalidated: ордер отменен, цена ушла на {distance_pct:.2f}% (> {reset_threshold_pct}%).")
            autopsy_logger.info(json.dumps({
                "event": "ZONE_INVALIDATED_AFTER_TIMEOUT", "symbol": self.symbol, "zone_id": zone_id,
                "distance_pct": round(distance_pct, 2), "threshold": reset_threshold_pct,
                "current_price": current_price
            }))
        self._save_zones()

    async def _process_zones(self, current_price: float, bid_wall: float, ask_wall: float, delta_5m: float):
        if self.current_thresholds is None:
            return
        
        now = datetime.now(timezone.utc)
        thresholds = self.current_thresholds
        zones = self.active_zones
        
        # ✅ ЕДИНЫЙ ИСТОЧНИК ИСТИНЫ: читаем коэффициент из конфига
        wall_quality_mult = self.config.get("trading.structure_validation.wall_volume_multiple", 2.0)
        
        # ✅ БАЗОВЫЙ УРОВЕНЬ: используем МЕДИАНУ, а не EMA+Std
        median_bid = getattr(thresholds, 'wall_median_bid', thresholds.wall_threshold)
        median_ask = getattr(thresholds, 'wall_median_ask', thresholds.wall_threshold)
        
        # Защита от нулевых значений
        safe_med_bid = max(median_bid, 1.0)
        safe_med_ask = max(median_ask, 1.0)

        # Рассчитываем пороги на основе медианы
        quality_threshold_bid = safe_med_bid * wall_quality_mult
        quality_threshold_ask = safe_med_ask * wall_quality_mult

        def has_nearby_zone(price: float, side: str, radius_pct: float = 0.5) -> bool:
            for zone in zones:
                if zone.side != side:
                    continue
                if zone.state in ("INVALIDATED", "ACTIVE_TRADE"):
                    continue
                distance_pct = abs(zone.price - price) / price * 100
                if distance_pct <= radius_pct:
                    return True
            return False

        # ✅ LONG: Создаем зону ТОЛЬКО если стена >= multiplier x МЕДИАНЫ
        if bid_wall > quality_threshold_bid and not has_nearby_zone(current_price, "LONG"):
            zone_id = f"Z_{now.strftime('%Y%m%d_%H%M')}_LONG"
            zone = AnomalyZone(
                id=zone_id, 
                price=current_price, 
                volume=bid_wall,
                timestamp=now.isoformat(), 
                side="LONG",
                candles_buffer=[{"close": current_price, "low": current_price * 0.998, "delta": delta_5m}]
            )
            zones.append(zone)
            self._save_zones()
            
            autopsy_logger.info(json.dumps({
                "event": "ZONE_DETECTED_MEDIAN_BASED", 
                "symbol": self.symbol, 
                "zone_id": zone_id, 
                "price": current_price, 
                "volume": round(bid_wall, 2), 
                "median_ref": round(safe_med_bid, 2),
                "multiplier_applied": wall_quality_mult,
                "required_vol": round(quality_threshold_bid, 2),
                "side": "LONG"
            }))
            logger.info(f"🎯 [{self.symbol}] [ZONE DETECTED] {zone_id} | Vol: {bid_wall:.0f} > Req: {quality_threshold_bid:.0f} ({wall_quality_mult}x Med)")

        # ✅ SHORT: Создаем зону ТОЛЬКО если стена >= multiplier x МЕДИАНЫ
        if ask_wall > quality_threshold_ask and not has_nearby_zone(current_price, "SHORT"):
            zone_id = f"Z_{now.strftime('%Y%m%d_%H%M')}_SHORT"
            zone = AnomalyZone(
                id=zone_id, 
                price=current_price, 
                volume=ask_wall,
                timestamp=now.isoformat(), 
                side="SHORT",
                candles_buffer=[{"close": current_price, "high": current_price * 1.002, "delta": delta_5m}]
            )
            zones.append(zone)
            self._save_zones()
            
            autopsy_logger.info(json.dumps({
                "event": "ZONE_DETECTED_MEDIAN_BASED", 
                "symbol": self.symbol, 
                "zone_id": zone_id, 
                "price": current_price, 
                "volume": round(ask_wall, 2), 
                "median_ref": round(safe_med_ask, 2),
                "multiplier_applied": wall_quality_mult,
                "required_vol": round(quality_threshold_ask, 2),
                "side": "SHORT"
            }))
            logger.info(f"🎯 [{self.symbol}] [ZONE DETECTED] {zone_id} | Vol: {ask_wall:.0f} > Req: {quality_threshold_ask:.0f} ({wall_quality_mult}x Med)")

        # Обработка существующих зон (без изменений)
        zones_to_remove = []
        for zone in zones:
            zone.candles_buffer.append({"close": current_price, "delta": delta_5m})
            if len(zone.candles_buffer) > 200:
                zone.candles_buffer.pop(0)

            if zone.state == "DETECTED":
                self._check_departure(zone, current_price)
            elif zone.state == "DEPARTED":
                signal = await self._check_return_and_entry(zone, current_price, delta_5m, thresholds)
                if signal:
                    if self.shadow_mode:
                        logger.info(f" [{self.symbol}] [SHADOW] Сигнал: {signal}")
                    else:
                        await self._execute_signal(signal, zone)

            zone_dt = datetime.fromisoformat(zone.timestamp)
            is_too_old = (now - zone_dt).total_seconds() > (7 * 24 * 3600)
            if zone.state == "INVALIDATED" or is_too_old:
                zones_to_remove.append(zone)

        if zones_to_remove:
            for z in zones_to_remove:
                zones.remove(z)
            self._save_zones()

    def _check_departure(self, zone: AnomalyZone, current_price: float):
        atr_5m = self.current_snapshot.get("atr_5m", 0.1) if self.current_snapshot else 0.1
        min_departure_abs = 1.5 * atr_5m
        max_departure_abs = zone.price * (self.max_departure_pct / 100.0)
        
        if zone.state == "DEPARTED":
            if zone.side == "LONG":
                if zone.min_price_after is None or current_price < zone.min_price_after:
                    zone.min_price_after = current_price
            else:
                if zone.max_price_after is None or current_price > zone.max_price_after:
                    zone.max_price_after = current_price

        if zone.side == "LONG":
            departure_down_abs = zone.price - current_price
            if departure_down_abs >= min_departure_abs:
                if departure_down_abs > max_departure_abs:
                    zone.state = "INVALIDATED"
                    autopsy_logger.info(json.dumps({
                        "event": "ZONE_INVALIDATED", "symbol": self.symbol, "zone_id": zone.id, 
                        "reason": "deep_departure", "departure_abs": round(departure_down_abs, 4), 
                        "max_allowed": round(max_departure_abs, 4)
                    }))
                else:
                    if zone.state != "DEPARTED":
                        zone.state = "DEPARTED"
                        zone.min_price_after = current_price
                    autopsy_logger.info(json.dumps({
                        "event": "ZONE_DEPARTED", "symbol": self.symbol, "zone_id": zone.id, 
                        "min_price": current_price, "side": "LONG",
                        "departure_abs": round(departure_down_abs, 4), "atr_5m_used": round(atr_5m, 4)
                    }))
                self._save_zones()
        else:
            departure_up_abs = current_price - zone.price
            if departure_up_abs >= min_departure_abs:
                if departure_up_abs > max_departure_abs:
                    zone.state = "INVALIDATED"
                    autopsy_logger.info(json.dumps({
                        "event": "ZONE_INVALIDATED", "symbol": self.symbol, "zone_id": zone.id, 
                        "reason": "deep_departure", "departure_abs": round(departure_up_abs, 4),
                        "max_allowed": round(max_departure_abs, 4)
                    }))
                else:
                    if zone.state != "DEPARTED":
                        zone.state = "DEPARTED"
                        zone.max_price_after = current_price
                    autopsy_logger.info(json.dumps({
                        "event": "ZONE_DEPARTED", "symbol": self.symbol, "zone_id": zone.id, 
                        "max_price": current_price, "side": "SHORT",
                        "departure_abs": round(departure_up_abs, 4), "atr_5m_used": round(atr_5m, 4)
                    }))
                self._save_zones()

    async def _check_return_and_entry(self, zone: AnomalyZone, current_price: float,
                                       current_delta: float, thresholds) -> Optional[Dict[str, Any]]:
        # 1. Возврат к зоне (±0.5%)
        is_in_zone = zone.price * 0.995 <= current_price <= zone.price * 1.005
        
        if not is_in_zone:
            # ✅ Зона не в коридоре — просто выходим, без логов (ждем возврата)
            return None

        # ✅ Проверка таймаута устаревания зоны
        departure_max_bars = self.config.get("trading.structure_validation.departure_max_bars", 24)
        max_age_seconds = departure_max_bars * 300
        zone_age_seconds = (datetime.now(timezone.utc) - datetime.fromisoformat(zone.timestamp)).total_seconds()
        
        if zone_age_seconds > max_age_seconds:
            # ✅ ФИНАЛЬНАЯ СВОДКА: пишем ZONE_ANALYSIS ОДИН РАЗ при инвалидации
            wall_mult = self.config.get("trading.structure_validation.wall_volume_multiple", 2.0)
            median_bid = getattr(thresholds, 'wall_median_bid', thresholds.wall_threshold)
            median_ask = getattr(thresholds, 'wall_median_ask', thresholds.wall_threshold)
            
            safe_med = max((median_bid if zone.side == "LONG" else median_ask), 1.0)
            expected_wall = wall_mult * safe_med
            wall_ratio = zone.volume / expected_wall if expected_wall > 0 else 0
            
            atr_5m = self.current_snapshot.get("atr_5m", 0.1) if self.current_snapshot else 0.1
            dep_threshold = atr_5m * self.config.get("trading.structure_validation.min_retracement_atr_multiplier", 1.5)
            
            has_departed = False
            if zone.side == "LONG":
                has_departed = hasattr(zone, 'min_price_after') and zone.min_price_after is not None
            else:
                has_departed = hasattr(zone, 'max_price_after') and zone.max_price_after is not None
            
            autopsy_logger.info(json.dumps({
                "event": "ZONE_ANALYSIS", 
                "symbol": self.symbol,
                "zone_id": zone.id,
                "side": zone.side,
                "wall_vol": round(zone.volume, 2),
                "median": round(safe_med, 2),
                "wall_ratio": round(wall_ratio, 2),
                "atr5m": round(atr_5m, 4),
                "dep_threshold": round(dep_threshold, 4),
                "age_hours": round(zone_age_seconds / 3600, 2),
                "max_age_bars": departure_max_bars,
                "has_departed": has_departed,
                "reason": "departure_timeout",
                "zone_price": zone.price
            }))
            
            zone.state = "INVALIDATED"
            self._save_zones()
            logger.info(f"⏳ [{zone.id}] Zone invalidated: departure timeout ({zone_age_seconds/3600:.1f}h > {departure_max_bars} bars)")
            autopsy_logger.info(json.dumps({
                "event": "ZONE_INVALIDATED", "symbol": self.symbol, "zone_id": zone.id, 
                "reason": "departure_timeout", "age_hours": round(zone_age_seconds/3600, 2)
            }))
            return None

        # 2. Сумма дельты за буфер
        zone_delta_sum = sum(c.get("delta", 0.0) for c in zone.candles_buffer)
        
        # 3. ✅ ПРОВЕРКА СПРЕДА СПОТ/ФЬЮЧЕРС
        futures_price = await self._get_futures_price(self.symbol)
        spread_pct = 0.0

        if futures_price and futures_price > 0:
            spread_pct = abs(current_price - futures_price) / current_price * 100
            if spread_pct > 0.15:
                logger.warning(f"⚠️ [{zone.id}] Спред спот/фьючерс {spread_pct:.3f}% > 0.15%. Сигнал отменен.")
                autopsy_logger.info(json.dumps({
                    "event": "SIGNAL_REJECTED", "symbol": self.symbol, "zone_id": zone.id, 
                    "reason": "spread_too_high", "spread_pct": round(spread_pct, 4), "threshold": 0.15
                }))
                return None

        # === ПЕРЕКЛЮЧАТЕЛЬ ЛОГИКИ ВЫХОДА ===
        exit_mode = getattr(self, 'exit_logic', 'atr')
        
        if exit_mode == "structure":
            atr_5m = self.current_snapshot.get("atr_5m", 0.1) if self.current_snapshot else 0.1
            
            # ✅ 1. ПРОВЕРКА ПУЛЕНЕПРОБИВАЕМОСТИ СТЕНЫ (синхронизировано с созданием через МЕДИАНА)
            wall_mult = self.config.get("trading.structure_validation.wall_volume_multiple", 2.0)
            median_bid = getattr(thresholds, 'wall_median_bid', thresholds.wall_threshold)
            median_ask = getattr(thresholds, 'wall_median_ask', thresholds.wall_threshold)
            
            safe_med_bid = max(median_bid, 1.0)
            safe_med_ask = max(median_ask, 1.0)
            
            if zone.side == "LONG":
                required_strength = safe_med_bid * wall_mult
                if zone.volume < required_strength:
                    if not zone.rejection_logged:
                        logger.info(f"⚠️ [{zone.id}] Structure signal rejected: weak wall volume {zone.volume:.0f} < {required_strength:.0f}")
                        zone.rejection_logged = True
                        autopsy_logger.info(json.dumps({
                            "event": "SIGNAL_REJECTED", "symbol": self.symbol, "zone_id": zone.id,
                            "reason": "weak_wall_volume", "wall_vol": round(zone.volume, 2),
                            "threshold": round(required_strength, 2)
                        }))
                    return None
                else:
                    zone.rejection_logged = False
            else:
                required_strength = safe_med_ask * wall_mult
                if zone.volume < required_strength:
                    if not zone.rejection_logged:
                        logger.info(f"️ [{zone.id}] Structure signal rejected: weak wall volume {zone.volume:.0f} < {required_strength:.0f}")
                        zone.rejection_logged = True
                        autopsy_logger.info(json.dumps({
                            "event": "SIGNAL_REJECTED", "symbol": self.symbol, "zone_id": zone.id,
                            "reason": "weak_wall_volume", "wall_vol": round(zone.volume, 2),
                            "threshold": round(required_strength, 2)
                        }))
                    return None
                else:
                    zone.rejection_logged = False

            # ✅ 2. РАСЧЁТ УРОВНЕЙ ЧЕРЕЗ RISK_DIST
            if zone.side == "LONG":
                min_extremum = zone.min_price_after
                if min_extremum is None or min_extremum >= zone.price:
                    return None
                
                dep_val = abs(zone.price - min_extremum)
                if dep_val < atr_5m * self.config.get("trading.structure_validation.min_retracement_atr_multiplier", 1.5):
                    return None
                
                sl_buffer_atr = self.config.get("trading.structure_validation.sl_buffer_atr5m", 1.0)
                sl_price = min_extremum - (atr_5m * sl_buffer_atr)
                sl_distance = abs(current_price - sl_price)
                
                tp1_mult = self.config.get("trading.structure_validation.tp1_risk_multiple", 1.5)
                tp2_mult = self.config.get("trading.structure_validation.tp2_risk_multiple", 3.0)
                
                tp1_price = current_price + (sl_distance * tp1_mult)
                tp2_price = current_price + (sl_distance * tp2_mult)
                be_price = None
                
            else:  # SHORT
                max_extremum = zone.max_price_after
                if max_extremum is None or max_extremum <= zone.price:
                    return None
                
                dep_val = abs(max_extremum - zone.price)
                if dep_val < atr_5m * self.config.get("trading.structure_validation.min_retracement_atr_multiplier", 1.5):
                    return None
                
                sl_buffer_atr = self.config.get("trading.structure_validation.sl_buffer_atr5m", 1.0)
                sl_price = max_extremum + (atr_5m * sl_buffer_atr)
                sl_distance = abs(current_price - sl_price)
                
                tp1_mult = self.config.get("trading.structure_validation.tp1_risk_multiple", 1.5)
                tp2_mult = self.config.get("trading.structure_validation.tp2_risk_multiple", 3.0)
                
                tp1_price = current_price - (sl_distance * tp1_mult)
                tp2_price = current_price - (sl_distance * tp2_mult)
                be_price = None
        else:
            # ATR-based fallback
            atr = self.current_snapshot.get("atr", 0.5) if self.current_snapshot else 0.5
            sl_distance = atr * 1.7
            if zone.side == "LONG":
                sl_price = current_price - sl_distance
                tp1_price = current_price + (sl_distance * 1.5)
                tp2_price = current_price + (sl_distance * 3.0)
                be_price = current_price * 1.001
            else:
                sl_price = current_price + sl_distance
                tp1_price = current_price - (sl_distance * 1.5)
                tp2_price = current_price - (sl_distance * 3.0)
                be_price = current_price * 0.999

        # === ФИЛЬТР ДЕЛЬТЫ ===
        if zone.side == "LONG":
            if zone_delta_sum >= thresholds.delta_threshold_long:
                return None
        else:
            if zone_delta_sum <= thresholds.delta_threshold_short:
                return None

        # === ДИНАМИЧЕСКИЙ САЙЗИНГ ===
        TARGET_RISK_USDT = 30.0
        MAX_POSITION_SIZE = 10.0
        MIN_POSITION_SIZE = 0.1
        
        sl_dist_usd = abs(current_price - sl_price)
        safe_qty = TARGET_RISK_USDT / sl_dist_usd if sl_dist_usd > 0 else MAX_POSITION_SIZE
        final_qty = max(MIN_POSITION_SIZE, min(safe_qty, MAX_POSITION_SIZE))
        actual_risk_usdt = round(sl_dist_usd * final_qty, 2)
        delta_formatted = round(zone_delta_sum, 2)

        autopsy_logger.info(json.dumps({
            "event": "EXIT_LOGIC_APPLIED", "symbol": self.symbol, "zone_id": zone.id, 
            "mode": exit_mode, "sl_price": round(sl_price, 4), 
            "tp1": round(tp1_price, 4), "tp2": round(tp2_price, 4),
            "risk_usd": actual_risk_usdt, "qty": final_qty
        }))

        autopsy_logger.info(json.dumps({
            "event": "SIGNAL_GENERATED", "symbol": self.symbol, "zone_id": zone.id, "side": zone.side,
            "spot_price": current_price, "futures_price": futures_price,
            "entry": current_price, "sl": sl_price, "tp1": tp1_price, "tp2": tp2_price, "be": be_price,
            "delta_sum": delta_formatted, "spread_pct": round(spread_pct, 4),
            "exit_mode": exit_mode, "actual_risk_usdt": actual_risk_usdt, "final_qty": final_qty
        }))

        zone.state = "ACTIVE_TRADE"
        self._save_zones()

        return {
            "signal_id": f"AdaptiveZoneV1_{uuid.uuid4().hex[:8]}",
            "symbol": self.symbol,
            "side": zone.side,
            "entry_price": current_price,
            "sl_price": sl_price,
            "tp1_price": tp1_price,
            "tp2_price": tp2_price,
            "be_price": be_price,
            "quantity": final_qty,
            "confidence": 0.95,
            "strategy": "AdaptiveZoneV1",
            "comment": f"Adaptive Zone (Δ: {delta_formatted}, Exit: {exit_mode}, Risk: ${actual_risk_usdt})"
        }
    
    async def _execute_signal(self, signal: Dict[str, Any], zone: AnomalyZone):
        from execution.passport import TradePassport, SignalSource

        logger.info(f"\n{'='*60} [{self.symbol}] СИГНАЛ: {signal['side']} @ {signal['entry_price']} {'='*60}")

        if not self.order_manager:
            logger.error(f"[{self.symbol}] OrderManager не инициализирован!")
            return
        if not self.passport_storage:
            logger.error(f"[{self.symbol}] ❌ PassportStorage не инициализирован!")
            return

        if self._has_active_trade():
            logger.info(f"🔒 [{zone.id}] LOCK_HELD: есть активная сделка — сигнал отклонён")
            autopsy_logger.info(json.dumps({
                "event": "SIGNAL_REJECTED", "symbol": self.symbol, "zone_id": zone.id, "side": signal['side'],
                "reason": "LOCK_HELD", "entry": signal['entry_price']
            }))
            zone.state = "DEPARTED"
            self._save_zones()
            return

        passport = TradePassport(
            signal_id=signal['signal_id'],
            strategy=signal['strategy'],
            signal_source=SignalSource.ADAPTIVE_ZONE,
            symbol=signal['symbol'],
            side=signal['side'],
            planned_size=signal['quantity'],
            position_size=signal['quantity'],
            entry_price=signal['entry_price'],
            fill_price=signal['entry_price'],
            tp1_price=signal['tp1_price'],
            tp2_price=signal['tp2_price'],
            sl_price=signal['sl_price'],
            be_price=signal.get('be_price'),
        )
        mgr = PassportManager(passport)
        mgr.set_state("SIGNAL_GENERATED", reason="adaptive zone signal", source="STRATEGY")
        mgr.set_state("ORDER_PREPARING", reason="building order", source="STRATEGY")

        passport.add_event("SIGNAL: DETAILS",
                           f"SL: {signal['sl_price']}, TP1: {signal['tp1_price']}, TP2: {signal['tp2_price']}, BE: {signal.get('be_price')}",
                           source="STRATEGY")

        await self.bus.publish("EXEC:NEW_TRADE", passport)

        limit_price = str(round(signal['entry_price'] * 0.9998, 2)) if signal['side'] == "LONG" else str(round(signal['entry_price'] * 1.0002, 2))
        
        success = await self.order_manager.place_entry_order(
            passport, limit_price, 
            timeout_seconds=self.order_ttl_seconds,
            order_link_id=passport.client_order_id,
            zone_id=zone.id
        )

        if success:
            mgr.set_state("ORDER_SENT", reason=f"limit @ {limit_price}", source="STRATEGY")
            passport.add_event("ACTION: ENTRY_PLACED", f"Лимитный ордер размещен по {limit_price}", source="STRATEGY")
            logger.info(f"✅ [{passport.passport_id}] Ордер размещен (TTL={self.order_ttl_seconds}s, orderLinkId={passport.client_order_id}).")
            self.passport_storage.save(passport)
        else:
            # ❌ ОШИБКА РАЗМЕЩЕНИЯ ОРДЕРА
            mgr.set_state("ORDER_REJECTED", reason="place_entry_order failed", source="STRATEGY")
            passport.add_event("ERROR: ENTRY_FAILED", "Не удалось разместить ордер", source="STRATEGY")
            logger.warning(f"⚠️ [{passport.passport_id}] Ордер не размещён → ORDER_REJECTED")
            self.passport_storage.save(passport)
            
            # 🔥 НОВОЕ: Инвалидируем зону немедленно
            zone.state = "INVALIDATED"
            self._save_zones()
            autopsy_logger.info(json.dumps({
                "event": "ZONE_INVALIDATED_ON_ERROR", 
                "symbol": self.symbol, 
                "zone_id": zone.id,
                "reason": "order_placement_failed"
            }))