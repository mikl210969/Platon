"""
Adaptive Zone V1 Strategy.
Поиск аномальных зон (крупные стены) + возврат цены + фильтр дельты.
LONG (стены bid) и SHORT (стены ask).
V2/Блок4: замок R4 (одна сделка за раз), orderLinkId, жизненный цикл через стейт-машину.
"""
import asyncio
import json
import logging
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import List, Optional, Dict, Any, TYPE_CHECKING
from pathlib import Path

from core.event_bus import EventBus
from execution.passport_manager import PassportManager, generate_client_order_id  # V2

if TYPE_CHECKING:
    from execution.order_manager import OrderManager
    from execution.passport_storage import PassportStorage
    from execution.position_monitor import PositionMonitor

logger = logging.getLogger("plato.strategy")

# Логгер для аутопсии
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
    side: str = "LONG"  # LONG или SHORT
    min_price_after: Optional[float] = None
    max_price_after: Optional[float] = None
    candles_buffer: List[Dict[str, Any]] = field(default_factory=list)


class AdaptiveZoneV1:
    def __init__(self, event_bus: EventBus, rest_client=None, order_manager=None, 
                 passport_storage=None, position_monitor=None, shadow_mode: bool = True, 
                 min_departure_pct: float = 0.3, max_departure_pct: float = 4.0,
                 config=None):
        
        self.bus = event_bus
        self.rest_client = rest_client
        self.order_manager: Optional['OrderManager'] = order_manager
        self.passport_storage: Optional['PassportStorage'] = passport_storage
        self.position_monitor: Optional['PositionMonitor'] = position_monitor
        self.shadow_mode = shadow_mode
        self.min_departure_pct = min_departure_pct
        self.max_departure_pct = max_departure_pct
        
        self.config = config or {}
        
        # ← КРИТИЧЕСКИ ВАЖНО: ЧИТАЕМ ФЛАГ ИЗ КОНФИГА
        self.exit_logic = self.config.get("trading", {}).get("exit_logic", "atr")
        # ← V2/Блок5: Читаем TTL из конфига (дефолт 120 сек)
        self.order_ttl_seconds = self.config.get("trading.orders.order_ttl_seconds", 120)
        
        logger.info(f"🎯 AdaptiveZoneV1: exit_logic = '{self.exit_logic}', order_ttl = {self.order_ttl_seconds}s")
        
        self.active_zones: List[AnomalyZone] = []
        self.current_thresholds: Optional[Any] = None
        self.current_snapshot: Optional[Dict[str, Any]] = None

        self.zones_file = Path("logs/active_zones.json")
        self.zones_file.parent.mkdir(exist_ok=True)
        self._load_zones()

        self.bus.subscribe("ANALYTICS:THRESHOLDS", self._on_thresholds)
        self.bus.subscribe("MARKET:SNAPSHOT", self._on_snapshot)
        # ✅ V2/Блок5: Подписка на событие отмены таймера для условного сброса зон
        self.bus.subscribe("ORDER:TIMER_CANCELLED_WITH_ZONE", self._on_timer_cancelled)

        logger.info(f"🎯 AdaptiveZoneV1 инициализирована. Загружено {len(self.active_zones)} зон. Теневой режим: {shadow_mode}")

    # ------------------------------------------------------------
    # V2/Блок4: глобальный замок R4 — одна активная сделка за раз
    # ------------------------------------------------------------
    def _has_active_trade(self) -> bool:
        """Есть ли в мониторинге паспорт в нетерминальном статусе."""
        if not self.position_monitor:
            return False
        for p in self.position_monitor.active_passports.values():
            try:
                mgr = self.position_monitor._get_manager(p)
                if mgr.is_active():
                    return True
            except Exception:
                # Если стейт-машина ещё не инициализирована — считаем по старому статусу
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
                    self.active_zones.append(zone)
        except Exception as e:
            logger.error(f"Ошибка загрузки зон: {e}")

    def _save_zones(self):
        try:
            data = [asdict(zone) for zone in self.active_zones]
            with open(self.zones_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Ошибка сохранения зон: {e}")

    async def _get_futures_price(self, symbol: str) -> Optional[float]:
        """Получает текущую цену фьючерса через REST (один запрос на сигнал)."""
        if not self.rest_client:
            logger.warning("⚠️ REST-клиент не передан, проверка спреда пропущена")
            return None

        try:
            ticker = await self.rest_client.get_ticker("linear", symbol)
            if ticker and ticker.get("list"):
                futures_price = float(ticker["list"][0]["lastPrice"])
                return futures_price
        except Exception as e:
            logger.error(f"❌ Ошибка получения фьючерсной цены: {e}")

        return None

    async def _on_thresholds(self, thresholds):
        self.current_thresholds = thresholds

    async def _on_snapshot(self, snapshot: Dict[str, Any]):
        """Основной цикл стратегии — вызывается каждую секунду."""
        self.current_snapshot = snapshot

        if not self.current_thresholds:
            return  # Ждем готовности StatisticsEngine

        current_price = snapshot.get("current_price", 0.0)
        if current_price == 0:
            return

        walls = snapshot.get("walls", {})
        delta = snapshot.get("delta", {})

        bid_v1_max = walls.get("bid_v1_max", 0.0)
        ask_v1_max = walls.get("ask_v1_max", 0.0)
        delta_5m = delta.get("windows", {}).get(300, {}).get("velocity", 0.0)

        # Обновляем зоны
        await self._process_zones(current_price, bid_v1_max, ask_v1_max, delta_5m)

    async def _on_timer_cancelled(self, payload: Dict[str, Any]):
        """
        V2/Блок5: Обработка отмены ордера по таймауту.
        Проверяет актуальность цены и решает: сбросить зону или инвалидировать.
        """
        zone_id = payload.get("zone_id")
        if not zone_id:
            return
            
        # Находим зону
        zone = next((z for z in self.active_zones if z.id == zone_id), None)
        if not zone or zone.state != "ACTIVE_TRADE":
            return
            
        current_price = self.current_snapshot.get("current_price", 0.0) if self.current_snapshot else 0.0
        if current_price == 0:
            return
            
        # ✅ Проверка актуальности: цена всё ещё в коридоре ±0.8%?
        distance_pct = abs(zone.price - current_price) / zone.price * 100
        reset_threshold_pct = 0.8
        
        if distance_pct <= reset_threshold_pct:
            # Цена близко → сбрасываем зону в DEPARTED для повторной попытки
            zone.state = "DEPARTED"
            logger.info(f"🔄 [{zone_id}] Conditional Reset: ордер отменен, цена в коридоре ({distance_pct:.2f}%). Зона сброшена в DEPARTED.")
            autopsy_logger.info(json.dumps({
                "event": "ZONE_RESET_AFTER_TIMEOUT", "zone_id": zone_id,
                "distance_pct": round(distance_pct, 2), "threshold": reset_threshold_pct,
                "current_price": current_price
            }))
        else:
            # Цена ушла далеко → зона больше не актуальна
            zone.state = "INVALIDATED"
            logger.info(f"❌ [{zone_id}] Zone Invalidated: ордер отменен, цена ушла на {distance_pct:.2f}% (> {reset_threshold_pct}%).")
            autopsy_logger.info(json.dumps({
                "event": "ZONE_INVALIDATED_AFTER_TIMEOUT", "zone_id": zone_id,
                "distance_pct": round(distance_pct, 2), "threshold": reset_threshold_pct,
                "current_price": current_price
            }))
            
        self._save_zones()

    async def _process_zones(self, current_price: float, bid_wall: float, ask_wall: float, delta_5m: float):
        if self.current_thresholds is None:
            return

        now = datetime.now(timezone.utc)
        thresholds = self.current_thresholds

        # ✅ ПРОВЕРКА: не создавать зону, если уже есть активная рядом (в радиусе 0.5%)
        def has_nearby_zone(price: float, side: str, radius_pct: float = 0.5) -> bool:
            for zone in self.active_zones:
                if zone.side != side:
                    continue
                if zone.state in ("INVALIDATED", "ACTIVE_TRADE"):
                    continue
                distance_pct = abs(zone.price - price) / price * 100
                if distance_pct <= radius_pct:
                    return True
            return False

        # 1. Поиск новых зон (LONG — стены bid, SHORT — стены ask)
        min_volume_multiplier = 1.10

        if bid_wall > thresholds.wall_threshold * min_volume_multiplier and not has_nearby_zone(current_price, "LONG"):
            zone_id = f"Z_{now.strftime('%Y%m%d_%H%M')}_LONG"
            zone = AnomalyZone(
                id=zone_id, price=current_price, volume=bid_wall,
                timestamp=now.isoformat(), side="LONG",
                candles_buffer=[{"close": current_price, "low": current_price * 0.998, "delta": delta_5m}]
            )
            self.active_zones.append(zone)
            self._save_zones()
            autopsy_logger.info(json.dumps({"event": "ZONE_DETECTED", "zone_id": zone_id, "price": current_price, "volume": bid_wall, "threshold": thresholds.wall_threshold, "session": thresholds.current_session, "side": "LONG"}))
            logger.info(f"🎯 [ZONE DETECTED] {zone_id} | Vol: {bid_wall:.0f} > {thresholds.wall_threshold:.0f} | LONG")

        if ask_wall > thresholds.wall_threshold * min_volume_multiplier and not has_nearby_zone(current_price, "SHORT"):
            zone_id = f"Z_{now.strftime('%Y%m%d_%H%M')}_SHORT"
            zone = AnomalyZone(
                id=zone_id, price=current_price, volume=ask_wall,
                timestamp=now.isoformat(), side="SHORT",
                candles_buffer=[{"close": current_price, "high": current_price * 1.002, "delta": delta_5m}]
            )
            self.active_zones.append(zone)
            self._save_zones()
            autopsy_logger.info(json.dumps({"event": "ZONE_DETECTED", "zone_id": zone_id, "price": current_price, "volume": ask_wall, "threshold": thresholds.wall_threshold, "session": thresholds.current_session, "side": "SHORT"}))
            logger.info(f"🎯 [ZONE DETECTED] {zone_id} | Vol: {ask_wall:.0f} > {thresholds.wall_threshold:.0f} | SHORT")

        # 2. Обработка существующих зон
        zones_to_remove = []
        for zone in self.active_zones:
            zone.candles_buffer.append({"close": current_price, "delta": delta_5m})
            if len(zone.candles_buffer) > 200:
                zone.candles_buffer.pop(0)

            if zone.state == "DETECTED":
                self._check_departure(zone, current_price)
            elif zone.state == "DEPARTED":
                signal = await self._check_return_and_entry(zone, current_price, delta_5m, thresholds)
                if signal:
                    if self.shadow_mode:
                        logger.info(f"👻 [SHADOW] Сигнал: {signal}")
                    else:
                        # ✅ БОЕВОЙ РЕЖИМ: замок R4 → создаём паспорт → ордер
                        await self._execute_signal(signal, zone)

            # Удаление старых зон
            zone_dt = datetime.fromisoformat(zone.timestamp)
            is_too_old = (now - zone_dt).total_seconds() > (7 * 24 * 3600)
            if zone.state == "INVALIDATED" or is_too_old:
                zones_to_remove.append(zone)

        if zones_to_remove:
            for z in zones_to_remove:
                self.active_zones.remove(z)
            self._save_zones()

    def _check_departure(self, zone: AnomalyZone, current_price: float):
        """Проверка отхода цены от зоны + ОБНОВЛЕНИЕ ЭКСТРЕМУМОВ ОТКАТА."""
        atr_5m = self.current_snapshot.get("atr_5m", 0.1) if self.current_snapshot else 0.1
        min_departure_abs = 1.5 * atr_5m
        max_departure_abs = zone.price * (self.max_departure_pct / 100.0)
        
        # ✅ КРИТИЧЕСКИ ВАЖНО: Обновляем экстремумы КАЖДЫЙ ТИК, пока зона активна
        if zone.state == "DEPARTED":
            if zone.side == "LONG":
                if zone.min_price_after is None or current_price < zone.min_price_after:
                    zone.min_price_after = current_price
            else:  # SHORT
                if zone.max_price_after is None or current_price > zone.max_price_after:
                    zone.max_price_after = current_price

        if zone.side == "LONG":
            departure_down_abs = zone.price - current_price
            if departure_down_abs >= min_departure_abs:
                if departure_down_abs > max_departure_abs:
                    zone.state = "INVALIDATED"
                    autopsy_logger.info(json.dumps({
                        "event": "ZONE_INVALIDATED", "zone_id": zone.id, 
                        "reason": "deep_departure", "departure_abs": round(departure_down_abs, 4), 
                        "max_allowed": round(max_departure_abs, 4)
                    }))
                else:
                    if zone.state != "DEPARTED": # Фиксируем переход только один раз
                        zone.state = "DEPARTED"
                        zone.min_price_after = current_price # Инициализация при переходе
                    autopsy_logger.info(json.dumps({
                        "event": "ZONE_DEPARTED", "zone_id": zone.id, 
                        "min_price": current_price, "side": "LONG",
                        "departure_abs": round(departure_down_abs, 4), "atr_5m_used": round(atr_5m, 4)
                    }))
                self._save_zones()
                
        else:  # SHORT
            departure_up_abs = current_price - zone.price
            if departure_up_abs >= min_departure_abs:
                if departure_up_abs > max_departure_abs:
                    zone.state = "INVALIDATED"
                    autopsy_logger.info(json.dumps({
                        "event": "ZONE_INVALIDATED", "zone_id": zone.id, 
                        "reason": "deep_departure", "departure_abs": round(departure_up_abs, 4),
                        "max_allowed": round(max_departure_abs, 4)
                    }))
                else:
                    if zone.state != "DEPARTED":
                        zone.state = "DEPARTED"
                        zone.max_price_after = current_price
                    autopsy_logger.info(json.dumps({
                        "event": "ZONE_DEPARTED", "zone_id": zone.id, 
                        "max_price": current_price, "side": "SHORT",
                        "departure_abs": round(departure_up_abs, 4), "atr_5m_used": round(atr_5m, 4)
                    }))
                self._save_zones()

    async def _check_return_and_entry(self, zone: AnomalyZone, current_price: float,
                                       current_delta: float, thresholds) -> Optional[Dict[str, Any]]:
        # 1. Возврат к зоне (±0.5%)
        if not (zone.price * 0.995 <= current_price <= zone.price * 1.005):
            return None

        # 2. Сумма дельты за буфер
        zone_delta_sum = sum(c.get("delta", 0.0) for c in zone.candles_buffer)
        
        # 3. ✅ ПРОВЕРКА СПРЕДА СПОТ/ФЬЮЧЕРС
        futures_price = await self._get_futures_price("SOLUSDT")
        spread_pct = 0.0

        if futures_price and futures_price > 0:
            spread_pct = abs(current_price - futures_price) / current_price * 100
            if spread_pct > 0.15:
                logger.warning(f"⚠️ [{zone.id}] Спред спот/фьючерс {spread_pct:.3f}% > 0.15%. Сигнал отменен.")
                autopsy_logger.info(json.dumps({
                    "event": "SIGNAL_REJECTED", "zone_id": zone.id, "reason": "spread_too_high",
                    "spread_pct": round(spread_pct, 4), "threshold": 0.15
                }))
                return None

        # === ПЕРЕКЛЮЧАТЕЛЬ ЛОГИКИ ВЫХОДА ===
        exit_mode = getattr(self, 'exit_logic', 'atr')
        
        if exit_mode == "structure":
            atr_5m = self.current_snapshot.get("atr_5m", 0.1) if self.current_snapshot else 0.1
            
            # Читаем настройки валидации из конфига
            struct_cfg = self.config.get("trading", {}).get("structure_validation", {})
            wall_mult = struct_cfg.get("wall_volume_multiple", 4.0)
            departure_max_bars = struct_cfg.get("departure_max_bars", 24)
            sl_buffer_atr = struct_cfg.get("sl_buffer_atr5m", 1.0)
            tp1_mult = struct_cfg.get("tp1_risk_multiple", 1.5)
            tp2_mult = struct_cfg.get("tp2_risk_multiple", 3.0)

            # ✅ 1. ПРОВЕРКА ТАЙМАУТА ЗОНЫ (24 бара * 5 мин = 7200 сек)
            zone_dt = datetime.fromisoformat(zone.timestamp)
            now = datetime.now(timezone.utc)
            bars_since_creation = int((now - zone_dt).total_seconds() / 300)
            
            if bars_since_creation > departure_max_bars:
                logger.info(f"⚠️ [{zone.id}] Structure signal rejected: departure timeout ({bars_since_creation} bars)")
                zone.state = "INVALIDATED"
                self._save_zones()
                return None

            # ✅ 2. ПРОВЕРКА ПУЛЕНЕПРОБИВАЕМОСТИ СТЕНЫ (4.0 × медиана)
            median_bid = getattr(thresholds, 'wall_median_bid', thresholds.wall_threshold)
            median_ask = getattr(thresholds, 'wall_median_ask', thresholds.wall_threshold)
            
            if zone.side == "LONG":
                if zone.volume < (wall_mult * median_bid):
                    logger.info(f"⚠️ [{zone.id}] Structure signal rejected: weak wall volume {zone.volume:.0f} < {wall_mult * median_bid:.0f}")
                    return None
            else:
                if zone.volume < (wall_mult * median_ask):
                    logger.info(f"⚠️ [{zone.id}] Structure signal rejected: weak wall volume {zone.volume:.0f} < {wall_mult * median_ask:.0f}")
                    return None

            # ✅ 3. РАСЧЁТ УРОВНЕЙ ЧЕРЕЗ RISK_DIST (математический R:R)
            if zone.side == "LONG":
                min_extremum = zone.min_price_after
                if min_extremum is None or min_extremum >= zone.price:
                    return None
                
                dep_val = abs(zone.price - min_extremum)
                if dep_val < atr_5m * 1.5:
                    return None
                
                sl_price = min_extremum - (atr_5m * sl_buffer_atr)
                sl_distance = abs(current_price - sl_price)
                
                tp1_price = current_price + (sl_distance * tp1_mult)
                tp2_price = current_price + (sl_distance * tp2_mult)
                be_price = None
                sl_reason = f"Structure HIGH: SL за минимумом (Dist={sl_distance:.3f}, ATR_buf={atr_5m:.3f})"
                
            else:  # SHORT
                max_extremum = zone.max_price_after
                if max_extremum is None or max_extremum <= zone.price:
                    return None
                
                dep_val = abs(max_extremum - zone.price)
                if dep_val < atr_5m * 1.5:
                    return None
                
                sl_price = max_extremum + (atr_5m * sl_buffer_atr)
                sl_distance = abs(current_price - sl_price)
                
                tp1_price = current_price - (sl_distance * tp1_mult)
                tp2_price = current_price - (sl_distance * tp2_mult)
                be_price = None
                sl_reason = f"Structure HIGH: SL за максимумом (Dist={sl_distance:.3f}, ATR_buf={atr_5m:.3f})"
        else:
            # ATR-based fallback (оставлен для совместимости, если exit_logic != "structure")
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
            sl_reason = f"ATR(1m)-based: SL dist={sl_distance:.3f} (1.7x)"

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
            "event": "EXIT_LOGIC_APPLIED", "zone_id": zone.id, 
            "mode": exit_mode, "sl_price": round(sl_price, 4), 
            "tp1": round(tp1_price, 4), "tp2": round(tp2_price, 4),
            "risk_usd": actual_risk_usdt, "qty": final_qty
        }))

        autopsy_logger.info(json.dumps({
            "event": "SIGNAL_GENERATED", "zone_id": zone.id, "side": zone.side,
            "spot_price": current_price, "futures_price": futures_price,
            "entry": current_price, "sl": sl_price, "tp1": tp1_price, "tp2": tp2_price, "be": be_price,
            "delta_sum": delta_formatted, "spread_pct": round(spread_pct, 4),
            "exit_mode": exit_mode, "actual_risk_usdt": actual_risk_usdt, "final_qty": final_qty
        }))

        zone.state = "ACTIVE_TRADE"
        self._save_zones()

        return {
            "signal_id": f"AdaptiveZoneV1_{uuid.uuid4().hex[:8]}",
            "symbol": "SOLUSDT",
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
        """V2/Блок4+5: замок R4 → паспорт → ордер с orderLinkId → умный TTL."""
        from execution.passport import TradePassport, SignalSource

        logger.info(f"\n{'='*60} СИГНАЛ: {signal['side']} @ {signal['entry_price']} {'='*60}")

        if not self.order_manager:
            logger.error(" OrderManager не инициализирован!")
            return
        if not self.passport_storage:
            logger.error("❌ PassportStorage не инициализирован!")
            return

        # 🔒 ЗАМОК R4: одна активная сделка за раз
        if self._has_active_trade():
            logger.info(f"🔒 [{zone.id}] LOCK_HELD: есть активная сделка — сигнал отклонён")
            autopsy_logger.info(json.dumps({
                "event": "SIGNAL_REJECTED", "zone_id": zone.id, "side": signal['side'],
                "reason": "LOCK_HELD", "entry": signal['entry_price']
            }))
            zone.state = "DEPARTED"
            self._save_zones()
            return

        # Создаём паспорт
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

        # Отправляем ордер с orderLinkId
        limit_price = str(round(signal['entry_price'] * 0.9998, 2)) if signal['side'] == "LONG" else str(round(signal['entry_price'] * 1.0002, 2))
        
        # ✅ V2/Блок5: Передаем zone_id и используем configurable TTL
        success = await self.order_manager.place_entry_order(
            passport, limit_price, 
            timeout_seconds=self.order_ttl_seconds,  # Было: 30, Стало: 120 (из конфига)
            order_link_id=passport.client_order_id,
            zone_id=zone.id  # ← Передаем ID зоны в OrderManager/TIMER
        )

        if success:
            mgr.set_state("ORDER_SENT", reason=f"limit @ {limit_price}", source="STRATEGY")
            passport.add_event("ACTION: ENTRY_PLACED", f"Лимитный ордер размещен по {limit_price}", source="STRATEGY")
            logger.info(f"✅ [{passport.passport_id}] Ордер размещен (TTL={self.order_ttl_seconds}s, orderLinkId={passport.client_order_id}).")
            self.passport_storage.save(passport)
        else:
            mgr.set_state("ORDER_REJECTED", reason="place_entry_order failed", source="STRATEGY")
            passport.add_event("ERROR: ENTRY_FAILED", "Не удалось разместить ордер", source="STRATEGY")
            logger.warning(f" [{passport.passport_id}] Ордер не размещён → ORDER_REJECTED")
            self.passport_storage.save(passport)