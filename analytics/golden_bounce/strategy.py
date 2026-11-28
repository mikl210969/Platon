"""
Golden Bounce Strategy.
Версия: v3.4 (Шаг 1). Мультивалютная, USDT-ориентированный риск, Swing SL, early_BE, 
инструмент-специфичные фильтры + подготовка к кластерному SL.
"""

import logging
import functools
import math
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional

from core.config import Config
from core.event_bus import EventBus
from analytics.golden_bounce.wall_tracker import WallPersistenceTracker
from analytics.golden_bounce.zone import GBZone, ZoneStatus

logger = logging.getLogger("plato.golden_bounce")


class GoldenBounceStrategy:
    def __init__(
        self,
        event_bus: EventBus,
        order_manager=None,
        passport_storage=None,
        position_monitor=None,
        market_buffer=None,
        config=None,
        symbol: str = "SOLUSDT"
    ):
        self.bus = event_bus
        self.om = order_manager
        self.storage = passport_storage
        self.monitor = position_monitor
        self.market_buffer = market_buffer
        self.symbol = symbol

        self.config = {}
        if config is not None:
            if isinstance(config, dict):
                self.config = config
            elif hasattr(config, 'get'):
                self.config = config
            else:
                self.config = getattr(config, 'data', None) or getattr(config, '_data', None) or getattr(config, 'config', None) or {}

        gb_cfg = self.config.get("trading", {}).get("strategies", {}).get("golden_bounce", {}).get("wall_persistence", {})
        if not gb_cfg:
            gb_cfg = {
                "min_consecutive_ticks": 2, "detect_window_sec": 1.0,
                "same_wall_price_tolerance_ticks": 5, "min_wall_duration_sec": 2.0,
                "half_life_sec": 25, "partial_fill_min_ratio": 0.25,
                "min_timeout_sec": 3, "max_timeout_sec": 8,
                "tape_window_ms": 400, "tape_price_tolerance_ticks": 3,
                "refill_ttl_sec": 60, "refill_boost_per_event": 0.25,
                "refill_boost_max": 2.0, "pull_cycle_demerit": 0.5, "max_pull_cycles": 3
            }

        self.wall_tracker = WallPersistenceTracker(gb_cfg)
        self.active_zones: List[GBZone] = []
        self.atr_multiplier = self.config.get("trading", {}).get("structure_validation", {}).get("min_retracement_atr_multiplier", 1.5)

        self.delta_buffer: List[tuple] = []
        self.delta_window_sec = 60
        self.delta_short_window_sec = 15

        self.bar_history: List[Dict] = []
        self._current_bar_time_ms: int = 0
        
        self.swing_sl_cfg = self.config.get("trading", {}).get("swing_sl", {})
        self.instrument_filters = self.config.get("trading", {}).get("instrument_filters", {})

        # ✅ ШАГ 1: Хранилище для последнего снапшота стакана
        self._latest_ob_snapshot: Optional[Dict[str, Any]] = None

        health = self.config.get("trading", {}).get("strategies", {}).get("golden_bounce", {}).get("health", {})
        self.min_atr_bps = float(health.get("min_atr_bps", 20))
        self.dead_market_hysteresis_sec = float(health.get("dead_market_hysteresis_sec", 600))
        self.zone_dedup_atr_mult = float(health.get("zone_dedup_atr_mult", 1.5))
        self.zone_dedup_min_pct = float(health.get("zone_dedup_min_pct", 0.001))
        self.signal_cooldown_sec = float(health.get("signal_cooldown_sec", 300))
        self.max_active_per_symbol = int(health.get("max_active_per_symbol", 1))
        self.max_passports_per_hour = int(health.get("max_passports_per_hour", 10))
        self.max_zones_per_hour = int(health.get("max_zones_per_hour", 60))
        self.entry_order_ttl_sec = float(health.get("entry_order_ttl_sec", 600))
        self.min_departure_atr_mult = float(health.get("min_departure_atr_mult", 0.8))
        self.sl_buffer_pct = float(health.get("sl_buffer_pct", 0.002))
        self.tp1_fraction = float(health.get("tp1_fraction", 0.5))

        regime_cfg = self.config.get("trading", {}).get("strategies", {}).get("golden_bounce", {}).get("regime_shift", {})
        self.min_atr_ratio = float(regime_cfg.get("min_atr_ratio", 0.3))
        self.max_atr_ratio = float(regime_cfg.get("max_atr_ratio", 3.0))

        risk_cfg = self.config.get("trading", {}).get("strategies", {}).get("golden_bounce", {}).get("risk_management", {})
        self.min_rr_ratio = float(risk_cfg.get("min_rr_ratio", 1.0))
        self.risk_slippage_mult = float(risk_cfg.get("risk_slippage_mult", 1.2))
        self.fee_rate = float(risk_cfg.get("fee_rate", 0.0004))

        self._pending_entries: Dict[str, Any] = {}
        self._atr_below_since: Optional[datetime] = None
        self._last_signal_attempt: Optional[datetime] = None
        self._passport_attempts: List[datetime] = []
        self._zone_creations: List[datetime] = []

        self.bus.subscribe(f"EVENT:TAP:{symbol}", functools.partial(self.wall_tracker.on_trades, symbol=symbol))
        self.bus.subscribe(f"EVENT:TAP:{symbol}", self._on_trade_tape)
        self.bus.subscribe(f"PUBLIC:ORDERBOOK:{symbol}", self._on_orderbook)
        self.bus.subscribe("ORDER:TIMER_CANCELLED", self._on_timer_cancelled)

        logger.info(f"✨ [{self.symbol}] GoldenBounceStrategy initialized (v3.4 Cluster SL + Opposite Cluster Check)")

    def _get_current_session(self) -> str:
        hour = datetime.now(timezone.utc).hour
        if 0 <= hour < 6: return "asian_night"
        if 6 <= hour < 12: return "asian"
        if 12 <= hour < 20: return "european"
        return "american"

    def _get_session_multipliers(self) -> Dict[str, float]:
        session = self._get_current_session()
        defaults = {"k_wall": 2.0, "k_atr_sl_buffer": 1.0, "k_atr_retracement": 1.5, "k_delta": 1.0}
        session_cfg = self.config.get("trading", {}).get("strategies", {}).get("golden_bounce", {}).get("session_multipliers", {})
        return session_cfg.get(session, defaults)

    def _get_current_atr(self) -> float:
        if self.market_buffer is None: return 0.5
        atr_obj = getattr(self.market_buffer, 'atr_5m', None)
        if atr_obj is None: return 0.5
        try:
            atr_value = atr_obj.get_atr()
            if isinstance(atr_value, (int, float)) and atr_value > 0: return float(atr_value)
        except Exception: pass
        return 0.5

    def _current_atr_bps(self, atr: float, price: float) -> float:
        if price <= 0: return 0.0
        return atr / price * 10_000.0

    def _market_is_dead(self, now: datetime, atr: float, price: float) -> bool:
        if price <= 0 or atr <= 0: return True
        if self._current_atr_bps(atr, price) >= self.min_atr_bps:
            self._atr_below_since = None
            return False
        if self._atr_below_since is None: self._atr_below_since = now
        return (now - self._atr_below_since).total_seconds() >= self.dead_market_hysteresis_sec

    def _find_near_zone(self, price: float, radius: float) -> Optional[GBZone]:
        for z in self.active_zones:
            if z.status == ZoneStatus.INVALIDATED: continue
            if abs(z.price - price) <= radius: return z
        return None

    async def _on_timer_cancelled(self, event: Dict[str, Any]):
        try:
            zone_id = event.get("zone_id")
            order_id = event.get("order_id")
            passport = self._pending_entries.get(zone_id) if zone_id else None
            if passport is None and order_id:
                for p in self._pending_entries.values():
                    if any(o.get("order_id") == order_id for o in p.orders):
                        passport = p; break
            if passport is not None and getattr(passport, "cancel_source", None) is None:
                passport.cancel_source = "SELF_TIMER"
                logger.info(f"⏱️ [{self.symbol}] cancel_source=SELF_TIMER для {passport.passport_id}")
        except Exception as e: logger.error(f"[{self.symbol}] Error in _on_timer_cancelled: {e}")

    async def _invalidate_zone(self, zone: GBZone, reason: str):
        if zone.status == ZoneStatus.INVALIDATED: return
        zone.status = ZoneStatus.INVALIDATED
        zone.invalidation_reason = reason
        logger.info(f"⛔ [{self.symbol}] Zone {zone.id} INVALIDATED ({reason})")
        if reason == "WALL_CONSUMED": return

        pending = self._pending_entries.pop(zone.id, None)
        if pending is None or self.om is None: return
        state = await self.om.cancel_entry_order(pending, "SELF_ZONE_INVALIDATION")
        if state == "FILLED": logger.info(f"✅ [{self.symbol}] Гонка cancel-vs-fill: вход по зоне {zone.id} исполнился")
        elif state == "PENDING": logger.warning(f"⚠️ [{self.symbol}] Отмена входа по зоне {zone.id} не подтверждена")

    def _log_autopsy(self, event: str, data: Dict[str, Any]):
        try:
            import json
            from pathlib import Path
            log_file = Path("strategy_autopsy.jsonl")
            record = {"event": event, "symbol": self.symbol, **data}
            with open(log_file, "a", encoding="utf-8") as f: f.write(json.dumps(record, default=str) + "\n")
        except Exception as e: logger.error(f"Ошибка записи autopsy: {e}")

    def _save_active_zones(self):
        try:
            import json
            from pathlib import Path
            data_dir = Path("data"); data_dir.mkdir(parents=True, exist_ok=True)
            zones_file = data_dir / f"active_zones_{self.symbol}.json"
            zones_data = [{
                "id": z.id, "price": z.price, "volume": z.peak_volume,
                "timestamp": z.created_at.isoformat(), "state": z.status.value, "side": z.trade_side,
                "min_price_after": z.extreme_price if z.side == "bid" else None,
                "max_price_after": z.extreme_price if z.side == "ask" else None,
                "cumulative_delta": round(z.cumulative_delta, 2),
                "return_bars": z.calculate_return_bars(),
                "return_delta": round(z.calculate_return_delta(), 2)
            } for z in self.active_zones]
            with open(zones_file, "w", encoding="utf-8") as f: json.dump(zones_data, f, indent=2, ensure_ascii=False, default=str)
        except Exception as e: logger.error(f"Ошибка записи active_zones: {e}")

    def _update_bar_history(self, ts_ms: int, price: float):
        if price <= 0 or ts_ms <= 0:
            return
        
        bar_time_ms = (ts_ms // 300000) * 300000
        
        if not self.bar_history or self.bar_history[-1]["time"] < bar_time_ms:
            self.bar_history.append({
                "time": bar_time_ms,
                "open": price,
                "high": price,
                "low": price,
                "close": price
            })
            self._current_bar_time_ms = bar_time_ms
        else:
            last = self.bar_history[-1]
            last["high"] = max(last["high"], price)
            last["low"] = min(last["low"], price)
            last["close"] = price
        
        if len(self.bar_history) > 200:
            self.bar_history = self.bar_history[-200:]
        
        for zone in self.active_zones:
            if zone.status != ZoneStatus.INVALIDATED:
                zone.bar_history = self.bar_history

    async def _on_trade_tape(self, trades: List[Dict]):
        try:
            now = datetime.now(timezone.utc)
            cutoff = now - timedelta(seconds=self.delta_window_sec)
            cutoff_short = now - timedelta(seconds=self.delta_short_window_sec)

            for t in trades:
                ts_ms = t.get("T", 0)
                if ts_ms <= 0: continue
                trade_ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                side = t.get("S", "")
                qty = float(t.get("v", 0) or t.get("qty", 0) or 0.0)
                price = float(t.get("p", 0) or t.get("price", 0) or 0.0)
                
                if side == "Buy": signed_delta = qty
                elif side == "Sell": signed_delta = -qty
                else: continue

                self._update_bar_history(ts_ms, price)

                self.delta_buffer.append((trade_ts, signed_delta))
                for zone in self.active_zones:
                    if zone.status != ZoneStatus.INVALIDATED:
                        zone.update_cumulative_delta(signed_delta)
                        if zone.status in (ZoneStatus.DEPARTED, ZoneStatus.RETURNING):
                            zone.delta_pullback += signed_delta

            self.delta_buffer = [(ts, d) for ts, d in self.delta_buffer if ts >= cutoff]

            for zone in self.active_zones:
                if zone.status not in (ZoneStatus.DEPARTED, ZoneStatus.RETURNING): continue
                net_60 = sum(d for _, d in self.delta_buffer)
                gross_60 = sum(abs(d) for _, d in self.delta_buffer)
                net_15 = sum(d for ts, d in self.delta_buffer if ts >= cutoff_short)
                zone.update_delta_metrics(net_60, net_15, gross_60)
        except Exception as e:
            logger.error(f"[{self.symbol}] Error in _on_trade_tape: {e}", exc_info=True)

    async def _on_orderbook(self, snapshot: Dict[str, Any]):
        try:
            # ✅ РАЗЛИЧАЕМ snapshot и delta от Bybit WebSocket
            msg_type = snapshot.get("type", "delta")
            
            if msg_type == "snapshot":
                # Полный снапшот — сохраняем целиком
                self._latest_ob_snapshot = snapshot
            else:
                # Дельта-обновление — обновляем уровни в существующем снапшоте
                if self._latest_ob_snapshot is None:
                    self._latest_ob_snapshot = snapshot
                else:
                    # Bybit в дельте присылает ПОЛНЫЙ срез измененной стороны,
                    # поэтому просто заменяем ключи a/b
                    data = snapshot.get("data", {})
                    existing_data = self._latest_ob_snapshot.get("data", {})
                    
                    if "a" in data:
                        existing_data["a"] = data["a"]
                    if "b" in data:
                        existing_data["b"] = data["b"]
            
            now = datetime.now(timezone.utc)
            data = self._latest_ob_snapshot.get("data", {})
            bids = data.get("b", [])
            asks = data.get("a", [])

            current_price = None
            if bids and asks: current_price = (float(bids[0][0]) + float(asks[0][0])) / 2
            elif bids: current_price = float(bids[0][0])
            elif asks: current_price = float(asks[0][0])

            if current_price is not None: await self._check_zones_departure(current_price, now)

            instr_cfg = self.config.get("instruments", {}).get(self.symbol, {})
            base_wall_usd = instr_cfg.get("min_wall_value_usd", 50000)
            session_mult = self._get_session_multipliers()
            k_wall = session_mult.get("k_wall", 2.0)
            min_wall_usd = base_wall_usd / k_wall  # ✅ Сессионная подстройка: ночь → порог ниже

            if bids:
                best_bid_price, best_bid_vol = float(bids[0][0]), float(bids[0][1])
                if best_bid_price * best_bid_vol >= min_wall_usd:
                    event = await self.wall_tracker.update_ob(self.symbol, best_bid_price, best_bid_vol, "bid", now)
                    if event: await self._handle_wall_event(event)
            if asks:
                best_ask_price, best_ask_vol = float(asks[0][0]), float(asks[0][1])
                if best_ask_price * best_ask_vol >= min_wall_usd:
                    event = await self.wall_tracker.update_ob(self.symbol, best_ask_price, best_ask_vol, "ask", now)
                    if event: await self._handle_wall_event(event)
        except Exception as e: logger.error(f"[{self.symbol}] Error in _on_orderbook: {e}", exc_info=True)

    async def _check_zones_departure(self, current_price: float, now: datetime):
        multipliers = self._get_session_multipliers()
        current_atr = self._get_current_atr()
        k_retracement = multipliers.get("k_atr_retracement", 1.5)

        for zone in list(self.active_zones):
            if current_atr > 0 and zone.atr_at_creation > 0:
                ratio = current_atr / zone.atr_at_creation
                if ratio < self.min_atr_ratio or ratio > self.max_atr_ratio:
                    await self._invalidate_zone(zone, "REGIME_SHIFT")
                    continue

            if zone.is_expired(now):
                await self._invalidate_zone(zone, "EXPIRED")
                continue

            if zone.status == ZoneStatus.CONFIRMED:
                if zone.check_departure(current_price, k_retracement, self.min_departure_atr_mult):
                    dept = abs(zone.price - (zone.extreme_price or zone.price))
                    logger.info(f"📉 [{self.symbol}] ZONE DEPARTED: {zone.id} ({zone.trade_side}) @ {zone.price} | Extreme: {zone.extreme_price} | Dept: {dept:.2f}")

            elif zone.status == ZoneStatus.DEPARTED:
                if zone.is_return_expired(now):
                    await self._invalidate_zone(zone, "RETURN_TIMEOUT")
                else:
                    # ✅ Передаем tick_size для расчета адаптивного коридора
                    tick_size_map = {"SOLUSDT": 0.01, "BTCUSDT": 0.1, "ETHUSDT": 0.01, "LINKUSDT": 0.001, "BNBUSDT": 0.1, "AVAXUSDT": 0.01, "XRPUSDT": 0.0001}
                    tick_size = tick_size_map.get(self.symbol, 0.01)
                    
                    if zone.check_return(current_price, tick_size):
                        return_bars = zone.calculate_return_bars()
                        logger.info(f"🔄 [{self.symbol}] ZONE RETURNING: {zone.id} ({zone.trade_side}) @ {zone.price} | "
                                  f"return_bars={return_bars} | CumDelta: {zone.cumulative_delta:.2f}")
                        await self._evaluate_signal(zone, current_price)
                        zone.status = ZoneStatus.SIGNAL_READY

        self.active_zones = [z for z in self.active_zones if z.status != ZoneStatus.INVALIDATED]

        try:
            from execution.passport_manager import is_terminal
            for zid in [k for k, p in self._pending_entries.items() if is_terminal(getattr(p, "status", "CLOSED"))]:
                del self._pending_entries[zid]
        except Exception: pass

        self._save_active_zones()

    def _check_instrument_filter(self, zone: GBZone) -> bool:
        instr_filter = self.instrument_filters.get(self.symbol)
        if not instr_filter:
            return True
        
        filter_type = instr_filter.get("type")
        threshold = instr_filter.get("threshold", 0)
        
        if filter_type == "return_bars_max":
            return_bars = zone.calculate_return_bars()
            if return_bars is None:
                self._log_autopsy("SIGNAL_REJECTED", {"zone_id": zone.id, "reason": "return_bars_unknown"})
                return False
            if return_bars > threshold:
                self._log_autopsy("SIGNAL_REJECTED", {
                    "zone_id": zone.id, "reason": "return_bars_filter", "return_bars": return_bars,
                    "threshold": threshold, "filter": f"{self.symbol}:{filter_type}"
                })
                logger.info(f"⏱️ [{self.symbol}] RETURN_BARS FILTER: {return_bars} > {threshold}, reject")
                return False
            logger.info(f"✅ [{self.symbol}] RETURN_BARS FILTER PASSED: {return_bars} <= {threshold}")
            return True
        
        elif filter_type == "delta_agree":
            return_delta = zone.calculate_return_delta()
            if zone.trade_side == "LONG":
                if return_delta <= threshold:
                    self._log_autopsy("SIGNAL_REJECTED", {
                        "zone_id": zone.id, "reason": "delta_agree_filter", "return_delta": return_delta,
                        "threshold": threshold, "side": "LONG", "filter": f"{self.symbol}:{filter_type}"
                    })
                    logger.info(f"📉 [{self.symbol}] DELTA_AGREE FILTER (LONG): delta={return_delta:.2f} <= {threshold}, reject")
                    return False
            else:
                if return_delta >= -threshold:
                    self._log_autopsy("SIGNAL_REJECTED", {
                        "zone_id": zone.id, "reason": "delta_agree_filter", "return_delta": return_delta,
                        "threshold": threshold, "side": "SHORT", "filter": f"{self.symbol}:{filter_type}"
                    })
                    logger.info(f"📈 [{self.symbol}] DELTA_AGREE FILTER (SHORT): delta={return_delta:.2f} >= {-threshold}, reject")
                    return False
            logger.info(f"✅ [{self.symbol}] DELTA_AGREE FILTER PASSED: delta={return_delta:.2f}")
            return True
        
        return True

    async def _evaluate_signal(self, zone: GBZone, current_price: float):
        now = datetime.now(timezone.utc)

        if self._last_signal_attempt is not None and (now - self._last_signal_attempt).total_seconds() < self.signal_cooldown_sec:
            self._log_autopsy("SIGNAL_REJECTED", {"zone_id": zone.id, "reason": "cooldown"})
            return

        if self.monitor is not None and self.monitor.has_active_position(self.symbol):
            self._log_autopsy("SIGNAL_REJECTED", {"zone_id": zone.id, "reason": "position_exists_on_exchange"})
            return

        if self.monitor is not None and self.max_active_per_symbol > 0:
            from execution.passport_manager import is_terminal
            active = sum(1 for p in getattr(self.monitor, "active_passports", []) if getattr(p, "symbol", None) == self.symbol and not is_terminal(getattr(p, "status", "CLOSED")))
            if active >= self.max_active_per_symbol:
                self._log_autopsy("SIGNAL_REJECTED", {"zone_id": zone.id, "reason": "soft_guard"})
                return

        wall_status = self._get_wall_status(zone)
        if wall_status in ("GONE", "CONSUMED"):
            await self._invalidate_zone(zone, "WALL_GONE" if wall_status == "GONE" else "WALL_CONSUMED")
            return

        if not self._check_instrument_filter(zone):
            return

        current_atr = self._get_current_atr()
        if current_atr <= 0: current_atr = 0.5
        
        delta_bounce = abs(zone.delta_bounce_snapshot)
        delta_return = abs(zone.cumulative_delta - zone.delta_bounce_snapshot)
        
        extreme = zone.extreme_price if zone.extreme_price else zone.price
        departure_usd = abs(extreme - zone.price)
        departure_atr_mult = departure_usd / current_atr
        
        min_absolute_delta = max(10.0, current_atr * 0.05)
        
        if delta_bounce < min_absolute_delta and departure_atr_mult < 2.0:
            self._log_autopsy("SIGNAL_REJECTED", {"zone_id": zone.id, "reason": "weak_bounce_absolute", "delta_bounce": delta_bounce, "departure_atr_mult": departure_atr_mult})
            return

        delta_bounce_usd = delta_bounce * zone.price
        normalized_bounce = delta_bounce_usd / current_atr
        
        if normalized_bounce < 0.5:
            self._log_autopsy("SIGNAL_REJECTED", {"zone_id": zone.id, "reason": "weak_bounce_normalized", "norm_delta": normalized_bounce})
            return

        min_dar_ratio = 1.8
        ratio = delta_bounce / max(delta_return, 1.0)
        
        if ratio < min_dar_ratio:
            self._log_autopsy("SIGNAL_REJECTED", {"zone_id": zone.id, "reason": "aggressive_return", "dar_ratio": round(ratio, 2), "delta_return": delta_return})
            return
            
        if delta_return < 10.0:
            logger.info(f"🎯 [{self.symbol}] Zone {zone.id} ИДЕАЛЬНЫЙ РЕТЕСТ: delta_return={delta_return:.1f}")
            
        logger.info(f"✅ [{self.symbol}] Zone {zone.id} ФИЛЬТР ДЕЛЬТЫ ПРОЙДЕН: bounce={delta_bounce:.1f}, return={delta_return:.1f}, DAR={ratio:.2f}")

        if self._latest_ob_snapshot:
            zone.set_orderbook_snapshot(self._latest_ob_snapshot)
        else:
            logger.warning(f"⚠️ [{self.symbol}] Нет актуального снапшота стакана для зоны {zone.id}. Кластерный SL будет недоступен.")

        tick_size_map = {"SOLUSDT": 0.01, "BTCUSDT": 0.1, "ETHUSDT": 0.01, "LINKUSDT": 0.001, "BNBUSDT": 0.1, "AVAXUSDT": 0.01, "XRPUSDT": 0.0001}
        tick_size = tick_size_map.get(self.symbol, 0.01)
        offset_ticks = self.config.get("trading", {}).get("strategies", {}).get("golden_bounce", {}).get("entry_offset_ticks", 1)

        if zone.trade_side == "LONG":
            entry_price = zone.price + (tick_size * offset_ticks)
        else:
            entry_price = zone.price - (tick_size * offset_ticks)

        # ✅ ШАГ 2: Попытка найти кластерный SL
        cluster_cfg = self.config.get("trading", {}).get("cluster_search", {})
        instr_cfg = self.config.get("instruments", {}).get(self.symbol, {})
        cluster_cfg["min_cluster_volume_usd"] = instr_cfg.get("min_cluster_volume_usd", 20000)
        
        cluster_boundary_price = zone.find_cluster_boundary(tick_size, cluster_cfg)
        
        zone.bar_history = self.bar_history
        
        # ✅ ШАГ 3: Передаём найденную границу и конфиг кластерного SL в расчёт уровней
        cluster_sl_cfg = self.config.get("trading", {}).get("cluster_sl", {})
        
        levels = zone.calculate_sl_tp(
            entry_price, 
            self.sl_buffer_pct, 
            self.tp1_fraction,
            swing_sl_cfg=self.swing_sl_cfg,
            atr=current_atr,
            cluster_boundary_price=cluster_boundary_price,
            tick_size=tick_size,
            cluster_sl_cfg=cluster_sl_cfg,
            cluster_search_cfg=cluster_cfg
        )
        if levels is None:
            # ✅ ШАГ 7: используем точную причину отклонения из зоны
            rejection_reason = getattr(zone, "last_rejection_reason", None) or "invalid_risk"
            self._log_autopsy("SIGNAL_REJECTED", {
                "zone_id": zone.id,
                "reason": rejection_reason,
                "sl_source_attempted": "cluster" if cluster_boundary_price else "swing"
            })
            zone.last_rejection_reason = None  # сброс
            return

        sl_dist = abs(levels["entry"] - levels["sl"])
        tp2_dist = abs(levels["tp2"] - levels["entry"])

        if sl_dist <= 0:
            self._log_autopsy("SIGNAL_REJECTED", {"zone_id": zone.id, "reason": "zero_sl_distance"})
            return

        rr_ratio = tp2_dist / sl_dist
        if rr_ratio < self.min_rr_ratio:
            self._log_autopsy("SIGNAL_REJECTED", {"zone_id": zone.id, "reason": "poor_RR_ratio", "rr_ratio": round(rr_ratio, 3)})
            return

        logger.info(f"✅ [{self.symbol}] Zone {zone.id} ПРОШЛА ВСЕ ФИЛЬТРЫ! Создаем паспорт...")
        self._last_signal_attempt = now
        self._passport_attempts.append(now)
        self._passport_attempts = [t for t in self._passport_attempts if (now - t).total_seconds() < 3600]
        
        await self._create_passport(zone, levels, False)

    def _get_wall_status(self, zone: GBZone) -> str: return zone.wall_status

    async def _create_passport(self, zone: GBZone, levels: dict, is_exhaustion: bool):
        try:
            from execution.passport import TradePassport, SignalSource

            risk_usd = self.config.get("trading", {}).get("risk", {}).get("base_risk_usdt", 30.0)
            max_notional_usd = self.config.get("trading", {}).get("risk", {}).get("max_notional_usd", 50000.0)
            min_notional_usd = self.config.get("trading", {}).get("risk", {}).get("min_notional_usd", 100.0)

            entry_price = levels["entry"]
            risk_per_unit = levels["risk_per_unit"]

            if risk_per_unit <= 0 or entry_price <= 0:
                logger.error(f"🚫 [{self.symbol}] Zero risk_per_unit or entry_price. Aborting.")
                return

            fee_per_unit = entry_price * self.fee_rate * 1.5
            total_risk_per_unit = risk_per_unit + fee_per_unit

            if total_risk_per_unit <= 0:
                logger.error(f"🚫 [{self.symbol}] total_risk_per_unit <= 0. Aborting.")
                return

            ideal_size = risk_usd / total_risk_per_unit
            ideal_notional = ideal_size * entry_price
            final_notional = max(min_notional_usd, min(ideal_notional, max_notional_usd))
            size = final_notional / entry_price

            actual_risk = size * total_risk_per_unit
            risk_limit = risk_usd * self.risk_slippage_mult

            if actual_risk > risk_limit:
                self._log_autopsy("SIGNAL_REJECTED", {"zone_id": zone.id, "reason": "risk_limit_exceeded", "actual_risk": round(actual_risk, 2)})
                return

            logger.info(f"📐 [{self.symbol}] USDT Sizing: risk=${risk_usd:.2f}, notional=${final_notional:.2f} → size={size:.4f}")

            gross_profit_tp2 = size * abs(levels["tp2"] - entry_price)
            est_commission = final_notional * self.fee_rate * 2
            if gross_profit_tp2 < (est_commission * 1.5):
                self._log_autopsy("SIGNAL_REJECTED", {"zone_id": zone.id, "reason": "positive_ev_failed"})
                return

            early_be_fraction = self.swing_sl_cfg.get("early_be_fraction", 0.5)
            tp1_distance = abs(levels["tp1"] - entry_price)
            be_offset = tp1_distance * early_be_fraction

            min_be_offset = (est_commission * 1.2) / size if size > 0 else 0
            if be_offset < min_be_offset:
                be_offset = min_be_offset

            be_price = entry_price + be_offset if zone.trade_side == "LONG" else entry_price - be_offset
            logger.info(f"🛡️ [{self.symbol}] early_BE: {be_price:.4f} (offset={be_offset:.4f}, {early_be_fraction*100:.0f}% to TP1)")

            passport = TradePassport(
                signal_id=zone.id, strategy="GoldenBounce", signal_source=SignalSource.TEST,
                symbol=self.symbol, side=zone.trade_side, entry_price=entry_price,
                sl_price=levels["sl"], tp1_price=levels["tp1"], tp2_price=levels["tp2"],
                be_price=be_price, planned_size=size, position_size=size,
                target_risk_usdt=risk_usd, status="IDLE"
            )

            tick_size_map = {"SOLUSDT": 0.01, "BTCUSDT": 0.1, "ETHUSDT": 0.01, "LINKUSDT": 0.001, "BNBUSDT": 0.1, "AVAXUSDT": 0.01, "XRPUSDT": 0.0001}
            tick_size = tick_size_map.get(self.symbol, 0.01)
            offset_ticks = self.config.get("trading", {}).get("strategies", {}).get("golden_bounce", {}).get("entry_offset_ticks", 1)

            limit_price = zone.price + (tick_size * offset_ticks) if zone.trade_side == "LONG" else zone.price - (tick_size * offset_ticks)
            limit_price = round(limit_price / tick_size) * tick_size

            rr_ratio = abs(levels["tp2"] - entry_price) / risk_per_unit if risk_per_unit > 0 else 0
            logger.info(f"🎯 [{self.symbol}] SIGNAL GENERATED: {zone.id} | {zone.trade_side} @ {entry_price:.2f} | SL: {levels['sl']:.2f} | TP1: {levels['tp1']:.2f} | TP2: {levels['tp2']:.2f} | R:R={rr_ratio:.2f}")

            # ✅ ШАГ 7: расширенная телеметрия по кластерному SL и встречному кластеру
            autopsy_record = {
                "zone_id": zone.id, "side": zone.trade_side, "wall_price": zone.price, "limit_price": limit_price,
                "sl": levels["sl"], "tp1": levels["tp1"], "tp2": levels["tp2"], "risk_per_unit": risk_per_unit,
                "size": size, "notional_usd": round(final_notional, 2), "be_price": be_price,
                "cumulative_delta": zone.cumulative_delta, "rr_ratio": round(rr_ratio, 3), "actual_risk_usd": round(actual_risk, 2),
                "return_bars": zone.calculate_return_bars(), "return_delta": round(zone.calculate_return_delta(), 2),
                "swing_sl_enabled": self.swing_sl_cfg.get("enabled", False),
                # Кластерная телеметрия
                "sl_source": levels.get("sl_source", "unknown"),
                "cluster_sl_shadow": levels.get("cluster_sl_shadow"),
                "cluster_boundary_price": levels.get("cluster_boundary_price"),
                "opposite_cluster_found": levels.get("opposite_cluster_found", False),
                "opposite_cluster_price": levels.get("opposite_cluster_price"),
                "tp2_shifted": levels.get("tp2_shifted", False),
            }
            self._log_autopsy("SIGNAL_GENERATED", autopsy_record)

            await self.bus.publish("EXEC:NEW_TRADE", passport)
            logger.info(f"📋 [{self.symbol}] Passport registered in PositionMonitor (EXEC:NEW_TRADE)")

            if self.om:
                try:
                    await self.om.place_entry_order(passport, limit_price=str(limit_price), timeout_seconds=int(self.entry_order_ttl_sec), zone_id=zone.id)
                    logger.info(f"✅ [{self.symbol}] Passport handed over to OrderManager successfully.")
                    self._pending_entries[zone.id] = passport
                except Exception as om_error:
                    logger.error(f"❌ [{self.symbol}] OrderManager rejected passport. Reason: {om_error}", exc_info=True)
            else:
                logger.warning(f"[{self.symbol}] OrderManager not attached! Paper trade only.")

        except Exception as e:
            logger.error(f"🔥 [{self.symbol}] Critical error in _create_passport: {e}", exc_info=True)

    async def _handle_wall_event(self, event: Dict[str, Any]):
        evt_type = event.get("event")
        price = event.get("price")
        side = event.get("side")
        symbol = event.get("symbol", self.symbol)

        if price is None or side is None: return
        logger.info(f"🧱 [{symbol}] WALL EVENT: {evt_type} @ {price} ({side})")

        if evt_type == "WALL_CONFIRMED":
            now = datetime.now(timezone.utc)
            price_f = float(price)
            
            instr_cfg = self.config.get("instruments", {}).get(symbol, {}) if isinstance(self.config, dict) else getattr(self.config, 'get', lambda k, d: {})("instruments", {}).get(symbol, {})
            base_wall_usd = instr_cfg.get("min_wall_value_usd", 50000) if instr_cfg else 50000
            session_mult = self._get_session_multipliers()
            k_wall = session_mult.get("k_wall", 2.0)
            min_wall_usd = base_wall_usd / k_wall  # ✅ Сессионная подстройка
            
            peak_vol = float(event.get("peak_volume", 0))
            wall_value_usd = price_f * peak_vol
            
            if wall_value_usd < min_wall_usd:
                self._log_autopsy("ZONE_REJECTED", {"price": price_f, "reason": "wall_value_too_low", "value": wall_value_usd})
                return

            atr = self._get_current_atr()
            if self._market_is_dead(now, atr, price_f):
                self._log_autopsy("ZONE_REJECTED", {"price": price_f, "reason": "dead_market"})
                return

            radius = max(self.zone_dedup_min_pct * price_f, self.zone_dedup_atr_mult * atr)
            near = self._find_near_zone(price_f, radius)
            if near is not None:
                near.peak_volume = max(near.peak_volume, peak_vol)
                self._log_autopsy("ZONE_REJECTED", {"price": price_f, "reason": "dedup"})
                return

            zone = GBZone(symbol=symbol, price=price_f, side=str(side), created_at=now, atr_at_creation=atr, peak_volume=peak_vol, score_multiplier=1.0)
            zone.bar_history = self.bar_history
            zone.departure_threshold = zone.calculate_departure_threshold(self.atr_multiplier)
            self.active_zones.append(zone)

            self._zone_creations.append(now)
            self._zone_creations = [t for t in self._zone_creations if (now - t).total_seconds() < 3600]
            
            logger.info(f"✅ [{symbol}] ZONE CREATED: {zone.id} ({zone.trade_side}) @ {price} | Peak Vol: {zone.peak_volume:.0f} | ATR: {atr:.3f} | Bars: {len(self.bar_history)}")
            self._log_autopsy("ZONE_CREATED", {"zone_id": zone.id, "price": price_f, "volume": zone.peak_volume, "side": zone.trade_side, "atr": atr, "bars_available": len(self.bar_history)})
            self._save_active_zones()

        elif evt_type == "WALL_PARTIALLY_FILLED":
            for z in self.active_zones:
                if z.price == price and z.side == side and z.status != ZoneStatus.INVALIDATED:
                    z.wall_status = "PARTIALLY_FILLED"; break
        elif evt_type == "WALL_GONE_PULL_EXCEEDED":
            for z in self.active_zones:
                if z.price == price and z.side == side:
                    z.wall_status = "GONE"; await self._invalidate_zone(z, "WALL_GONE"); break
        elif evt_type == "WALL_CONSUMED":
            for z in self.active_zones:
                if z.price == price and z.side == side:
                    z.wall_status = "CONSUMED"; await self._invalidate_zone(z, "WALL_CONSUMED"); break