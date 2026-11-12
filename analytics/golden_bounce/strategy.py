"""
Golden Bounce Strategy. 
Версия: Golden Bounce & Retest (PF 7.06). Кумулятивная дельта, инвертированная логика поглощения, 48ч TTL.
v2.1: Усиленный риск-менеджмент (учет комиссий), фильтр R:R, логирование sizing.
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

        raw_config = {}
        if config is not None:
            if isinstance(config, dict):
                raw_config = config
            else:
                raw_config = getattr(config, 'data', None) or getattr(config, '_data', None) or {}
        self.config = raw_config

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

        health = (self.config.get("trading", {}).get("strategies", {})
                  .get("golden_bounce", {}).get("health", {}))
        self.min_atr_bps = float(health.get("min_atr_bps", 20))
        self.dead_market_hysteresis_sec = float(health.get("dead_market_hysteresis_sec", 600))
        self.zone_dedup_atr_mult = float(health.get("zone_dedup_atr_mult", 1.5))
        self.zone_dedup_min_pct = float(health.get("zone_dedup_min_pct", 0.001))
        self.signal_cooldown_sec = float(health.get("signal_cooldown_sec", 300))
        self.max_active_per_symbol = int(health.get("max_active_per_symbol", 1))
        self.max_passports_per_hour = int(health.get("max_passports_per_hour", 10))
        self.max_zones_per_hour = int(health.get("max_zones_per_hour", 60))
        self.entry_order_ttl_sec = float(health.get("entry_order_ttl_sec", 600))
        
        regime_cfg = self.config.get("trading", {}).get("strategies", {}).get("golden_bounce", {}).get("regime_shift", {})
        self.min_atr_ratio = float(regime_cfg.get("min_atr_ratio", 0.3))
        self.max_atr_ratio = float(regime_cfg.get("max_atr_ratio", 3.0))

        # ✅ v2.1: Параметры риск-менеджмента из конфига
        risk_cfg = self.config.get("trading", {}).get("strategies", {}).get("golden_bounce", {}).get("risk_management", {})
        self.min_rr_ratio = float(risk_cfg.get("min_rr_ratio", 1.2))  # TP2_dist / SL_dist минимум
        self.risk_slippage_mult = float(risk_cfg.get("risk_slippage_mult", 1.2))  # Допуск превышения риска
        self.fee_rate = float(risk_cfg.get("fee_rate", 0.0004))  # Taker комиссия

        self._pending_entries: Dict[str, Any] = {}
        self._atr_below_since: Optional[datetime] = None
        self._last_signal_attempt: Optional[datetime] = None
        self._passport_attempts: List[datetime] = []
        self._zone_creations: List[datetime] = []        

        self.bus.subscribe(f"EVENT:TAP:{symbol}", functools.partial(self.wall_tracker.on_trades, symbol=symbol))
        self.bus.subscribe(f"EVENT:TAP:{symbol}", self._on_trade_tape)
        self.bus.subscribe(f"PUBLIC:ORDERBOOK:{symbol}", self._on_orderbook)
        self.bus.subscribe("ORDER:TIMER_CANCELLED", self._on_timer_cancelled)

        logger.info(f"✨ [{self.symbol}] GoldenBounceStrategy initialized (Bounce & Retest | Cumulative Delta | v2.1 R:R filter)")

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
            zones_data = []
            for z in self.active_zones:
                zones_data.append({
                    "id": z.id, "price": z.price, "volume": z.peak_volume,
                    "timestamp": z.created_at.isoformat(), "state": z.status.value, "side": z.trade_side,
                    "min_price_after": z.extreme_price if z.side == "bid" else None,
                    "max_price_after": z.extreme_price if z.side == "ask" else None,
                    "cumulative_delta": round(z.cumulative_delta, 2)
                })
            with open(zones_file, "w", encoding="utf-8") as f: json.dump(zones_data, f, indent=2, ensure_ascii=False, default=str)
        except Exception as e: logger.error(f"Ошибка записи active_zones: {e}")

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
                if side == "Buy": signed_delta = qty
                elif side == "Sell": signed_delta = -qty
                else: continue
                
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
            now = datetime.now(timezone.utc)
            data = snapshot.get("data", {})
            bids = data.get("b", [])
            asks = data.get("a", [])

            current_price = None
            if bids and asks: current_price = (float(bids[0][0]) + float(asks[0][0])) / 2
            elif bids: current_price = float(bids[0][0])
            elif asks: current_price = float(asks[0][0])

            if current_price is not None: await self._check_zones_departure(current_price, now)

            instr_cfg = self.config.get("instruments", {}).get(self.symbol, {})
            min_wall_usd = instr_cfg.get("min_wall_value_usd", 50000)

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

        for zone in self.active_zones:
            if current_atr > 0 and zone.atr_at_creation > 0:
                ratio = current_atr / zone.atr_at_creation
                if ratio < self.min_atr_ratio or ratio > self.max_atr_ratio:
                    await self._invalidate_zone(zone, "REGIME_SHIFT")
                    continue

            if zone.is_expired(now):
                await self._invalidate_zone(zone, "EXPIRED")
                continue

            if zone.status == ZoneStatus.CONFIRMED:
                if zone.check_departure(current_price, k_retracement):
                    logger.info(f"📉 [{self.symbol}] ZONE DEPARTED: {zone.id} ({zone.trade_side}) @ {zone.price} | Extreme: {zone.extreme_price}")

            elif zone.status == ZoneStatus.DEPARTED:
                if zone.is_return_expired(now):
                    await self._invalidate_zone(zone, "RETURN_TIMEOUT")
                elif zone.check_return(current_price):
                    logger.info(f"🔄 [{self.symbol}] ZONE RETURNING: {zone.id} ({zone.trade_side}) @ {zone.price} | CumDelta: {zone.cumulative_delta:.2f}")
                    await self._evaluate_signal(zone, current_price)
                    zone.status = ZoneStatus.SIGNAL_READY
        
        self.active_zones = [z for z in self.active_zones if z.status != ZoneStatus.INVALIDATED]

        try:
            from execution.passport_manager import is_terminal
            for zid in [k for k, p in self._pending_entries.items() if is_terminal(getattr(p, "status", "CLOSED"))]:
                del self._pending_entries[zid]
        except Exception: pass
        
        self._save_active_zones()

    async def _evaluate_signal(self, zone: GBZone, current_price: float):
        now = datetime.now(timezone.utc)

        if self._last_signal_attempt is not None:
            since = (now - self._last_signal_attempt).total_seconds()
            if since < self.signal_cooldown_sec:
                self._log_autopsy("SIGNAL_REJECTED", {"zone_id": zone.id, "reason": "cooldown"})
                return

        # ✅ ПУТЬ А: Проверка наличия активной позиции ДО создания паспорта (Fail Fast)
        if self.monitor is not None and self.monitor.has_active_position(self.symbol):
            self._log_autopsy("SIGNAL_REJECTED", {"zone_id": zone.id, "reason": "position_exists_on_exchange"})
            logger.info(f"⏳ [{self.symbol}] Сигнал отклонен: уже есть активная позиция по {self.symbol}")
            return

        # Дополнительная проверка на лимит активных паспортов (на всякий случай)
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

        # === ЕДИНСТВЕННЫЙ ФИЛЬТР: КУМУЛЯТИВНАЯ ДЕЛЬТА ===
        delta_direction_ok = (zone.side == "bid" and zone.cumulative_delta < 0) or \
                             (zone.side == "ask" and zone.cumulative_delta > 0)
        
        if not delta_direction_ok:
            logger.info(f"⏳ [{self.symbol}] Zone {zone.id} wrong cumulative delta direction (CumDelta: {zone.cumulative_delta:.2f})")
            return

        levels = zone.calculate_sl_tp(current_price)
        if levels is None:
            self._log_autopsy("SIGNAL_REJECTED", {"zone_id": zone.id, "reason": "invalid_risk"})
            return

        # ✅ v2.1: ФИЛЬТР R:R — расстояние до TP2 должно быть минимум min_rr_ratio × расстояние до SL
        sl_dist = abs(levels["entry"] - levels["sl"])
        tp2_dist = abs(levels["tp2"] - levels["entry"])
        
        if sl_dist <= 0:
            self._log_autopsy("SIGNAL_REJECTED", {"zone_id": zone.id, "reason": "zero_sl_distance"})
            return
        
        rr_ratio = tp2_dist / sl_dist
        if rr_ratio < self.min_rr_ratio:
            logger.info(
                f"🚫 [{self.symbol}] Отказ по R:R: TP2_dist ({tp2_dist:.2f}) < "
                f"{self.min_rr_ratio} × SL_dist ({sl_dist:.2f}). R:R = {rr_ratio:.2f}"
            )
            self._log_autopsy("SIGNAL_REJECTED", {
                "zone_id": zone.id, 
                "reason": "poor_RR_ratio",
                "rr_ratio": round(rr_ratio, 3),
                "sl_dist": round(sl_dist, 4),
                "tp2_dist": round(tp2_dist, 4)
            })
            return

        self._last_signal_attempt = now
        self._passport_attempts.append(now)
        self._passport_attempts = [t for t in self._passport_attempts if (now - t).total_seconds() < 3600]
        if len(self._passport_attempts) > self.max_passports_per_hour:
            logger.warning(f"🚨 [{self.symbol}] HEALTH ALERT: passports_per_hour={len(self._passport_attempts)}")

        await self._create_passport(zone, levels, False)

    def _get_wall_status(self, zone: GBZone) -> str: return zone.wall_status

    async def _create_passport(self, zone: GBZone, levels: dict, is_exhaustion: bool):
        try:
            from execution.passport import TradePassport, SignalSource
            
            risk_usd = self.config.get("trading", {}).get("risk", {}).get("base_risk_usdt", 30.0)
            max_size = self.config.get("trading", {}).get("risk", {}).get("max_position_size", 10.0)
            min_size = self.config.get("trading", {}).get("risk", {}).get("min_position_size", 0.1)
            
            risk_per_unit = levels.get("risk_per_unit", 0.0)
            
            if risk_per_unit <= 0:
                logger.error(f"🚫 [{self.symbol}] Zero risk_per_unit. Aborting.")
                return

            # ✅ v2.1: УСИЛЕННАЯ ПРОВЕРКА РИСКА С УЧЕТОМ КОМИССИЙ
            # 1. Идеальный размер под целевой риск
            ideal_size = risk_usd / risk_per_unit
            
            # 2. Применяем лимиты
            final_size = max(min_size, min(ideal_size, max_size))
            
            # 3. Оцениваем реальные комиссии (вход + выход полным объемом по худшему сценарию SL)
            notional = levels["entry"] * final_size
            fees_estimate = notional * self.fee_rate * 2.5  # вход + выход + запас на проскальзывание
            
            # 4. Реальный долларовый риск = риск по цене + комиссии
            sl_dist_usd = abs(levels["entry"] - levels["sl"])
            # ✅ ФИКС ОШИБКИ Pylance: пересчитываем rr_ratio внутри метода (sl_dist_usd > 0 гарантирован выше)
            tp2_dist = abs(levels["tp2"] - levels["entry"])
            rr_ratio = (tp2_dist / sl_dist_usd) if sl_dist_usd > 0 else 0.0
            actual_risk = (sl_dist_usd * final_size) + fees_estimate
            
            # 5. Жесткая проверка: если риск превышает лимит с допуском — ОТКАЗ
            risk_limit = risk_usd * self.risk_slippage_mult
            if actual_risk > risk_limit:
                logger.warning(
                    f"🚫 [{self.symbol}] Отказ от сделки: риск {actual_risk:.2f}$ > лимит {risk_limit:.2f}$ "
                    f"(ideal={ideal_size:.4f}, max={max_size}, final={final_size:.4f}, "
                    f"sl_dist={sl_dist_usd:.2f}, fees={fees_estimate:.2f})"
                )
                self._log_autopsy("SIGNAL_REJECTED", {
                    "zone_id": zone.id, 
                    "reason": "risk_limit_exceeded",
                    "actual_risk": round(actual_risk, 2),
                    "risk_limit": round(risk_limit, 2),
                    "ideal_size": round(ideal_size, 4),
                    "final_size": round(final_size, 4)
                })
                return

            size = final_size
            
            # Логирование sizing для аудита
            logger.info(
                f"📐 [{self.symbol}] Sizing: ideal={ideal_size:.4f} → final={final_size:.4f} "
                f"(capped={final_size != ideal_size}, risk=${actual_risk:.2f} / ${risk_usd:.0f})"
            )

            # ✅ v2.1: Исправленный расчет комиссий — выход полным объемом по SL (худший случай)
            actual_risk_usdt = sl_dist_usd * size
            notional_entry = levels["entry"] * size
            notional_exit_full = levels["sl"] * size  # При SL выходим всем объемом
            est_commission = (notional_entry + notional_exit_full) * self.fee_rate
            
            # Проверка EV по TP2 (основная цель), а не только по TP1
            gross_profit_tp2 = size * abs(levels["tp2"] - levels["entry"])
            required_profit = est_commission * 1.5  # Прибыль должна минимум в 1.5 раза покрывать комиссии
            
            if gross_profit_tp2 < required_profit:
                logger.warning(
                    f"🚫 [{self.symbol}] Positive EV Filter failed: "
                    f"GP_TP2=${gross_profit_tp2:.2f} < Req=${required_profit:.2f}"
                )
                self._log_autopsy("SIGNAL_REJECTED", {
                    "zone_id": zone.id, 
                    "reason": "positive_ev_failed",
                    "gp_tp2_usd": round(gross_profit_tp2, 2),
                    "req_usd": round(required_profit, 2),
                    "est_commission": round(est_commission, 2)
                })
                return

            target_profit_usdt = est_commission + (actual_risk_usdt * 0.03)
            price_offset = target_profit_usdt / size
            be_price = levels["entry"] + price_offset if zone.trade_side == "LONG" else levels["entry"] - price_offset

            logger.info(f"🛡️ [{self.symbol}] BE: {be_price:.4f} (Comm: ${est_commission:.2f} + Profit: ${actual_risk_usdt * 0.03:.2f})")

            passport = TradePassport(
                signal_id=zone.id, 
                strategy="GoldenBounce", 
                signal_source=SignalSource.TEST,
                symbol=self.symbol, 
                side=zone.trade_side,
                entry_price=levels["entry"], 
                sl_price=levels["sl"],
                tp1_price=levels["tp1"], 
                tp2_price=levels["tp2"],
                be_price=be_price, 
                planned_size=size, 
                position_size=size,
                target_risk_usdt=risk_usd, 
                status="IDLE"
            )

            tick_size_map = {
                "SOLUSDT": 0.01, "BTCUSDT": 0.1, "ETHUSDT": 0.01, 
                "LINKUSDT": 0.001, "BNBUSDT": 0.1, "AVAXUSDT": 0.01
            }
            tick_size = tick_size_map.get(self.symbol, 0.01)
            offset_ticks = self.config.get("trading", {}).get("strategies", {}).get("golden_bounce", {}).get("entry_offset_ticks", 0)
            
            limit_price = levels["entry"]
            if offset_ticks > 0:
                if zone.trade_side == "LONG":
                    limit_price += (offset_ticks * tick_size)
                else:
                    limit_price -= (offset_ticks * tick_size)
                logger.info(f"📏 [{self.symbol}] Entry offset: {offset_ticks} ticks. Limit: {limit_price:.4f} (Wall: {levels['entry']:.4f})")

            logger.info(
                f"🎯 [{self.symbol}] SIGNAL GENERATED: {zone.id} | {zone.trade_side} @ {levels['entry']:.2f} | "
                f"SL: {levels['sl']:.2f} | TP1: {levels['tp1']:.2f} | TP2: {levels['tp2']:.2f} | R:R={rr_ratio:.2f}"
            )

            self._log_autopsy("SIGNAL_GENERATED", {
                "zone_id": zone.id, 
                "side": zone.trade_side, 
                "wall_price": levels["entry"], 
                "limit_price": limit_price,
                "sl": levels["sl"], 
                "tp1": levels["tp1"], 
                "tp2": levels["tp2"],
                "risk": levels["risk_per_unit"], 
                "size": size, 
                "be_price": be_price,
                "cumulative_delta": zone.cumulative_delta,
                "offset_ticks": offset_ticks,
                "rr_ratio": round(rr_ratio, 3),
                "actual_risk_usd": round(actual_risk, 2)
            })

            await self.bus.publish("EXEC:NEW_TRADE", passport)
            logger.info(f"📋 [{self.symbol}] Passport registered in PositionMonitor (EXEC:NEW_TRADE)")

            if self.om:
                try:
                    await self.om.place_entry_order(
                        passport, 
                        limit_price=str(limit_price),
                        timeout_seconds=int(self.entry_order_ttl_sec), 
                        zone_id=zone.id
                    )
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
            
            # ✅ Проверка ценности стены в момент подтверждения
            peak_vol = float(event.get("peak_volume", 0))
            instr_cfg = self.config.get("instruments", {}).get(symbol, {})
            min_wall_usd = instr_cfg.get("min_wall_value_usd", 50000)
            
            wall_value_usd = price_f * peak_vol
            if wall_value_usd < min_wall_usd:
                logger.info(f"🚫 [{symbol}] Стена отклонена при подтверждении: ценность {wall_value_usd:.0f} USDT < порога {min_wall_usd} USDT (Peak Vol: {peak_vol})")
                return

            atr = self._get_current_atr()
            multipliers = self._get_session_multipliers()
            k_wall = multipliers.get("k_wall", 2.0)

            if self._market_is_dead(now, atr, price_f):
                self._log_autopsy("ZONE_REJECTED", {"price": price_f, "reason": "dead_market"})
                return

            radius = max(self.zone_dedup_min_pct * price_f, self.zone_dedup_atr_mult * atr)
            near = self._find_near_zone(price_f, radius)
            if near is not None:
                near.peak_volume = max(near.peak_volume, float(event.get("peak_volume", 0)))
                self._log_autopsy("ZONE_REJECTED", {"price": price_f, "reason": "dedup"})
                return
            
            zone = GBZone(symbol=symbol, price=price_f, side=str(side), created_at=now, atr_at_creation=atr, peak_volume=peak_vol, score_multiplier=1.0)
            zone.departure_threshold = zone.calculate_departure_threshold(self.atr_multiplier)
            self.active_zones.append(zone)

            self._zone_creations.append(now)
            self._zone_creations = [t for t in self._zone_creations if (now - t).total_seconds() < 3600]
            if len(self._zone_creations) > self.max_zones_per_hour:
                logger.warning(f"🚨 [{symbol}] HEALTH ALERT: zones_created_per_hour={len(self._zone_creations)}")

            logger.info(f"✅ [{symbol}] ZONE CREATED: {zone.id} ({zone.trade_side}) @ {price} | Peak Vol: {zone.peak_volume:.0f} | ATR: {atr:.3f}")
            self._log_autopsy("ZONE_CREATED", {"zone_id": zone.id, "price": price_f, "volume": zone.peak_volume, "side": zone.trade_side, "atr": atr, "k_wall": k_wall})
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