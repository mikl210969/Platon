"""
Reconciliation Engine V2 — источник истины «биржа <-> паспорт».
Независимый от UDS канал правды через REST. Убивает фантомные беззащитные сделки.
✅ Fail-Safe: Защита молодых ордеров, принудительное закрытие, ДЕДУПЛИКАЦИЯ фантомов и ЗАЩИТА ОТ ГОНОК.
"""
import asyncio
import logging
import math
from datetime import datetime, timezone
from typing import Dict, Optional, List, Any

from core.event_bus import EventBus
from adapters.bybit_rest import BybitRestClient
from execution.passport_storage import PassportStorage
from execution.passport import TradePassport, SignalSource

logger = logging.getLogger("plato.recon_engine")


class ReconciliationEngine:
    def __init__(self, event_bus: EventBus, rest: BybitRestClient,
                 storage: PassportStorage, get_active_passports, get_manager,
                 order_manager=None,
                 symbol: str = "SOLUSDT", category: str = "linear",
                 interval_sec: int = 30, silence_sec: int = 300):
        self.bus = event_bus
        self.rest = rest
        self.storage = storage
        self.get_active_passports = get_active_passports
        self.get_manager = get_manager
        self.om = order_manager
        self.symbol = symbol
        self.category = category
        self.interval_sec = interval_sec
        self.silence_sec = silence_sec

        self._last_uds_event_ts: float = datetime.now(timezone.utc).timestamp()
        self._task: Optional[asyncio.Task] = None
        self._running = False
        
        # ✅ ДЕДУПЛИКАЦИЯ: Храним ключи обработанных фантомов
        self._known_phantoms: set = set()
        # ✅ СЧЕТЧИКИ ДЛЯ ЗАЩИТЫ ОТ МИГАЮЩИХ ПОЗИЦИЙ (Eventual Consistency)
        self._zero_position_cycles: dict[str, int] = {}  # passport_id -> кол-во циклов с pos_size=0
        self._phantom_cycles: dict[str, int] = {}        # symbol_side_entry -> кол-во циклов наличия фантома

        self.bus.subscribe("UDS:ORDER_UPDATE", self._on_uds_activity)
        self.bus.subscribe("UDS:POSITION_UPDATE", self._on_uds_activity)
        self.bus.subscribe("UDS:WALLET_UPDATE", self._on_uds_activity)

    async def start(self):
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop())
        logger.info(f"🛰️ [RECON] Запущен. interval={self.interval_sec}s, silence_threshold={self.silence_sec}s")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
        logger.info("🛰️ [RECON] Остановлен")

    def _on_uds_activity(self, _data):
        self._last_uds_event_ts = datetime.now(timezone.utc).timestamp()

    async def _loop(self):
        try:
            await self.reconcile(reason="STARTUP")
        except Exception as e:
            logger.error(f"🛰️ [RECON] Стартовая сверка упала: {e}")

        while self._running:
            try:
                await asyncio.sleep(self.interval_sec)
                if not self._running:
                    break

                silence = datetime.now(timezone.utc).timestamp() - self._last_uds_event_ts
                if silence > self.silence_sec:
                    logger.warning(f"🛰️ [RECON] Тишина UDS {silence:.0f}s > {self.silence_sec}s → форс-сверка")
                    await self.reconcile(reason="UDS_SILENCE")
                    self._last_uds_event_ts = datetime.now(timezone.utc).timestamp()
                else:
                    await self.reconcile(reason="PERIODIC")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"🛰️ [RECON] Ошибка цикла сверки: {e}")
                await asyncio.sleep(5)

    async def reconcile(self, reason: str = "PERIODIC"):
        try:
            positions = await self.rest.get_positions(self.category, self.symbol)
            open_orders = await self.rest.get_open_orders(self.category, self.symbol)
        except Exception as e:
            logger.error(f"🛰️ [RECON] Не удалось получить данные биржи: {e}")
            return

        exchange_size = 0.0
        for pos in positions:
            if pos.get("symbol") == self.symbol:
                exchange_size += abs(float(pos.get("size", 0)))

        exchange_order_ids = {o.get("orderId") for o in open_orders if o.get("orderId")}
        active = self.get_active_passports()

        logger.info(f"🛰️ [RECON:{reason}] pos_size={exchange_size}, open_orders={len(exchange_order_ids)}, active_passports={len(active)}")

        for pid, passport in list(active.items()):
            mgr = self.get_manager(passport)
            
            if mgr.current_state() == "CLOSED":
                continue

            # ✅ ШАГ 1: ЗАЩИТА ОТ ГОНКИ - Проверяем флаг is_closing
            if getattr(passport, 'is_closing', False):
                logger.debug(f"⏭️ [RECON] Пропуск {pid}: паспорт помечен как закрывающийся (source={getattr(passport, 'closing_source', 'UNKNOWN')})")
                continue

            if mgr.current_state() in ["ORDER_SENT", "ORDER_PENDING", "PARTIAL_FILL", "POSITION_OPEN"]:
                if mgr.get_state_age_seconds() < 60.0:
                    logger.debug(f"🛰️ [RECON] Пропуск сверки для молодого ордера {pid} (возраст < 60с)")
                    continue

            known_order_ids = {o.get("order_id") for o in passport.orders if o.get("order_id")}

            # ✅ R2a: Позиция на бирже исчезла (exchange_size=0)
            if mgr.is_active() and exchange_size == 0:
                protective_alive = known_order_ids & exchange_order_ids
                if not protective_alive:
                    zero_cycles = self._zero_position_cycles.get(pid, 0) + 1
                    self._zero_position_cycles[pid] = zero_cycles
                    
                    # Финализируем только после 2 подтверждений (например, 60 секунд)
                    if zero_cycles >= 2:
                        logger.info(f"🛰️ [RECON] [{passport.passport_id}] Позиция отсутствует {zero_cycles} цикла(ов). Финализация.")
                        await self._finalize_from_history(passport, mgr, reason)
                        self._zero_position_cycles.pop(pid, None)
                    else:
                        logger.debug(f"⏳ [RECON] [{passport.passport_id}] Позиция отсутствует ({zero_cycles}/2). Ожидание подтверждения...")
            
            # ✅ Сброс счетчика, если позиция вернулась
            elif mgr.is_active() and exchange_size > 0:
                self._zero_position_cycles.pop(pid, None)

            if mgr.is_active() and exchange_size > 0:
                protective_alive = known_order_ids & exchange_order_ids
                
                if not protective_alive:
                    if passport.filled_qty == 0:
                        logger.warning(f"️ [RECON] Рассинхрон: биржа имеет позицию {exchange_size}, а паспорт ждет входа. Синхронизация.")
                        passport.filled_qty = exchange_size
                        passport.position_size = exchange_size
                        
                        pos_data = next((p for p in positions if p.get("symbol") == self.symbol), {})
                        avg_price = pos_data.get("avgPrice")
                        if avg_price is not None:
                            try:
                                passport.fill_price = float(avg_price)
                                passport.entry_price = passport.fill_price
                            except (TypeError, ValueError):
                                pass
                        
                        mgr.set_state("POSITION_OPEN", reason="RECON: Synced with exchange position", source="RECON")
                        mgr.log_guard_registered(passport.sl_price, passport.tp1_price, passport.tp2_price, passport.be_price)

                    await self._reprotect(passport, mgr, reason)

        all_known_ids = set()
        for p in active.values():
            all_known_ids |= {o.get("order_id") for o in p.orders if o.get("order_id")}
        for oid in exchange_order_ids:
            if oid not in all_known_ids:
                await self._cancel_orphan(oid, reason)

        # ✅ Фантом: позиция есть, активного паспорта нет
        if exchange_size > 0 and not any(self.get_manager(p).is_active() for p in active.values()):
            pos = next((p for p in positions if p.get("symbol") == self.symbol), {})
            side_raw = pos.get("side", "")
            entry = float(pos.get("avgPrice", 0))
            side = "LONG" if side_raw == "Buy" else "SHORT"
            phantom_key = f"{self.symbol}_{side}_{entry}"
            
            phantom_cycles = self._phantom_cycles.get(phantom_key, 0) + 1
            self._phantom_cycles[phantom_key] = phantom_cycles
            
            # Усыновляем только после 2 подтверждений
            if phantom_cycles >= 2:
                logger.info(f"🛰️ [RECON] Фантом {phantom_key} подтвержден {phantom_cycles} цикла(ов). Усыновление.")
                await self._adopt_phantom(exchange_size, positions, reason)
                self._phantom_cycles.pop(phantom_key, None)
            else:
                logger.debug(f"⏳ [RECON] Фантом {phantom_key} ({phantom_cycles}/2). Ожидание подтверждения...")
        
        # ✅ Сброс счётчиков фантомов, если позиция исчезла
        if exchange_size == 0:
            for key in list(self._phantom_cycles.keys()):
                if key.startswith(f"{self.symbol}_"):
                    self._phantom_cycles.pop(key, None)
                    logger.debug(f"️ [RECON] Счётчик фантома {key} сброшен (позиция исчезла)")
        
        # ✅ Сброс счетчиков фантомов, если для этого символа появился активный паспорт
        for key in list(self._phantom_cycles.keys()):
            if key.startswith(f"{self.symbol}_"):
                if any(self.get_manager(p).is_active() and p.symbol == self.symbol for p in active.values()):
                    self._phantom_cycles.pop(key, None)

    async def _finalize_from_history(self, passport, mgr, reason: str):
        # ✅ ЗАЩИТА ОТ ПОВТОРНОЙ ФИНАЛИЗАЦИИ И МУСОРНОГО PNL
        if getattr(passport, 'exit_reason', '') and passport.exit_reason != "":
            logger.debug(f"⏭️ [{passport.passport_id}] Уже финализирован (reason={passport.exit_reason}), пропускаем.")
            return
            
        try:
            history = await self.rest.get_closed_pnl_history(self.category, self.symbol, limit=10)
        except Exception as e:
            logger.error(f"🛰️ [RECON] История PnL недоступна: {e}")
            history = []

        # ✅ ИЩЕМ ТОЛЬКО РЕЛЕВАНТНУЮ ЗАПИСЬ ПО ЦЕНЕ ВХОДА
        last = None
        for rec in history:
            rec_entry = float(rec.get("avgEntryPrice", 0))
            if abs(rec_entry - passport.entry_price) / max(passport.entry_price, 1e-9) < 0.001:
                last = rec
                break
                
        if not last and history:
             last = next((rec for rec in history if rec.get("symbol") == self.symbol), None)

        if last:
            cum_fee = float(last.get("cumFee", 0))
            closed_pnl = float(last.get("closedPnl", 0))
            avg_price = float(last.get("avgEntryPrice", passport.entry_price)) or passport.entry_price
            
            passport.commission = max(passport.commission, abs(cum_fee))
            if passport.tp1_pnl == 0 and passport.tp2_pnl == 0 and passport.sl_pnl == 0:
                passport.realized_pnl = closed_pnl - passport.commission
                passport.gross_pnl = closed_pnl
            else:
                passport.realized_pnl = passport.gross_pnl - passport.commission
                
            passport.exit_price = avg_price
            if not passport.exit_reason:
                passport.exit_reason = "RECON_FLAT"
            mgr.log_recon("FINALIZED", f"Паспорт финализирован по истории. closedPnl={closed_pnl}")
        else:
            mgr.log_recon("FINALIZED", "Позиция исчезла, история пуста → закрытие без PnL-реконструкции")
            if not passport.exit_reason:
                passport.exit_reason = "RECON_FLAT"

        mgr.force_close(reason=f"RECON_FINALIZED_NO_POSITION_{reason}")
        self.storage.save(passport)
        logger.info(f"🛰️ [RECON] [{passport.passport_id}] Финализирован → CLOSED ({reason})")

    async def _reprotect(self, passport: TradePassport, mgr, reason: str):
        """Безопасное восстановление защитных ордеров с проверкой наличия позиции."""
        if not self.om:
            mgr.log_recon("REPROTECT_SKIPPED", "OrderManager недоступен")
            return
            
        if getattr(passport, 'is_closing', False):
            logger.debug(f"️ Skipping reprotect for {passport.passport_id}: Flag is_closing=True.")
            return

        try:
            # ✅ АТОМАРНАЯ ПРОВЕРКА ПЕРЕД ВЫЗОВОМ ORDER MANAGER
            pos_list = await self.rest.get_positions(self.category, passport.symbol)
            current_pos = next((p for p in pos_list if p.get("symbol") == passport.symbol), None)
            current_size = float(current_pos.get("size", 0)) if current_pos else 0
            
            if current_size == 0:
                logger.warning(f"⚠️ [{passport.passport_id}] Position vanished during reprotect. Finalizing...")
                await self._finalize_from_history(passport, mgr, reason)
                return
                
            success = await self.om.place_protective_orders(passport)
            
            if not success:
                # ✅ ОБРАБОТКА ОШИБКИ ЗАЩИТЫ КАК СОСТОЯНИЕ
                mgr.log_recon("REPROTECT_FAILED", "Не удалось выставить защитные ордера")
                
                # Повторная проверка: позиция еще существует?
                pos_list_retry = await self.rest.get_positions(self.category, passport.symbol)
                current_pos_retry = next((p for p in pos_list_retry if p.get("symbol") == passport.symbol), None)
                retry_size = float(current_pos_retry.get("size", 0)) if current_pos_retry else 0
                
                if retry_size == 0:
                    await self._finalize_from_history(passport, mgr, reason)
                else:
                    mgr.set_state("ERROR_STATE", reason="protective_orders_failed", source="RECON")
                    self.storage.save(passport)
                return
                
            mgr.log_recon("REPROTECTED", "Защитные ордера восстановлены по паспорту")
            self.storage.save(passport)
            logger.warning(f"🛰️ [RECON] [{passport.passport_id}] Защита восстановлена ({reason})")
            
        except Exception as e:
            mgr.log_recon("REPROTECT_FAILED", str(e))
            logger.error(f"❌ Error during recon check for {passport.passport_id}: {e}")

    async def _cancel_orphan(self, order_id: str, reason: str):
        """Отменяет ордер-сироту ТОЛЬКО после подтверждения его существования."""
        try:
            # ✅ ИСПРАВЛЕНО: используем существующий метод get_order
            order_result = await self.rest.get_order(self.category, self.symbol, order_id)
            
            # Bybit возвращает структуру {"result": {"list": [...]}} или напрямую список
            order_info = None
            if isinstance(order_result, dict):
                result = order_result.get("result", order_result)
                if isinstance(result, dict):
                    order_list = result.get("list", [])
                    if isinstance(order_list, list) and order_list:
                        order_info = order_list[0]
                    else:
                        order_info = result
                elif isinstance(result, list) and result:
                    order_info = result[0]
            
            order_status = order_info.get("orderStatus") if order_info else None
            
            if not order_info or order_status in ("Cancelled", "Filled", "Rejected"):
                logger.info(f"🛰️ [RECON] Ордер {order_id} уже не активен (статус={order_status}). Пропуск отмены.")
                return
            
            # Только если ордер реально активен — отменяем
            await self.rest.cancel_order(self.category, self.symbol, order_id)
            logger.warning(f"🛰️ [RECON] Ордер-сирота отменён: {order_id} ({reason})")
            
        except Exception as e:
            error_msg = str(e).lower()
            if "not found" in error_msg or "10001" in error_msg or "order not exist" in error_msg:
                logger.info(f"🛰️ [RECON] Ордер {order_id} не существует на бирже. Пропуск.")
            else:
                logger.error(f"🛰️ [RECON] Не удалось проверить/отменить сироту {order_id}: {e}")

    async def _adopt_phantom(self, exchange_size: float, positions: list, reason: str):
        pos = next((p for p in positions if p.get("symbol") == self.symbol), {})
        side_raw = pos.get("side", "")
        entry = float(pos.get("avgPrice", 0))
        side = "LONG" if side_raw == "Buy" else "SHORT"
        
        # ✅ ДЕДУПЛИКАЦИЯ ПО ПОЛНОМУ СОСТОЯНИЮ (символ + сторона + цена + размер)
        phantom_key = f"{self.symbol}_{side}_{entry}_{exchange_size}"
        if phantom_key in self._known_phantoms:
            logger.debug(f"🛰️ [RECON] Фантом {phantom_key} уже обработан, пропускаем")
            return
        
        # ✅ ДОПОЛНИТЕЛЬНАЯ ЗАЩИТА: Если фантом с той же ценой/стороной уже обрабатывается
        # и размер изменился незначительно (<10%), считаем это тем же фантомом
        base_key = f"{self.symbol}_{side}_{entry}"
        if any(k.startswith(base_key) for k in self._known_phantoms):
            for k in self._known_phantoms:
                if k.startswith(base_key):
                    try:
                        old_size = float(k.split("_")[-1])
                        size_change = abs(exchange_size - old_size) / max(old_size, 1e-9)
                        if size_change < 0.1:  # Изменение < 10%
                            logger.debug(f"🛰️ [RECON] Фантом {base_key} уже обрабатывается (размер изменился на {size_change:.1%}). Пропускаем.")
                            return
                    except (ValueError, IndexError):
                        pass
        
        self._known_phantoms.add(phantom_key)
        
        logger.critical(f"🚨 [RECON] ФАНТОМНАЯ ПОЗИЦИЯ: {exchange_size} {self.symbol} {side} @ {entry}. ({reason})")
        
        await self.bus.publish("RECON:PHANTOM_DETECTED", {
            "symbol": self.symbol, "size": exchange_size, "side": side, "entry": entry
        })
        
        if self.om and entry > 0:
            try:
                # ✅ АТОМАРНАЯ ПРОВЕРКА ПЕРЕД СОЗДАНИЕМ ФАНТОМА
                pos_list = await self.rest.get_positions(self.category, self.symbol)
                current_pos = next((p for p in pos_list if p.get("symbol") == self.symbol), None)
                current_size = float(current_pos.get("size", 0)) if current_pos else 0
                
                if current_size <= 0:
                    logger.warning(f"⚠️ [RECON] Позиция {self.symbol} исчезла до создания фантома. Пропуск.")
                    return
                
                MIN_QTY_STEPS = {
                    "SOLUSDT": 0.1,
                    "LINKUSDT": 1.0,
                    "ETHUSDT": 0.01,
                    "BTCUSDT": 0.001,
                }
                min_step = MIN_QTY_STEPS.get(self.symbol, 0.01)
                safe_size = math.floor(current_size / min_step) * min_step
                decimals = len(str(min_step).split('.')[-1]) if '.' in str(min_step) else 0
                safe_size = round(safe_size, decimals)
                
                if safe_size < min_step:
                    logger.warning(f"🛰️ [RECON] Размер позиции {current_size} < мин. шага {min_step}. Пропуск.")
                    return
                
                phantom = TradePassport(
                    symbol=self.symbol, side=side, entry_price=entry, fill_price=entry,
                    planned_size=safe_size, position_size=safe_size, filled_qty=safe_size,
                    sl_price=entry * (0.99 if side == "LONG" else 1.01),
                    tp1_price=entry * (1.005 if side == "LONG" else 0.995),
                    tp2_price=entry * (1.01 if side == "LONG" else 0.99),
                    exchange_verified=True,
                    signal_source=SignalSource.ADAPTIVE_ZONE,
                )

                phantom.add_event("RECON: PHANTOM_ADOPTED", f"Усыновлена фантомная позиция {safe_size} @ {entry}", source="RECON")
                await self.bus.publish("EXEC:NEW_TRADE", phantom)
                await self.om.place_protective_orders(phantom)
                logger.warning(f"🛰️ [RECON] Фантом усыновлён и защищён: {phantom.passport_id}")
            except Exception as e:
                logger.error(f"🛰️ [RECON] Не удалось усыновить фантом: {e}")