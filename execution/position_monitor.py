"""
Монитор позиций V2. Слушает UDS, управляет жизненным циклом через PassportManager,
сохраняет паспорта. Все смены статуса — только через manager.set_state().
"""
import logging
from datetime import datetime, timezone
from typing import Optional
from core.event_bus import EventBus
from execution.passport import TradePassport, CloseReason
from execution.passport_manager import PassportManager, TransitionError
from execution.order_manager import OrderManager
from execution.passport_storage import PassportStorage

logger = logging.getLogger("plato.position_monitor")


class PositionMonitor:
    def __init__(self, event_bus: EventBus, order_manager: OrderManager, storage: PassportStorage):
        self.bus = event_bus
        self.om = order_manager
        self.storage = storage
        self.active_passports: dict[str, TradePassport] = {}
        self.managers: dict[str, PassportManager] = {}  # V2: стейт-машина на паспорт

        self.bus.subscribe("UDS:ORDER_UPDATE", self._on_order_update)
        self.bus.subscribe("EXEC:NEW_TRADE", self._on_new_trade)
        self.bus.subscribe("ORDER:TIMER_CANCELLED", self._on_timer_cancelled)
        self.bus.subscribe("UDS:POSITION_UPDATE", self._on_position_update)

    # ------------------------------------------------------------
    # Вспомогательные
    # ------------------------------------------------------------
    def _get_manager(self, passport: TradePassport) -> PassportManager:
        if passport.passport_id not in self.managers:
            self.managers[passport.passport_id] = PassportManager(passport)
        return self.managers[passport.passport_id]

    def _safe_transition(self, passport: TradePassport, new_status: str,
                         reason: str = "", details: str = "", source: str = "SYSTEM") -> bool:
        """Безопасный переход: ловит нелегальные переходы без падения платформы."""
        try:
            return self._get_manager(passport).set_state(new_status, reason, details, source)
        except TransitionError as e:
            logger.warning(f"⚠️ [{passport.passport_id}] Переход проигнорирован: {e}")
            return False

    def _on_new_trade(self, passport: TradePassport):
        self.active_passports[passport.passport_id] = passport
        self._get_manager(passport)  # инициализируем стейт-машину (IDLE + client_order_id)
        logger.info(f"📋 [{passport.passport_id}] Паспорт добавлен в мониторинг")

    def _find_passport_by_order_id(self, order_id: Optional[str]) -> Optional[TradePassport]:
        if not order_id:
            return None
        for passport in self.active_passports.values():
            for order in passport.orders:
                if order.get("order_id") == order_id:
                    return passport
        return None

    def _update_order_status(self, passport: TradePassport, order_id: str, new_status: str):
        for order in passport.orders:
            if order.get("order_id") == order_id:
                order["status"] = new_status
                order["updated_at"] = datetime.now(timezone.utc).isoformat()
                break

    def _finalize_closed(self, passport: TradePassport, exit_reason: str,
                         exit_price: float, exit_qty: float, exec_fee: float,
                         leg_field: str, close_status: str = "CLOSED"):
        """Общее завершение сделки: комиссия, PnL плеча, итог, переход в CLOSED."""
        commission = exec_fee if exec_fee > 0 else passport.calculate_commission(exit_price, exit_qty)
        passport.commission += commission
        leg_pnl = passport.calculate_leg_pnl(exit_price, exit_qty)
        setattr(passport, leg_field, leg_pnl)

        passport.exit_reason = exit_reason
        passport.exit_price = exit_price
        passport.gross_pnl = passport.tp1_pnl + passport.tp2_pnl + passport.sl_pnl
        passport.realized_pnl = passport.gross_pnl - passport.commission

        passport.add_event(
            "EXCHANGE: CLOSED",
            f"Причина: {exit_reason}, Gross: {passport.gross_pnl:.2f}, "
            f"Comm: {passport.commission:.4f}, Realized: {passport.realized_pnl:.2f}",
            source="UDS",
        )
        logger.info(f"🏁 [{passport.passport_id}] Закрыта. {exit_reason}, Realized P&L: {passport.realized_pnl:.2f} USDT")

        await_self = None  # placeholder, реальный await ниже
        self._safe_transition(passport, close_status, reason=exit_reason, source="UDS")
        self.storage.save(passport)
        self.active_passports.pop(passport.passport_id, None)
        self.managers.pop(passport.passport_id, None)
        return leg_pnl

    # ------------------------------------------------------------
    # Таймер: ордер не исполнился за TTL
    # ------------------------------------------------------------
    async def _on_timer_cancelled(self, data: dict):
        order_id = data.get("order_id")
        if not order_id:
            return

        cancelled_qty = data.get("cancelled_qty", 0)
        filled_qty = data.get("filled_qty", 0)

        passport = self._find_passport_by_order_id(order_id)
        if not passport:
            return

        self._update_order_status(passport, order_id, "Cancelled")

        if filled_qty > 0:
            # Частичное исполнение — ставим защиту на то, что успело исполниться
            passport.filled_qty = filled_qty
            passport.position_size = filled_qty
            passport.fill_price = passport.entry_price if passport.fill_price == 0 else passport.fill_price
            passport.add_event("TIMER: PARTIAL_FILL",
                               f"Частичное исполнение: {filled_qty}, отменено: {cancelled_qty}",
                               source="TIMER")
            self._safe_transition(passport, "PARTIAL_FILL",
                                  reason=f"filled={filled_qty}, cancelled={cancelled_qty}", source="TIMER")
            logger.info(f"⚠️ [{passport.passport_id}] Частичное исполнение: {filled_qty} SOL")
            await self.om.place_protective_orders(passport)
        else:
            passport.add_event("TIMER: CANCELLED",
                               f"Ордер {order_id} отменён таймером. Объём: {cancelled_qty}",
                               source="TIMER")
            self._safe_transition(passport, "ORDER_CANCELLED",
                                  reason="TTL expired, no fill", source="TIMER")
            logger.info(f"⏱️ [{passport.passport_id}] Ордер отменён таймером")

        self.storage.save(passport)
        if self._get_manager(passport).current_state() in ("ORDER_CANCELLED",):
            self.active_passports.pop(passport.passport_id, None)
            self.managers.pop(passport.passport_id, None)

    # ------------------------------------------------------------
    # Обновление позиции (закрытие биржой → size=0)
    # ------------------------------------------------------------
    async def _on_position_update(self, data: list):
        for pos_info in data:
            symbol = pos_info.get("symbol")
            size = float(pos_info.get("size", 0))

            if symbol != "SOLUSDT":
                continue

            # ✅ ГЛАВНОЕ ИСПРАВЛЕНИЕ: Если размер позиции стал 0
            if size == 0:
                for passport in list(self.active_passports.values()):
                    mgr = self._get_manager(passport)
                    current_state = mgr.current_state()
                    
                    # ИГНОРИРУЕМ size=0, если мы еще даже не вошли в позицию!
                    # (ждем исполнения ордера или он частично исполнился)
                    if current_state in ("ORDER_PREPARING", "ORDER_SENT", "ORDER_PENDING", "PARTIAL_FILL"):
                        continue  # Пропускаем, это нормальное состояние до входа
                    
                    # Закрываем только если мы БЫЛИ в позиции, а теперь вышли
                    if current_state in ("POSITION_OPEN", "TP1_PARTIAL_CLOSE"):
                        await self.om.cancel_all_protective_orders(passport)
                        passport.add_event("EXCHANGE: POSITION_CLOSED",
                                           "Позиция закрыта биржей (size=0). Защитные ордера очищены.",
                                           source="UDS")
                        
                        if not self._safe_transition(passport, "CLOSED",
                                                     reason="position size=0", source="UDS"):
                            passport.status = "CLOSED"
                            passport.closed_at = datetime.now(timezone.utc)
                        if not passport.closed_at:
                            passport.closed_at = datetime.now(timezone.utc)
                            
                        logger.info(f"🏁 [{passport.passport_id}] Позиция закрыта биржей → CLOSED")
                        self.storage.save(passport)
                        self.active_passports.pop(passport.passport_id, None)
                        self.managers.pop(passport.passport_id, None)
                break

    # ------------------------------------------------------------
    # Обновление ордера (UDS)
    # ------------------------------------------------------------
    async def _on_order_update(self, data: list):
        for order_info in data:
            order_id = order_info.get("orderId")
            if not order_id:
                continue

            status = order_info.get("orderStatus")
            passport = self._find_passport_by_order_id(order_id)
            if not passport:
                continue

            logger.info(f"📡 [Monitor] order_id={order_id}, status={status}")
            exec_fee = float(order_info.get("cumExecFee", 0))
            is_entry = bool(passport.orders and passport.orders[0].get("order_id") == order_id)
            order_type = next((o.get("type") for o in passport.orders if o.get("order_id") == order_id), "")

            # --- Отмена ордера биржей ---
            if status == "Cancelled":
                self._update_order_status(passport, order_id, "Cancelled")
                passport.add_event("EXCHANGE: ORDER_CANCELLED",
                                   f"Ордер {order_id} ({order_type}) отменён биржей", source="UDS")
                # Если отменился входной ордер целиком и позиции нет → терминал
                if is_entry and passport.filled_qty == 0:
                    self._safe_transition(passport, "ORDER_CANCELLED",
                                          reason="entry cancelled by exchange", source="UDS")
                    self.storage.save(passport)
                    self.active_passports.pop(passport.passport_id, None)
                    self.managers.pop(passport.passport_id, None)
                    continue
                self.storage.save(passport)

            # --- Частичное исполнение входа ---
            elif status == "PartiallyFilled" and is_entry:
                self._update_order_status(passport, order_id, "PartiallyFilled")
                cum_qty = float(order_info.get("cumExecQty", 0))
                passport.filled_qty = cum_qty
                passport.position_size = cum_qty
                avg = float(order_info.get("avgPrice", passport.entry_price))
                if avg > 0:
                    passport.entry_price = avg
                    passport.fill_price = avg
                passport.add_event("EXCHANGE: PARTIAL_FILL",
                                   f"Исполнено {cum_qty} из {passport.planned_size}", source="UDS")
                self._safe_transition(passport, "PARTIAL_FILL",
                                      reason=f"partial {cum_qty}", source="UDS")
                logger.info(f"📊 [{passport.passport_id}] Частичное исполнение: {cum_qty} SOL")
                self.storage.save(passport)

            # --- Вход исполнен полностью ---
            elif is_entry and status == "Filled":
                self.om.order_timer.cancel_timer(order_id)
                self._update_order_status(passport, order_id, "Filled")

                avg = float(order_info.get("avgPrice", passport.entry_price))
                if avg > 0:
                    passport.entry_price = avg
                    passport.fill_price = avg
                passport.filled_qty = float(order_info.get("cumExecQty", passport.planned_size))
                passport.position_size = passport.filled_qty
                passport.exchange_verified = True

                entry_comm = exec_fee if exec_fee > 0 else passport.calculate_commission(passport.entry_price, passport.filled_qty)
                passport.commission += entry_comm

                # Фактический риск (баг 1 архива)
                sl_dist = abs(passport.entry_price - passport.sl_price)
                passport.actual_risk_usdt = passport.filled_qty * sl_dist
                passport.cap_applied = passport.filled_qty < (passport.target_risk_usdt / sl_dist - 1e-9) if sl_dist > 0 else False

                passport.add_event("EXCHANGE: FILLED",
                                   f"Вход исполнен по {passport.entry_price}. Комиссия: {entry_comm:.4f}",
                                   source="UDS")
                self._get_manager(passport).log_exchange_verified(order_id, passport.entry_price, passport.filled_qty)

                # Переход: PARTIAL_FILL -> POSITION_OPEN или ORDER_SENT/PENDING -> POSITION_OPEN
                cur = self._get_manager(passport).current_state()
                if cur == "PARTIAL_FILL":
                    self._safe_transition(passport, "POSITION_OPEN", reason="entry fully filled", source="UDS")
                else:
                    # из ORDER_SENT/ORDER_PENDING сразу в POSITION_OPEN (матрица разрешает)
                    self._safe_transition(passport, "POSITION_OPEN", reason="entry filled", source="UDS")

                passport.add_event("GUARD_REGISTERED",
                                   f"Защита: SL={passport.sl_price}, TP1={passport.tp1_price}, TP2={passport.tp2_price}",
                                   source="GUARD")
                logger.info(f"🎯 [{passport.passport_id}] Вход по {passport.entry_price}. Ставлю защиту...")
                await self.om.place_protective_orders(passport)
                self.storage.save(passport)

            # --- TP1 исполнен ---
            elif order_type == "LIMIT_TP1" and status in ("Filled", "PartiallyFilled"):
                self._update_order_status(passport, order_id, "Filled")
                tp1_qty = passport.position_size / 2.0
                tp1_pnl = passport.calculate_leg_pnl(passport.tp1_price, tp1_qty)
                tp1_comm = exec_fee if exec_fee > 0 else passport.calculate_commission(passport.tp1_price, tp1_qty)
                passport.commission += tp1_comm
                passport.tp1_pnl = tp1_pnl
                passport.gross_pnl = passport.tp1_pnl + passport.tp2_pnl + passport.sl_pnl
                passport.realized_pnl = passport.gross_pnl - passport.commission

                passport.add_event("EXCHANGE: TP1_FILLED",
                                f"TP1 исполнен. P&L: {tp1_pnl:.2f}, комиссия: {tp1_comm:.4f}", source="UDS")
                self._safe_transition(passport, "TP1_PARTIAL_CLOSE",
                                    reason=f"TP1 filled, pnl={tp1_pnl:.2f}", source="UDS")

                # ✅ ИСПРАВЛЕНИЕ: Уменьшаем объем SL до остатка позиции после TP1
                try:
                    filled_tp1_qty = next(
                        (o.get("qty", 0) for o in passport.orders 
                        if o.get("type") == "LIMIT_TP1" and o.get("status") == "Filled"), 
                        0
                    )
                    remaining_qty = passport.position_size - filled_tp1_qty

                    if remaining_qty > 0:
                        # Находим и отменяем старый SL
                        old_sl = next((o for o in passport.orders if o.get("type") == "STOP_SL" and o.get("status") == "New"), None)
                        if old_sl:
                            await self.om.rest.cancel_order(self.om.category, passport.symbol, old_sl["order_id"])
                            old_sl["status"] = "Cancelled"
                            passport.add_event("ACTION: SL_RESIZED", f"Старый SL ({old_sl['order_id']}) отменен для ресайза")
                            
                            # Выставляем новый SL на остаток
                            trigger_dir = 2 if passport.side == "LONG" else 1
                            new_sl_res = await self.om.rest.place_order(
                                category=self.om.category, symbol=passport.symbol,
                                side="Sell" if passport.side == "LONG" else "Buy",
                                qty=str(remaining_qty), price="", order_type="Market",
                                trigger_price=str(passport.sl_price), trigger_direction=trigger_dir,
                                reduce_only=True
                            )
                            new_sl_id = new_sl_res.get("orderId")
                            if new_sl_id:
                                passport.add_order_record(new_sl_id, "STOP_SL", "New", passport.sl_price, float(remaining_qty))
                                passport.add_event("SUCCESS: SL_RESIZED", f"Новый SL выставлен на {remaining_qty} SOL @ {passport.sl_price}")
                                logger.info(f"🔄 [{passport.passport_id}] SL уменьшен до {remaining_qty} SOL после TP1")
                            else:
                                passport.add_event("ERROR: SL_RESIZE_FAILED", "Биржа не вернула orderId для нового SL")
                                logger.error(f"❌ [{passport.passport_id}] Ошибка ресайза SL: нет orderId")
                except Exception as e:
                    # КРИТИЧЕСКИ ВАЖНО: Не даем сбою API убить мониторинг позиции
                    passport.add_event("ERROR: SL_RESIZE_EXCEPTION", str(e))
                    logger.error(f"❌ [{passport.passport_id}] Исключение при ресайзе SL: {e}. Позиция может остаться со старым SL или без него.")

                self.storage.save(passport)

                # ✅ ИСПРАВЛЕНИЕ: Переносим SL в безубыток ТОЛЬКО если be_price задан стратегией
                if passport.be_price is not None:
                    logger.info(f"🚀 [{passport.passport_id}] TP1! P&L: {tp1_pnl:.2f}. SL → безубыток ({passport.be_price})...")
                    await self.om.move_sl_to_breakeven(passport, str(passport.be_price))
                else:
                    logger.info(f"⚠️ [{passport.passport_id}] TP1! P&L: {tp1_pnl:.2f}. Безубыток ОТКЛЮЧЕН (be_price=null), оставляем структурный SL.")
            # --- TP2 / SL / SL_BE исполнен → закрытие ---
            elif order_type in ("LIMIT_TP2", "STOP_SL", "STOP_SL_BE") and status == "Filled":
                self._update_order_status(passport, order_id, "Filled")
                await self.om.cancel_all_protective_orders(passport)

                exit_qty = passport.position_size / 2.0
                if order_type == "LIMIT_TP2":
                    reason = CloseReason.TP2.value
                    price = passport.tp2_price
                    leg = "tp2_pnl"
                elif order_type == "STOP_SL_BE":
                    reason = CloseReason.BE.value
                    price = float(order_info.get("avgPrice", passport.entry_price)) or passport.entry_price
                    leg = "sl_pnl"
                else:
                    reason = CloseReason.SL.value
                    price = float(order_info.get("avgPrice", passport.sl_price)) or passport.sl_price
                    leg = "sl_pnl"

                commission = exec_fee if exec_fee > 0 else passport.calculate_commission(price, exit_qty)
                passport.commission += commission
                leg_pnl = passport.calculate_leg_pnl(price, exit_qty)
                setattr(passport, leg, leg_pnl)
                passport.exit_reason = reason
                passport.exit_price = price
                passport.gross_pnl = passport.tp1_pnl + passport.tp2_pnl + passport.sl_pnl
                passport.realized_pnl = passport.gross_pnl - passport.commission

                passport.add_event("EXCHANGE: CLOSED",
                                   f"Причина: {reason}, Gross: {passport.gross_pnl:.2f}, "
                                   f"Comm: {passport.commission:.4f}, Realized: {passport.realized_pnl:.2f}",
                                   source="UDS")
                logger.info(f"🏁 [{passport.passport_id}] Закрыта. {reason}, Realized: {passport.realized_pnl:.2f}")

                # TP_FULL_CLOSE -> CLOSED (для TP2) или SL_TRIGGERED -> CLOSED
                if order_type == "LIMIT_TP2":
                    self._safe_transition(passport, "TP_FULL_CLOSE", reason=reason, source="UDS")
                    self._safe_transition(passport, "CLOSED", reason=reason, source="UDS")
                else:
                    self._safe_transition(passport, "SL_TRIGGERED", reason=reason, source="UDS")
                    self._safe_transition(passport, "CLOSED", reason=reason, source="UDS")

                self.storage.save(passport)
                self.active_passports.pop(passport.passport_id, None)
                self.managers.pop(passport.passport_id, None)