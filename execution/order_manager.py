"""
Менеджер ордеров. Отправляет, модифицирует и отменяет ордера на бирже.
Универсальное округление и ФОРМАТИРОВАНИЕ количества (Qty) до шага биржи.
ВОЛНА 1: hard-guard (ордера + позиция, fail-closed).
ВОЛНА 1.5: жизнь ордера = жизнь зоны; отмена с тремя состояниями гонки.
"""
import logging
import math
from typing import Optional
from core.event_bus import EventBus
from adapters.bybit_rest import BybitRestClient
from execution.passport import TradePassport
from execution.order_timer import OrderTimer

logger = logging.getLogger("plato.order_manager")


class OrderManager:
    def __init__(self, rest_client: BybitRestClient, category: str = "linear", 
                 event_bus: Optional[EventBus] = None, storage=None):
        self.rest = rest_client
        self.category = category
        self.bus = event_bus
        self.storage = storage
        self.order_timer = OrderTimer(rest_client, event_bus)

    @staticmethod
    def _round_down_qty(qty: float, symbol: str) -> float:
        """Безопасное округление количества ВНИЗ до минимального шага биржи."""
        MIN_QTY_STEPS = {
            "SOLUSDT": 0.1,
            "LINKUSDT": 1.0,
            "ETHUSDT": 0.01,
            "BTCUSDT": 0.001,
            "BNBUSDT": 0.01,
            "AVAXUSDT": 0.1,
        }
        min_step = MIN_QTY_STEPS.get(symbol, 0.001)
        rounded = math.floor(qty / min_step) * min_step
        decimals = len(str(min_step).split('.')[-1]) if '.' in str(min_step) else 0
        return round(rounded, decimals)
    
    @staticmethod
    def _format_qty(qty: float, symbol: str) -> str:
        """Форматирует количество в строку с точным количеством знаков после запятой."""
        MIN_QTY_STEPS = {
            "SOLUSDT": 0.1,
            "LINKUSDT": 1.0,
            "ETHUSDT": 0.01,
            "BTCUSDT": 0.001,
            "BNBUSDT": 0.01,
            "AVAXUSDT": 0.1,
        }
        min_step = MIN_QTY_STEPS.get(symbol, 0.001)
        decimals = len(str(min_step).split('.')[-1]) if '.' in str(min_step) else 0
        return f"{qty:.{decimals}f}"

    async def has_open_position(self, symbol: str) -> bool:
        try:
            result = await self.rest._request(
                "GET", 
                "/v5/position/list", 
                {"category": self.category, "symbol": symbol}, 
                signed=True
            )
            positions = result.get("list", [])
            for pos in positions:
                size = float(pos.get("size", 0))
                if size > 0:
                    return True
            return False
        except Exception as e:
            logger.error(f"❌ Ошибка проверки позиции: {e}")
            return False

    # === ВОЛНА 1.5: отмена входа с тремя состояниями ===
    async def cancel_entry_order(self, passport, reason: str) -> str:
        """Отменить висящий входной ордер. Возвращает одно из:
        CANCELLED / FILLED / PENDING / NO_ORDER.
        Гонка cancel-vs-fill трактуется как ШТАТНЫЙ исход, не как exception."""
        entry = None
        for o in passport.orders:
            if o.get("type") == "LIMIT_ENTRY" and o.get("status") in ("New", "PartiallyFilled"):
                entry = o
        if entry is None:
            return "NO_ORDER"

        order_id = entry["order_id"]
        passport.cancel_source = reason

        try:
            await self.rest.cancel_order(
                category=self.category, symbol=passport.symbol, order_id=order_id
            )
            state = "CANCELLED"
        except Exception as e:
            # Отмена не прошла: скорее всего гонка (ордер уже исполнен,
            # Bybit вернёт OrderNotFound/already-filled). Смотрим факт на бирже.
            state = await self._query_order_state(passport.symbol, order_id)
            if state == "FILLED":
                logger.warning(f"⚠️ [{passport.passport_id}] Гонка cancel-vs-fill: ордер {order_id} исполнился до отмены — позицию подхватывает monitor")
            elif state == "PENDING":
                logger.warning(f"⚠️ [{passport.passport_id}] Отмена {order_id} не подтверждена (PENDING) — сверка через UDS/RECON")
            else:
                logger.info(f"[{passport.passport_id}] Отмена {order_id}: состояние биржи {state} ({e})")

        passport.add_event("ACTION: ENTRY_CANCEL", f"{reason} → {state}", source="ORDER_MANAGER")
        logger.info(f"🛑 [{passport.passport_id}] Отмена входа: reason={reason}, state={state}")
        return state

    async def _query_order_state(self, symbol: str, order_id: str) -> str:
        """Фактическое состояние ордера на бирже: FILLED / CANCELLED / PENDING."""
        try:
            resp = await self.rest._request(
                "GET", "/v5/order/history",
                {"category": self.category, "symbol": symbol, "orderId": order_id},
                signed=True,
            )
            lst = resp.get("list") or []
            if not lst:
                resp = await self.rest._request(
                    "GET", "/v5/order/realtime",
                    {"category": self.category, "symbol": symbol, "orderId": order_id},
                    signed=True,
                )
                lst = resp.get("list") or []
                if lst:
                    st = (lst[0].get("orderStatus") or "").upper()
                    return "PENDING" if st in ("NEW", "PARTIALLYFILLED", "UNTRIGGERED") else st
                return "PENDING"
            st = (lst[0].get("orderStatus") or "").upper()
            if st in ("FILLED", "PARTIALLYFILLED"):
                return "FILLED"
            if st in ("CANCELLED", "REJECTED", "DEACTIVATED"):
                return "CANCELLED"
            return "PENDING"
        except Exception:
            return "PENDING"

    async def place_entry_order(self, passport, limit_price: str, 
                                timeout_seconds: int = 600, 
                                order_link_id: Optional[str] = None,
                                zone_id: Optional[str] = None):
        try:
            # === ВОЛНА 1: hard-guard по правде биржи (fail-closed) ===
            guard_code = await self._entry_guard_check(passport.symbol)
            if guard_code:
                passport.add_event("ERROR: ENTRY_GUARD", guard_code)
                logger.error(f"❌ [{passport.passport_id}] Hard guard: {guard_code}")
                return False

            has_position = await self.has_open_position(passport.symbol)
            if has_position:
                passport.add_event("ERROR: POSITION_EXISTS", f"Уже есть открытая позиция по {passport.symbol}")
                logger.warning(f"️ [{passport.passport_id}] Пропуск: уже есть открытая позиция по {passport.symbol}")
                return False

            safe_qty = self._round_down_qty(float(passport.position_size), passport.symbol)
            
            if safe_qty <= 0:
                passport.add_event("ERROR: QTY_INVALID", f"Рассчитанное количество {safe_qty} <= 0")
                logger.error(f"❌ [{passport.passport_id}] Ошибка: рассчитанное количество {safe_qty} <= 0")
                return False

            passport.planned_size = safe_qty
            passport.sizing_info.update({
                "final_quantity": safe_qty,
                "base_sl_distance": abs(passport.entry_price - passport.sl_price) / passport.entry_price * 100,
                "sl_distance": abs(passport.entry_price - passport.sl_price) / passport.entry_price * 100
            })

            passport.add_event("ACTION: PLACE_ENTRY", f"Размещение {passport.side} {safe_qty} @ {limit_price}")
            logger.info(f"[{passport.passport_id}] Размещение входа: {passport.side} {safe_qty} @ {limit_price}")

            extra_params = {}
            if order_link_id:
                extra_params["orderLinkId"] = order_link_id

            result = await self.rest.place_order(
                category=self.category,
                symbol=passport.symbol,
                side="Buy" if passport.side == "LONG" else "Sell",
                qty=self._format_qty(safe_qty, passport.symbol),
                price=limit_price,
                order_type="Limit",
                time_in_force="GTC",
                extra_params=extra_params,
            )

            order_id = result.get("orderId")
            if order_id:
                passport.add_order_record(order_id, "LIMIT_ENTRY", "New", float(limit_price),
                                          safe_qty, client_order_id=order_link_id)
                passport.add_event("SUCCESS: ORDER_PLACED", f"Биржа приняла ордер {order_id}")

                await self.order_timer.start_timer(
                    category=self.category,
                    symbol=passport.symbol,
                    order_id=order_id,
                    timeout_seconds=timeout_seconds,
                    planned_qty=safe_qty,
                    zone_id=zone_id,
                    fallback_to_market=False
                )

                logger.info(f"✅ [{passport.passport_id}] Вход размещен. Order ID: {order_id}, TTL: {timeout_seconds}s")
                # Сохраняем паспорт сразу после размещения ордера (без задержки)
                self._save_passport_json(passport)
                return True
            else:
                passport.add_event("ERROR: NO_ORDER_ID", "Биржа не вернула orderId")
                logger.error(f"❌ [{passport.passport_id}] Биржа не вернула orderId")
                return False

        except Exception as e:
            passport.add_event("ERROR: ENTRY_FAILED", str(e))
            logger.error(f"❌ [{passport.passport_id}] Ошибка входа: {e}")
            return False

    async def _entry_guard_check(self, symbol: str):
        """ВОЛНА 1: hard-guard по правде биржи. Возвращает код отказа или None.
        ОБЕ проверки (позиция и входные ордера) живут в ОДНОМ try:
        fail-closed общий — любой сбой REST => CHECK_FAILED."""
        try:
            # ПРОВЕРКА 1: открытая позиция по символу
            pos_resp = await self.rest._request(
                "GET", "/v5/position/list",
                {"category": self.category, "symbol": symbol},
                signed=True,
            )
            for pos in (pos_resp.get("list") or []):
                if float(pos.get("size", 0) or 0) != 0:
                    logger.warning(f"🛡️ [GUARD] {symbol}: открыта позиция {pos.get('size')} → POSITION_OPEN")
                    return "ENTRY_GUARD: POSITION_OPEN"

            # ПРОВЕРКА 2: открытые ВХОДНЫЕ ордера.
            ord_resp = await self.rest._request(
                "GET", "/v5/order/realtime",
                {"category": self.category, "symbol": symbol, "openOnly": 1},
                signed=True,
            )
            orders_list = ord_resp.get("list") or []
            logger.info(f"🛡️ [GUARD] {symbol}: найдено {len(orders_list)} открытых ордеров")
            
            # ДЕТАЛЬНОЕ ЛОГИРОВАНИЕ КАЖДОГО ОРДЕРА
            for i, o in enumerate(orders_list):
                order_id = o.get("orderId", "Unknown")
                reduce_only = o.get("reduceOnly", False)
                order_status = o.get("orderStatus", "Unknown")
                side = o.get("side", "Unknown")
                order_link_id = o.get("orderLinkId", "")
                
                logger.info(f"🛡️ [GUARD] Ордер #{i+1}: id={order_id}, side={side}, reduceOnly={reduce_only}, status={order_status}, linkId={order_link_id}")
                
                # Входные ордера: не reduceOnly и активный статус
                if not reduce_only and order_status in ("New", "PartiallyFilled", "Untriggered"):
                    logger.warning(f"🛡️ [GUARD] {symbol}: открытый входной ордер {order_id} → PENDING_EXISTS")
                    return "ENTRY_GUARD: PENDING_EXISTS"

        except Exception as e:
            logger.error(f"🛡️ [GUARD] {symbol}: REST-проверка не прошла ({e}) — fail-closed отказ")
            return "ENTRY_GUARD: CHECK_FAILED"

        return None

    def _save_passport_json(self, passport: TradePassport):
        """Сохранить паспорт в data/passports/<PASS_ID>_<SYMBOL>.json"""
        try:
            import json
            from pathlib import Path
            
            passports_dir = Path("data/passports")
            passports_dir.mkdir(parents=True, exist_ok=True)
            
            filename = f"{passport.passport_id}_{passport.symbol}.json"
            filepath = passports_dir / filename
            
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(passport.to_dict(), f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"💾 [{passport.passport_id}] Passport saved to {filepath}")
            
        except Exception as e:
            logger.error(f"❌ [{passport.passport_id}] Failed to save passport JSON: {e}", exc_info=True)

    async def place_protective_orders(self, passport: TradePassport) -> bool:
        try:
            if getattr(passport, 'is_closing', False):
                logger.info(f"🛑 Blocked protective orders for {passport.passport_id}: Passport is closing (source={getattr(passport, 'closing_source', 'UNKNOWN')}).")
                return False
            
            pos_size = self._round_down_qty(float(passport.position_size), passport.symbol)
            
            if pos_size <= 0:
                logger.error(f" [{passport.passport_id}] Размер позиции {pos_size} <= 0. Защита пропущена.")
                return False

            qty_half = self._round_down_qty(pos_size / 2.0, passport.symbol)
            if qty_half <= 0:
                qty_half = pos_size
                
            qty_full = pos_size
            symbol = passport.symbol
            side_exit = "Sell" if passport.side == "LONG" else "Buy"
            
            tp1_res = await self.rest.place_order(
                category=self.category, symbol=symbol, side=side_exit,
                qty=self._format_qty(qty_half, passport.symbol),
                price=str(passport.tp1_price),
                order_type="Limit", time_in_force="GTC", reduce_only=True
            )
            tp1_id = tp1_res.get("orderId")
            if tp1_id: passport.add_order_record(tp1_id, "LIMIT_TP1", "New", passport.tp1_price, float(qty_half))
            
            tp2_res = await self.rest.place_order(
                category=self.category, symbol=symbol, side=side_exit,
                qty=self._format_qty(qty_half, passport.symbol),
                price=str(passport.tp2_price),
                order_type="Limit", time_in_force="GTC", reduce_only=True
            )
            tp2_id = tp2_res.get("orderId")
            if tp2_id: passport.add_order_record(tp2_id, "LIMIT_TP2", "New", passport.tp2_price, float(qty_half))

            trigger_dir = 2 if passport.side == "LONG" else 1
            sl_res = await self.rest.place_order(
                category=self.category, symbol=symbol, side=side_exit,
                qty=self._format_qty(qty_full, passport.symbol),
                price="", 
                order_type="Market", 
                trigger_price=str(passport.sl_price),
                trigger_direction=trigger_dir,
                reduce_only=True
            )
            sl_id = sl_res.get("orderId")
            if sl_id: passport.add_order_record(sl_id, "STOP_SL", "New", passport.sl_price, float(qty_full))
            
            if tp1_id and tp2_id and sl_id:
                be_txt = f", BE: {passport.be_price}" if passport.be_price else ""
                passport.add_event("GUARD_REGISTERED", 
                                   f"Защита выставлена. SL: {passport.sl_price}, TP1: {passport.tp1_price}, TP2: {passport.tp2_price}{be_txt}", 
                                   source="ORDER_MANAGER")
                if self.storage:
                    self.storage.save(passport)
                
                # ✅ Сохраняем паспорт в JSON для анализа
                self._save_passport_json(passport)
                
                return True
            else:
                logger.error(f"❌ [{passport.passport_id}] Не все защитные ордера размещены. TP1:{bool(tp1_id)}, TP2:{bool(tp2_id)}, SL:{bool(sl_id)}")
                return False
                
        except Exception as e:
            logger.error(f"❌ [{passport.passport_id}] Ошибка защитных ордеров: {e}")
            return False

    async def move_sl_to_breakeven(self, passport: TradePassport, new_sl_price: str) -> bool:
        try:
            current_sl = next((o for o in passport.orders if o.get("type") == "STOP_SL" and o.get("status") in ("New", "Untriggered", "Active")), None)
            
            if current_sl:
                await self.rest.cancel_order(self.category, passport.symbol, current_sl["order_id"])
                current_sl["status"] = "Cancelled"
                passport.add_event("ACTION: SL_CANCELLED", f"Старый SL {current_sl['order_id']} отменен")
                logger.info(f"🔄 [{passport.passport_id}] Старый SL отменен")

            filled_tp_qty = sum(
                o.get("qty", 0) for o in passport.orders 
                if o.get("type") in ("LIMIT_TP1", "LIMIT_TP2") and o.get("status") == "Filled"
            )
            remaining_qty = float(passport.position_size) - filled_tp_qty
            
            safe_remaining_qty = self._round_down_qty(remaining_qty, passport.symbol)

            if safe_remaining_qty <= 0:
                logger.warning(f"⚠️ [{passport.passport_id}] Нет остатка позиции ({safe_remaining_qty}) для переноса SL")
                return False

            if passport.be_price is None and float(new_sl_price) == passport.entry_price:
                passport.add_event("WARNING: BE_SKIPPED", f"BE отключен стратегией (be_price=null). Оставляем структурный SL.")
                logger.warning(f"️ [{passport.passport_id}] BE пропущен: be_price=null. Структурный SL сохранен.")
                return False

            trigger_dir = 2 if passport.side == "LONG" else 1
            
            res = await self.rest.place_order(
                category=self.category, symbol=passport.symbol, 
                side="Sell" if passport.side == "LONG" else "Buy",
                qty=self._format_qty(safe_remaining_qty, passport.symbol),
                price="", order_type="Market", 
                trigger_price=new_sl_price, trigger_direction=trigger_dir,
                reduce_only=True
            )
            
            new_sl_id = res.get("orderId")
            if new_sl_id:
                passport.add_order_record(new_sl_id, "STOP_SL_BE", "New", float(new_sl_price), float(safe_remaining_qty))
                passport.add_event("SUCCESS: SL_MOVED_TO_BE", f"Новый SL выставлен по {new_sl_price} на {safe_remaining_qty}")
                logger.info(f"✅ [{passport.passport_id}] SL перенесен в BE ({new_sl_price}) на остаток {safe_remaining_qty}.")
                return True
            else:
                passport.add_event("ERROR: NO_NEW_SL_ID", "Биржа не вернула orderId для нового SL")
                logger.error(f"❌ [{passport.passport_id}] Биржа не вернула orderId для нового SL")
                return False
            
        except Exception as e:
            passport.add_event("ERROR: MOVE_SL_FAILED", str(e))
            logger.error(f"❌ [{passport.passport_id}] Ошибка переноса SL: {e}")
            return False

    async def cancel_all_protective_orders(self, passport: TradePassport) -> bool:
        cancelled_count = 0
        protective_types = ["LIMIT_TP1", "LIMIT_TP2", "STOP_SL", "STOP_SL_BE"]
        
        for order in passport.orders:
            order_id = order.get("order_id")
            if not order_id:
                continue
                
            if order.get("type") in protective_types and order.get("status") in ("New", "Untriggered", "Active"):
                try:
                    await self.rest.cancel_order(
                        category=self.category,
                        symbol=passport.symbol,
                        order_id=order_id
                    )
                    order["status"] = "Cancelled"
                    cancelled_count += 1
                    logger.info(f"🧹 [{passport.passport_id}] Отменен висящий ордер {order.get('type')}: {order_id}")
                except Exception as e:
                    logger.debug(f"Не удалось отменить ордер {order_id}: {e}")
        
        if cancelled_count > 0:
            passport.add_event("ACTION: PROTECTIVE_ORDERS_CANCELLED", f"Очищено биржевых ордеров: {cancelled_count}")
            
        return True