"""
Менеджер ордеров. Отправляет, модифицирует и отменяет ордера на бирже.
"""
import logging
from typing import Optional
from core.event_bus import EventBus
from adapters.bybit_rest import BybitRestClient
from execution.passport import TradePassport
from execution.order_timer import OrderTimer

logger = logging.getLogger("plato.order_manager")

class OrderManager:
    def __init__(self, rest_client: BybitRestClient, category: str = "linear", event_bus: Optional[EventBus] = None):
        self.rest = rest_client
        self.category = category
        self.bus = event_bus
        self.order_timer = OrderTimer(rest_client, event_bus)

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

    async def place_entry_order(self, passport, limit_price: str, 
                                timeout_seconds: int = 30, 
                                order_link_id: Optional[str] = None,
                                zone_id: Optional[str] = None):  # ← ДОБАВИТЬ ПАРАМЕТР
        try:
            has_position = await self.has_open_position(passport.symbol)
            if has_position:
                passport.add_event("ERROR: POSITION_EXISTS", f"Уже есть открытая позиция по {passport.symbol}")
                logger.warning(f"️ [{passport.passport_id}] Пропуск: уже есть открытая позиция по {passport.symbol}")
                return False

            passport.planned_size = passport.position_size

            passport.sizing_info.update({
                "final_quantity": passport.position_size,
                "base_sl_distance": abs(passport.entry_price - passport.sl_price) / passport.entry_price * 100,
                "sl_distance": abs(passport.entry_price - passport.sl_price) / passport.entry_price * 100
            })

            # ✅ ИСПРАВЛЕНО: используем limit_price вместо price
            passport.add_event("ACTION: PLACE_ENTRY", f"Размещение {passport.side} {passport.position_size} @ {limit_price}")
            logger.info(f"[{passport.passport_id}] Размещение входа: {passport.side} {passport.position_size} @ {limit_price}")

            # V2/Блок4: orderLinkId для сопоставления UDS<->REST
            extra_params = {}
            if order_link_id:
                extra_params["orderLinkId"] = order_link_id

            result = await self.rest.place_order(
                category=self.category,
                symbol=passport.symbol,
                side="Buy" if passport.side == "LONG" else "Sell",
                qty=str(passport.position_size),
                price=limit_price,  # ✅ ИСПРАВЛЕНО: используем limit_price
                order_type="Limit",
                time_in_force="GTC",
                extra_params=extra_params,
            )

            order_id = result.get("orderId")
            if order_id:
                passport.add_order_record(order_id, "LIMIT_ENTRY", "New", float(limit_price),
                                          passport.position_size, client_order_id=order_link_id)
                passport.add_event("SUCCESS: ORDER_PLACED", f"Биржа приняла ордер {order_id}")

                # ✅ V2/Блок5: Передаем zone_id в таймер для условного сброса зоны
                await self.order_timer.start_timer(
                    category=self.category,
                    symbol=passport.symbol,
                    order_id=order_id,
                    timeout_seconds=timeout_seconds,
                    planned_qty=passport.position_size,
                    zone_id=zone_id,  # ← ПЕРЕДАЕМ ZONE_ID
                    fallback_to_market=False
                )

                logger.info(f"✅ [{passport.passport_id}] Вход размещен. Order ID: {order_id}, TTL: {timeout_seconds}s")
                return True
            else:
                passport.add_event("ERROR: NO_ORDER_ID", "Биржа не вернула orderId")
                logger.error(f"❌ [{passport.passport_id}] Биржа не вернула orderId")
                return False

        except Exception as e:
            passport.add_event("ERROR: ENTRY_FAILED", str(e))
            logger.error(f"❌ [{passport.passport_id}] Ошибка входа: {e}")
            return False

    async def place_protective_orders(self, passport: TradePassport) -> bool:
        try:
            qty_half = str(passport.position_size / 2.0)
            qty_full = str(passport.position_size)
            symbol = passport.symbol
            side_exit = "Sell" if passport.side == "LONG" else "Buy"
            
            tp1_res = await self.rest.place_order(
                category=self.category, symbol=symbol, side=side_exit,
                qty=qty_half, price=str(passport.tp1_price),
                order_type="Limit", time_in_force="GTC",
                reduce_only=True
            )
            tp1_id = tp1_res.get("orderId")
            if tp1_id: passport.add_order_record(tp1_id, "LIMIT_TP1", "New", passport.tp1_price, float(qty_half))
            
            tp2_res = await self.rest.place_order(
                category=self.category, symbol=symbol, side=side_exit,
                qty=qty_half, price=str(passport.tp2_price),
                order_type="Limit", time_in_force="GTC",
                reduce_only=True
            )
            tp2_id = tp2_res.get("orderId")
            if tp2_id: passport.add_order_record(tp2_id, "LIMIT_TP2", "New", passport.tp2_price, float(qty_half))

            trigger_dir = 2 if passport.side == "LONG" else 1
            
            sl_res = await self.rest.place_order(
                category=self.category, symbol=symbol, side=side_exit,
                qty=qty_full, price="",
                order_type="Market", 
                trigger_price=str(passport.sl_price),
                trigger_direction=trigger_dir,
                reduce_only=True
            )
            sl_id = sl_res.get("orderId")
            if sl_id: passport.add_order_record(sl_id, "STOP_SL", "New", passport.sl_price, float(qty_full))
            
            if tp1_id and tp2_id and sl_id:
                passport.add_event("SUCCESS: PROTECTIVE_PLACED", f"Защитные ордера: TP1={tp1_id}, TP2={tp2_id}, SL={sl_id}")
                logger.info(f"✅ [{passport.passport_id}] Защитные ордера размещены.")
                return True
            else:
                passport.add_event("ERROR: PROTECTIVE_INCOMPLETE", "Не все ордера размещены")
                logger.error(f"❌ [{passport.passport_id}] Не все защитные ордера размещены")
                return False
            
        except Exception as e:
            passport.add_event("ERROR: PROTECTIVE_FAILED", str(e))
            logger.error(f"❌ [{passport.passport_id}] Ошибка защитных ордеров: {e}")
            return False

    async def move_sl_to_breakeven(self, passport: TradePassport, new_sl_price: str) -> bool:
        try:
            current_sl = next((o for o in passport.orders if o.get("type") == "STOP_SL" and o.get("status") == "New"), None)
            
            if current_sl:
                await self.rest.cancel_order(self.category, passport.symbol, current_sl["order_id"])
                current_sl["status"] = "Cancelled"
                passport.add_event("ACTION: SL_CANCELLED", f"Старый SL {current_sl['order_id']} отменен")
                logger.info(f"🔄 [{passport.passport_id}] Старый SL отменен")

            qty_half = str(passport.position_size / 2.0)
            trigger_dir = 2 if passport.side == "LONG" else 1
            
            res = await self.rest.place_order(
                category=self.category, symbol=passport.symbol, side="Sell" if passport.side == "LONG" else "Buy",
                qty=qty_half, price="", order_type="Market", 
                trigger_price=new_sl_price, trigger_direction=trigger_dir,
                reduce_only=True
            )
            
            new_sl_id = res.get("orderId")
            if new_sl_id:
                passport.add_order_record(new_sl_id, "STOP_SL_BE", "New", float(new_sl_price), float(qty_half))
                passport.add_event("SUCCESS: SL_MOVED_TO_BE", f"Новый SL выставлен по {new_sl_price}")
                logger.info(f"✅ [{passport.passport_id}] SL перенесен в безубыток ({new_sl_price}).")
                return True
            else:
                passport.add_event("ERROR: NO_NEW_SL_ID", "Биржа не вернула orderId для нового SL")
                logger.error(f"❌ [{passport.passport_id}] Биржа не вернула orderId для нового SL")
                return False
            
        except Exception as e:
            passport.add_event("ERROR: MOVE_SL_FAILED", str(e))
            logger.error(f"❌ [{passport.passport_id}] Ошибка переноса SL: {e}")
            return False

    # ✅ НОВЫЙ МЕТОД: Гарантированная очистка всех висящих защитных ордеров
    async def cancel_all_protective_orders(self, passport: TradePassport) -> bool:
        """Отменяет все активные защитные ордера при закрытии позиции."""
        cancelled_count = 0
        protective_types = ["LIMIT_TP1", "LIMIT_TP2", "STOP_SL", "STOP_SL_BE"]
        
        for order in passport.orders:
            order_id = order.get("order_id")
            # ✅ ПРОВЕРКА: order_id не должен быть None
            if not order_id:
                continue
                
            if order.get("type") in protective_types and order.get("status") in ("New", "Untriggered", "Active"):
                try:
                    await self.rest.cancel_order(
                        category=self.category,
                        symbol=passport.symbol,
                        order_id=order_id  # ✅ Теперь точно str
                    )
                    order["status"] = "Cancelled"
                    cancelled_count += 1
                    logger.info(f"🧹 [{passport.passport_id}] Отменен висящий ордер {order.get('type')}: {order_id}")
                except Exception as e:
                    # Игнорируем ошибки, если ордер уже был исполнен или отменен биржей
                    logger.debug(f"Не удалось отменить ордер {order_id}: {e}")
        
        if cancelled_count > 0:
            passport.add_event("ACTION: PROTECTIVE_ORDERS_CANCELLED", f"Очищено биржевых ордеров: {cancelled_count}")
            
        return True