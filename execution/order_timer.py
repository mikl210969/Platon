"""
Таймер жизни ордера.
V2/Блок5: Умный TTL + передача zone_id для условного сброса зон.
"""

import asyncio
import logging
from typing import Optional
from core.event_bus import EventBus
from adapters.bybit_rest import BybitRestClient

logger = logging.getLogger("plato.order_timer")

class OrderTimer:
    def __init__(self, rest_client: BybitRestClient, event_bus: Optional[EventBus]):
        self.rest = rest_client
        self.bus = event_bus
        self._active_timers: dict[str, asyncio.Task] = {}

    async def start_timer(self, category: str, symbol: str, order_id: str, 
                          timeout_seconds: int, planned_qty: float, 
                          zone_id: Optional[str] = None,
                          fallback_to_market: bool = False, 
                          side: Optional[str] = None, qty: Optional[str] = None):
        logger.info(f"⏱️ [OrderTimer] Запущен таймер для ордера {order_id}: {timeout_seconds} сек | Zone: {zone_id}")
        
        async def _timer_task():
            await asyncio.sleep(timeout_seconds)
            try:
                order_status = await self.rest.get_order(category, symbol, order_id)
                status = order_status.get("orderStatus")
                cum_exec_qty = float(order_status.get("cumExecQty", 0))
                
                if status in ["New", "Untriggered"]:
                    logger.warning(f"⏱️ [OrderTimer] Ордер {order_id} не исполнился. Отменяю...")
                    await self.rest.cancel_order(category, symbol, order_id)
                    if self.bus:
                        # ✅ V2/Блок5: Публикуем событие с zone_id для условного сброса
                        await self.bus.publish("ORDER:TIMER_CANCELLED_WITH_ZONE", {
                            "order_id": order_id, 
                            "zone_id": zone_id,
                            "cancelled_qty": planned_qty, 
                            "filled_qty": 0,
                            "reason": "TTL_EXPIRED"
                        })
                        
                elif status == "PartiallyFilled":
                    remaining = planned_qty - cum_exec_qty
                    logger.warning(f"️ [OrderTimer] Ордер {order_id} частично исполнен ({cum_exec_qty}/{planned_qty}). Отменяю остаток {remaining}...")
                    await self.rest.cancel_order(category, symbol, order_id)
                    if self.bus:
                        await self.bus.publish("ORDER:TIMER_CANCELLED_WITH_ZONE", {
                            "order_id": order_id,
                            "zone_id": zone_id,
                            "cancelled_qty": remaining, 
                            "filled_qty": cum_exec_qty,
                            "reason": "PARTIAL_FILL_TIMEOUT"
                        })
                else:
                    logger.info(f"✅ [OrderTimer] Ордер {order_id} уже исполнен ({status}).")
            except Exception as e:
                logger.error(f"❌ [OrderTimer] Ошибка проверки ордера {order_id}: {e}")

        task = asyncio.create_task(_timer_task())
        self._active_timers[order_id] = task

    def cancel_timer(self, order_id: str):
        if order_id in self._active_timers:
            self._active_timers[order_id].cancel()
            del self._active_timers[order_id]