"""
Восстановление состояния платформы после перезапуска.
"""

import logging
from datetime import datetime
from execution.passport import TradePassport, SignalSource

logger = logging.getLogger("plato.state_recovery")

async def recover_state(rest, category: str, symbol: str, monitor, storage):
    """Восстанавливает состояние платформы из биржи."""
    logger.info("🔄 Восстановление состояния...")
    
    try:
        # 1. Проверяем позицию
        positions = await rest._request("GET", "/v5/position/list", 
                                       {"category": category, "symbol": symbol}, signed=True)
        
        position_list = positions.get("list", [])
        for pos in position_list:
            size = float(pos.get("size", 0))
            if size > 0:
                logger.info(f"✅ Найдена открытая позиция: {size} {symbol}")
                
                # Создаем паспорт для существующей позиции
                passport = TradePassport(
                    signal_id="RECOVERED",
                    strategy="AdaptiveZoneV1",
                    signal_source=SignalSource.MANUAL,
                    symbol=symbol,
                    side="LONG" if pos.get("side") == "Buy" else "SHORT",
                    planned_size=size,
                    position_size=size,
                    entry_price=float(pos.get("avgPrice", 0)),
                )
                
                passport.status = "RECOVERED"
                passport.add_event("RECOVERY: POSITION_FOUND", f"Позиция восстановлена из биржи")
                
                # Ищем ордера
                orders = await rest._request("GET", "/v5/order/realtime", 
                                            {"category": category, "symbol": symbol, "openOnly": 0}, signed=True)
                
                for order in orders.get("list", []):
                    if order.get("orderStatus") not in ["Cancelled", "Filled"]:
                        order_type = "LIMIT_ENTRY" if order.get("orderType") == "Limit" else "STOP_SL"
                        passport.add_order_record(
                            order.get("orderId"),
                            order_type,
                            order.get("orderStatus"),
                            float(order.get("price", 0)),
                            float(order.get("qty", 0))
                        )
                
                # Добавляем в мониторинг
                await monitor.bus.publish("EXEC:NEW_TRADE", passport)
                storage.save(passport)
                
                logger.info(f"✅ Позиция восстановлена: {passport.passport_id}")
                return passport
        
        logger.info("️ Открытых позиций не найдено")
        return None
        
    except Exception as e:
        logger.error(f"❌ Ошибка восстановления: {e}")
        return None