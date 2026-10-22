"""
Полный цикл сделки: вход → TP1 → безубыток → TP2 → закрытие.
Запускается 10 раз с разными параметрами.
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime

sys.path.append(str(Path(__file__).parent))

from core.config import Config
from core.event_bus import EventBus
from core.logger import setup_logger
from adapters.bybit_rest import BybitRestClient
from execution.passport import TradePassport, SignalSource, CloseReason
from execution.order_manager import OrderManager
from execution.position_monitor import PositionMonitor

async def simulate_trade(rest, om, monitor, bus, trade_num: int):
    """Симулирует одну полную сделку."""
    logger = setup_logger("plato.test", "test_full_cycle.log", "INFO")
    logger.info(f"\n{'='*60}")
    logger.info(f"СДЕЛКА #{trade_num}")
    logger.info(f"{'='*60}")
    
    # 1. Получаем текущую цену
    ticker = await rest.get_ticker("linear", "SOLUSDT")
    current_price = float(ticker['list'][0]['lastPrice'])
    
    # 2. Создаем паспорт
    passport = TradePassport(
        trade_id=f"TEST_{trade_num:03d}",
        signal_source=SignalSource.TEST,
        symbol="SOLUSDT",
        side="Buy",
        qty=0.1,
        entry_price=current_price,
        tp1_price=current_price * 1.005,  # +0.5%
        tp2_price=current_price * 1.01,   # +1%
        sl_price=current_price * 0.995,   # -0.5%
        signal_time=datetime.now()
    )
    
    logger.info(f"Создан паспорт: {passport.to_dict()}")
    
    # 3. Размещаем лимитный ордер (очень близко к рынку, чтобы быстро исполнился)
    limit_price = str(round(current_price * 0.999, 2))  # На 0.1% ниже рынка
    logger.info(f"Размещение лимитного ордера: Buy 0.1 @ {limit_price} (рынок: {current_price})")
    
    success = await om.place_entry_order(passport, limit_price, timeout_seconds=60)
    if not success:
        logger.error("❌ Не удалось разместить ордер")
        return False
    
    # 4. Ждем исполнения (максимум 60 секунд)
    logger.info("⏳ Ожидание исполнения ордера...")
    for i in range(60):
        await asyncio.sleep(1)
        if passport.status == "OPEN" and passport.entry_order_id:
            try:
                order_info = await rest.get_order("linear", "SOLUSDT", passport.entry_order_id)
                if order_info.get("orderStatus") == "Filled":
                    logger.info(f"✅ Ордер исполнен по {order_info.get('avgPrice')}")
                    break
            except Exception:
                pass
    
    # 5. Если ордер не исполнился — отменяем и переходим к следующей сделке
    if passport.status != "OPEN" or not passport.entry_order_id:
        logger.warning("⚠️ Ордер не исполнился за 60 сек. Пропускаем сделку.")
        return False
    
    # 6. Ждем размещения защитных ордеров (TP1, TP2, SL)
    logger.info("⏳ Ожидание размещения защитных ордеров...")
    await asyncio.sleep(5)
    
    if not passport.tp1_order_id:
        logger.error("❌ Защитные ордера не размещены")
        return False
    
    logger.info(f"✅ Защитные ордера размещены:")
    logger.info(f"   TP1: {passport.tp1_order_id} @ {passport.tp1_price}")
    logger.info(f"   TP2: {passport.tp2_order_id} @ {passport.tp2_price}")
    logger.info(f"   SL:  {passport.sl_order_id} @ {passport.sl_price}")
    
    # 7. Симулируем сработку TP1 (отменяем TP1 и выставляем рыночный ордер для закрытия 50%)
    logger.info(f"🚀 Симуляция сработки TP1...")
    await rest.cancel_order("linear", "SOLUSDT", passport.tp1_order_id)
    
    # Закрываем 50% позиции по рыночной цене
    await rest.place_market_order("linear", "SOLUSDT", "Sell", "0.05")
    passport.tp1_time = datetime.now()
    passport.pnl_tp1 = passport.calculate_pnl(passport.tp1_price, 0.05)
    
    # 8. Переносим SL в безубыток
    logger.info(f"🔄 Перенос SL в безубыток...")
    await om.move_sl_to_breakeven(passport, str(passport.entry_price))
    
    # 9. Симулируем сработку TP2
    logger.info(f"🚀 Симуляция сработки TP2...")
    await rest.cancel_order("linear", "SOLUSDT", passport.tp2_order_id)
    await rest.cancel_order("linear", "SOLUSDT", passport.sl_order_id)
    
    # Закрываем остаток по рыночной цене
    await rest.place_market_order("linear", "SOLUSDT", "Sell", "0.05")
    passport.tp2_time = datetime.now()
    passport.close_time = datetime.now()
    passport.pnl_tp2 = passport.calculate_pnl(passport.tp2_price, 0.05)
    passport.pnl_total = (passport.pnl_tp1 or 0) + (passport.pnl_tp2 or 0)
    passport.status = "CLOSED"
    passport.close_reason = CloseReason.TP2
    
    logger.info(f"🏁 Сделка закрыта")
    logger.info(f"📋 Финальный паспорт:")
    for key, value in passport.to_dict().items():
        logger.info(f"   {key}: {value}")
    
    return True

async def main():
    logger = setup_logger("plato", "test_full_cycle.log", "INFO")
    logger.info("="*60)
    logger.info("ТЕСТ ПОЛНОГО ЦИКЛА (10 СДЕЛОК)")
    logger.info("="*60)
    
    config = Config("config.yaml")
    bus = EventBus()
    
    use_demo = config.get("bybit.use_demo")
    rest_url = "https://api-demo.bybit.com" if use_demo else "https://api-testnet.bybit.com"
    
    rest = BybitRestClient(
        config.get("bybit.api_key"),
        config.get("bybit.api_secret"),
        rest_url
    )
    
    om = OrderManager(rest, category="linear")
    monitor = PositionMonitor(bus, om)
    
    successful_trades = 0
    
    for i in range(1, 11):
        success = await simulate_trade(rest, om, monitor, bus, i)
        if success:
            successful_trades += 1
        
        # Пауза между сделками
        await asyncio.sleep(5)
    
    logger.info(f"\n{'='*60}")
    logger.info(f"РЕЗУЛЬТАТ: {successful_trades}/10 сделок завершены успешно")
    logger.info(f"{'='*60}")
    
    await rest.close()

if __name__ == "__main__":
    asyncio.run(main())