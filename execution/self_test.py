"""
Самопроверка платформы.
Каждые N секунд генерирует тестовый сигнал, размещает ордер и ждет отмены таймером.
Если тест не прошел — платформа останавливает прием реальных сигналов.
"""

import asyncio
import logging
from typing import Optional
from core.event_bus import EventBus
from execution.passport import TradePassport
from execution.order_manager import OrderManager
from adapters.bybit_rest import BybitRestClient

logger = logging.getLogger("plato.self_test")

class SelfTest:
    def __init__(
        self,
        event_bus: EventBus,
        order_manager: OrderManager,
        rest_client: BybitRestClient,
        interval_seconds: int = 120,
        test_qty: float = 0.01,
        price_offset_percent: float = 10.0,
        symbol: str = "SOLUSDT",
        category: str = "linear"
    ):
        self.bus = event_bus
        self.om = order_manager
        self.rest = rest_client
        self.interval = interval_seconds
        self.test_qty = test_qty
        self.price_offset = price_offset_percent / 100.0
        self.symbol = symbol
        self.category = category
        self._running = False
        self._consecutive_failures = 0
        self.max_failures = 3  # После 3 подряд неудач — стоп

    async def start(self):
        """Запускает цикл самопроверки."""
        self._running = True
        logger.info(f"🔄 [SelfTest] Запущен. Интервал: {self.interval} сек")
        
        while self._running:
            try:
                success = await self._run_test()
                if success:
                    self._consecutive_failures = 0
                    logger.info(f"✅ [SelfTest] Тест пройден. Следующий через {self.interval} сек")
                else:
                    self._consecutive_failures += 1
                    logger.warning(f"⚠️ [SelfTest] Тест не пройден. Подряд неудач: {self._consecutive_failures}/{self.max_failures}")
                    
                    if self._consecutive_failures >= self.max_failures:
                        logger.critical(" [SelfTest] Превышен лимит неудач. Платформа остановлена.")
                        break
            except Exception as e:
                logger.error(f"❌ [SelfTest] Ошибка в цикле: {e}")
            
            # Ждем до следующего теста
            await asyncio.sleep(self.interval)

    async def _run_test(self) -> bool:
        """Выполняет один тестовый цикл: создать ордер -> подождать отмены таймером."""
        try:
            # 1. Получаем текущую цену
            ticker = await self.rest.get_ticker(self.category, self.symbol)
            current_price = float(ticker['list'][0]['lastPrice'])
            
            # 2. Создаем тестовый паспорт
            passport = TradePassport(
                symbol=self.symbol,
                side="Buy",
                qty=self.test_qty,
                entry_price=current_price,
                tp1_price=current_price * 1.01,
                tp2_price=current_price * 1.02,
                sl_price=current_price * 0.99
            )
            
            # 3. Размещаем ордер на 10% ниже рынка (гарантированно не исполнится)
            test_price = str(round(current_price * (1 - self.price_offset), 2))
            logger.info(f" [SelfTest] Размещение тестового ордера: {passport.side} {passport.qty} @ {test_price} (рынок: {current_price})")
            
            # 4. Размещаем ордер с таймером 30 секунд (без fallback)
            success = await self.om.place_entry_order(passport, test_price, timeout_seconds=30)
            if not success:
                return False
            
            # 5. Ждем, пока таймер сам отменит ордер (35 секунд с запасом)
            logger.info(f"⏳ [SelfTest] Ожидание отмены ордера таймером...")
            await asyncio.sleep(35)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ [SelfTest] Ошибка в тесте: {e}")
            return False

    async def stop(self):
        """Останавливает цикл самопроверки."""
        self._running = False
        logger.info(" [SelfTest] Остановлен")