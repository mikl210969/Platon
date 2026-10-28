"""
PLATO V2 — Точка входа. Фаза 2: сбор рыночных данных и аналитика.
V2: добавлен Reconciliation Engine (сверка биржа<->паспорт) и канонические статусы.
✅ МУЛЬТИВАЛЮТНОСТЬ: Поддержка списка символов из конфига.
"""
import json 
import asyncio
import signal
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))

from core.config import Config
from core.event_bus import EventBus
from core.logger import setup_logger
from adapters.bybit_rest import BybitRestClient
from adapters.bybit_private_ws import BybitPrivateWSClient
from adapters.bybit_public_ws import BybitPublicWSClient
from analytics.market_buffer import MarketBuffer
from execution.order_manager import OrderManager
from execution.position_monitor import PositionMonitor
from execution.passport_storage import PassportStorage
from execution.recon_engine import ReconciliationEngine  # V2

from datetime import datetime
from execution.passport import TradePassport, SignalSource


async def signal_generator(rest, om, monitor, storage, interval_seconds: int = 180):
    """Тестовый генератор сигналов. V2: канонические статусы стейт-машины."""
    logger = setup_logger("plato")
    logger.info(f"🎲 Генератор случайных сигналов запущен. Интервал: {interval_seconds} сек")

    trade_counter = 0
    current_side = "LONG"

    while True:
        trade_counter += 1
        has_position = await om.has_open_position("SOLUSDT")
        if has_position:
            logger.info(f"⏳ [{trade_counter}] Позиция уже открыта. Пропускаем сигнал.")
            await asyncio.sleep(interval_seconds)
            continue

        logger.info(f"\n{'='*60} СИГНАЛ #{trade_counter} ({current_side}) {'='*60}")
        ticker = await rest.get_ticker("linear", "SOLUSDT")
        current_price = float(ticker['list'][0]['lastPrice'])

        passport = TradePassport(
            signal_id=f"SIGNAL_{trade_counter:03d}", strategy="AdaptiveZoneV1",
            signal_source=SignalSource.TEST, symbol="SOLUSDT", side=current_side,
            planned_size=7.0, position_size=7.0, entry_price=current_price,
            tp1_price=current_price * 1.005 if current_side == "LONG" else current_price * 0.995,
            tp2_price=current_price * 1.01 if current_side == "LONG" else current_price * 0.99,
            sl_price=current_price * 0.995 if current_side == "LONG" else current_price * 1.005,
        )

        passport.add_event("SIGNAL: GENERATED", f"Сигнал #{trade_counter}: {current_side} @ {current_price}")
        await monitor.bus.publish("EXEC:NEW_TRADE", passport)

        limit_price = str(round(current_price * 0.9998, 2)) if current_side == "LONG" else str(round(current_price * 1.0002, 2))
        success = await om.place_entry_order(passport, limit_price, timeout_seconds=30)

        if success:
            passport.add_event("ACTION: ENTRY_PLACED", f"Лимитный ордер размещен по {limit_price}")
            await asyncio.sleep(60)

            # V2: канонические статусы стейт-машины
            st = passport.status
            if st in ("IDLE", "SIGNAL_GENERATED", "ORDER_PREPARING", "ORDER_SENT", "ORDER_PENDING"):
                # Ордер не исполнился за TTL — монитор/таймер уже перевели в ORDER_CANCELLED;
                # если нет — фиксируем здесь
                if st not in ("ORDER_CANCELLED",):
                    passport.add_event("TIMER: EXPIRED", "Ордер не исполнился за 60 сек")
                logger.info(f"⏱️ [{passport.passport_id}] Статус={st}. Переход к следующему.")
                storage.save(passport)
            elif st in ("POSITION_OPEN", "TP1_PARTIAL_CLOSE", "PARTIAL_FILL"):
                logger.info(f"📂 [{passport.passport_id}] Позиция открыта (статус={st}). Ждем TP1/TP2/SL...")
                await asyncio.sleep(300)
                has_position_now = await om.has_open_position("SOLUSDT")
                if has_position_now:
                    passport.add_event("WARNING: POSITION_STILL_OPEN", "Позиция не закрылась за 5 минут")
                    logger.warning(f"⚠️ [{passport.passport_id}] Позиция все еще открыта!")
                else:
                    passport.add_event("SUCCESS: POSITION_CLOSED", "Позиция закрыта")
                    logger.info(f"✅ [{passport.passport_id}] Позиция закрыта.")
                storage.save(passport)
            elif st in ("CLOSED", "ORDER_CANCELLED", "ORDER_REJECTED", "ERROR_STATE"):
                logger.info(f"⏹ [{passport.passport_id}] Сделка завершена (статус: {st}). Идем дальше.")
                storage.save(passport)
            else:
                logger.info(f" [{passport.passport_id}] Неожиданный статус: {st}. Идем дальше.")
                storage.save(passport)
        else:
            passport.add_event("ERROR: ENTRY_FAILED", "Не удалось разместить ордер")
            storage.save(passport)

        current_side = "SHORT" if current_side == "LONG" else "LONG"
        await asyncio.sleep(interval_seconds)


async def main():
    config = Config("config.yaml")
    logger = setup_logger("plato", "plato.log", config.get("logging.level", "INFO"))

    logger.info("="*60)
    logger.info("PLATO V2 — ФАЗА 2: АНАЛИТИКА (сбор данных)")
    logger.info("="*60)

    # ✅ 1. БЕЗОПАСНАЯ ЗАГРУЗКА СЕКРЕТОВ
    secrets_file = Path("secrets.json")
    if not secrets_file.exists():
        logger.error("❌ ОШИБКА: Файл secrets.json не найден! Создай его на основе secrets.example.json")
        sys.exit(1)  # ✅ Убран локальный import sys
        
    with open(secrets_file, 'r', encoding='utf-8') as f:
        secrets = json.load(f)
        
    bybit_cfg = secrets.get("bybit", {})
    api_key = bybit_cfg.get("api_key")
    api_secret = bybit_cfg.get("api_secret")
    use_demo = bybit_cfg.get("use_demo", True) # Берем из secrets для консистентности

    if not api_key or not api_secret:
        logger.error("❌ ОШИБКА: API ключи пусты в secrets.json. Проверь файл.")
        sys.exit(1)  # ✅ Убран локальный import sys
        
    logger.info("✅ Секреты успешно и безопасно загружены из secrets.json")

    bus = EventBus()
    storage = PassportStorage("data/passports")

    # ✅ 2. ИСПОЛЬЗУЕМ БЕЗОПАСНЫЕ ПЕРЕМЕННЫЕ (а не config.get)
    rest_url = "https://api-demo.bybit.com" if use_demo else "https://api-testnet.bybit.com"
    ws_url = "wss://stream-demo.bybit.com/v5/private" if use_demo else "wss://stream-testnet.bybit.com/v5/private"

    rest = BybitRestClient(api_key, api_secret, rest_url)
    uds = BybitPrivateWSClient(api_key, api_secret, ws_url, bus)

    # ✅ ОБЩИЙ ORDER MANAGER И MONITOR (работают с любым символом через passport.symbol)
    om = OrderManager(rest, category="linear", event_bus=bus)
    monitor = PositionMonitor(config, bus, om, storage)

    # ✅ МУЛЬТИВАЛЮТНОСТЬ: Получаем список активных символов
    symbols = config.get("trading.symbols", ["SOLUSDT"])
    instruments_cfg = config.get("instruments", {})
    
    active_symbols = [
        s for s in symbols 
        if instruments_cfg.get(s, {}).get("enabled", False)
    ]
    
    if not active_symbols:
        logger.error("❌ Нет активных символов в конфиге!")
        sys.exit(1)
        
    logger.info(f" Активные инструменты: {', '.join(active_symbols)}")

    # ✅ СОЗДАЕМ ПУБЛИЧНЫЕ КОМПОНЕНТЫ ДЛЯ КАЖДОГО СИМВОЛА
    public_ws_clients = []
    market_buffers = {}
    
    for symbol in active_symbols:
        pub_ws = BybitPublicWSClient(symbol=symbol, event_bus=bus)
        public_ws_clients.append(pub_ws)
        
        mb = MarketBuffer(event_bus=bus, symbol=symbol)
        market_buffers[symbol] = mb
        
        logger.info(f"📡 Подписка на публичные данные: {symbol}")

    # V2: Reconciliation Engine (источник истины биржа<->паспорт)
    # ✅ ОТДЕЛЬНЫЙ RECON ДЛЯ КАЖДОГО СИМВОЛА
    recons = []
    for symbol in active_symbols:
        recon = ReconciliationEngine(
            event_bus=bus,
            rest=rest,
            storage=storage,
            get_active_passports=lambda: monitor.active_passports,
            get_manager=lambda p: monitor._get_manager(p),
            order_manager=om,
            symbol=symbol,
            category="linear",
            interval_sec=30,
            silence_sec=300,
        )
        recons.append(recon)

    try:
        logger.info("🚀 Запуск компонентов...")
        uds_task = asyncio.create_task(uds.connect())
        
        # Запускаем все публичные WS
        public_ws_tasks = [asyncio.create_task(ws.connect()) for ws in public_ws_clients]

        # ✅ ЕДИНСТВЕННЫЙ REST-ЗАПРОС ДЛЯ РЫНКА: загрузка истории свечей для всех символов
        for symbol, mb in market_buffers.items():
            await mb.bootstrap_atr(rest, "spot", symbol)
            await mb.start()

        logger.info("🔄 Проверка наличия открытых позиций...")
        try:
            # ✅ ИСПРАВЛЕНО: Bybit требует symbol или settleCoin при запросе списка позиций
            positions = await rest._request("GET", "/v5/position/list", {"category": "linear", "settleCoin": "USDT"}, signed=True)
            for pos in positions.get("list", []):
                sym = pos.get("symbol")
                if sym in active_symbols and float(pos.get("size", 0)) > 0:
                    logger.warning(f"⚠️ Найдена открытая позиция: {pos.get('size')} {sym}")
        except Exception as e:
            logger.error(f"❌ Ошибка проверки позиций: {e}")

        # V2: запуск RECON сразу после восстановления соединений (стартовая сверка R1 внутри)
        for recon in recons:
            await recon.start()

        # ✅ Переключатель источника сигналов
        signal_source = config.get("system.signal_source", "off")
        if signal_source == "random":
            asyncio.create_task(signal_generator(rest, om, monitor, storage, interval_seconds=180))
            logger.info("🎲 Источник сигналов: RANDOM (тестовый генератор)")
        else:
            logger.info("🔕 Источник сигналов: ВЫКЛЮЧЕН (только сбор рыночных данных)")

        stop_event = asyncio.Event()
        def signal_handler(sig, frame):
            logger.info("🛑 Получен сигнал завершения (Ctrl+C)...")
            stop_event.set()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        logger.info("✅ Платформа работает. Сбор публичных данных активен.")
        logger.info("💡 Смотри в логе блоки '🔍 [MarketBuffer]' каждые 30 сек")

        # ✅ Запуск стратегии ДЛЯ КАЖДОГО СИМВОЛА
        from analytics.statistics_engine import StatisticsEngine
        from analytics.strategy import AdaptiveZoneV1

        shadow_mode = config.get("system.shadow_mode", True)
        
        # ✅ ОТДЕЛЬНЫЙ STATISTICS ENGINE ДЛЯ КАЖДОГО СИМВОЛА
        stats_engines = {}
        for symbol in active_symbols:
            stats_engine = StatisticsEngine(
                event_bus=bus, 
                window_size=100, 
                min_observations=60,
                symbol=symbol  # ← Передаем символ
            )
            stats_engines[symbol] = stats_engine
        
        strategies = {}
        for symbol in active_symbols:
            strategy = AdaptiveZoneV1(
                event_bus=bus,
                rest_client=rest,
                order_manager=om,
                passport_storage=storage,
                position_monitor=monitor,
                shadow_mode=shadow_mode,
                config=config,
                symbol=symbol
            )
            strategies[symbol] = strategy
            
            if shadow_mode:
                logger.info(f"👻 [{symbol}] Стратегия работает в ТЕНЕВОМ режиме")
            else:
                logger.info(f"🎯 [{symbol}] Стратегия работает в БОЕВОМ режиме")

        await stop_event.wait()

    finally:
        logger.info("🛑 Остановка платформы...")
        for recon in recons:
            await recon.stop()           # V2: сначала останавливаем сверку
        for mb in market_buffers.values():
            await mb.stop()
        for ws in public_ws_clients:
            await ws.stop()
        await uds.stop()
        await rest.close()
        logger.info("✅ Платформа остановлена")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Завершение по Ctrl+C")