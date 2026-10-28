"""
BACKGROUND RECORDER v2.0 - BULLETPROOF EDITION
Features: Auto-reconnect, Heartbeat (Ping), Memory Safety, File Logging.
"""

import asyncio
import websockets
import json
import csv
import os
import sys
import logging
from datetime import datetime, timezone
from pathlib import Path
import time

# ================= CONFIGURATION =================
SYMBOLS = [
    "SOLUSDT", "BTCUSDT", "ETHUSDT", 
    "BNBUSDT", "XRPUSDT", "LINKUSDT", "AVAXUSDT"
]

BASE_OUTPUT_DIR = r"C:\Users\user\YandexDisk\data21"
WS_URL_SPOT = "wss://stream.bybit.com/v5/public/spot"
PING_INTERVAL_SEC = 15      # Как часто слать ping Bybit
RECONNECT_DELAY_SEC = 5     # Пауза перед переподключением
FLUSH_INTERVAL_SEC = 5      # Как часто сбрасывать буфер на диск

# Настройка логирования в файл и консоль
LOG_FILE = os.path.join(BASE_OUTPUT_DIR, "recorder_log.txt")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("Recorder")

class DualDataRecorder:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.dir_path = Path(BASE_OUTPUT_DIR) / symbol
        self.dir_path.mkdir(parents=True, exist_ok=True)
        
        # Имя файла привязано к дате UTC при создании объекта
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        self.ob_file_path = self.dir_path / f"ob_snapshot_{today_str}.csv"
        self.trade_file_path = self.dir_path / f"trades_raw_{today_str}.csv"
        
        self.ob_buffer = []
        self.trade_buffer = []
        self.last_flush_time = time.time()
        
        # Создание файлов с заголовками, если их нет
        self._ensure_headers(self.ob_file_path, ["timestamp_utc", "bid_p_1", "bid_v_1", "ask_p_1", "ask_v_1"])
        self._ensure_headers(self.trade_file_path, ["timestamp_ms", "price", "qty", "side"])
        
        logger.info(f"[{symbol}] Recorder initialized.")

    def _ensure_headers(self, filepath: Path, headers: list):
        if not filepath.exists():
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(headers)

    def process_orderbook(self, message: dict):
        data = message.get("data", {})
        bids = data.get("b", [])
        asks = data.get("a", [])
        
        if not bids or not asks: return
        
        try:
            best_bid_p = float(bids[0][0])
            best_bid_q = float(bids[0][1])
            best_ask_p = float(asks[0][0])
            best_ask_q = float(asks[0][1])
            
            row = [datetime.now(timezone.utc).isoformat(), best_bid_p, best_bid_q, best_ask_p, best_ask_q]
            self.ob_buffer.append(row)
        except Exception as e:
            logger.error(f"[{self.symbol}] OB Parse Error: {e}")

    def process_trade(self, message: dict):
        data_list = message.get("data", [])
        if not isinstance(data_list, list): return
        
        for t in data_list:
            try:
                ts_ms = int(t.get("T", 0))
                price = float(t.get("p", 0))
                qty = float(t.get("v", 0))
                side = t.get("S", "Unknown")
                
                if price > 0 and qty > 0:
                    row = [ts_ms, price, qty, side]
                    self.trade_buffer.append(row)
            except Exception as e:
                logger.error(f"[{self.symbol}] Trade Parse Error: {e}")

    def flush_to_disk(self):
        current_time = time.time()
        should_flush = False
        
        # Условие 1: Прошло время
        if current_time - self.last_flush_time > FLUSH_INTERVAL_SEC:
            should_flush = True
            
        # Условие 2: Буферы большие (защита от OOM)
        if len(self.ob_buffer) > 500 or len(self.trade_buffer) > 1000:
            should_flush = True
            
        if not should_flush:
            return

        try:
            # Flush OrderBook
            if self.ob_buffer:
                with open(self.ob_file_path, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerows(self.ob_buffer)
                self.ob_buffer.clear()
                
            # Flush Trades
            if self.trade_buffer:
                with open(self.trade_file_path, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerows(self.trade_buffer)
                self.trade_buffer.clear()
                
            self.last_flush_time = current_time
            # logger.debug(f"[{self.symbol}] Flushed to disk.")
            
        except Exception as e:
            logger.critical(f"[{self.symbol}] DISK WRITE ERROR: {e}")
            # При ошибке диска лучше остановить поток, чтобы не терять данные бесконечно
            raise e

async def run_symbol_worker(symbol: str):
    """
    Основной воркер с автоматическим переподключением.
    """
    recorder = DualDataRecorder(symbol)
    
    while True: # Бесконечный цикл переподключения
        try:
            async with websockets.connect(WS_URL_SPOT, ping_interval=None) as ws:
                logger.info(f"[{symbol}] Connected successfully.")
                
                # Подписка на темы
                topics = [f"orderbook.1.{symbol}", f"publicTrade.{symbol}"]
                sub_msg = {"op": "subscribe", "args": topics}
                await ws.send(json.dumps(sub_msg))
                logger.info(f"[{symbol}] Subscribed to {topics}")
                
                # Запуск фоновой задачи для отправки PING
                ping_task = asyncio.create_task(send_ping(ws, symbol))
                
                try:
                    while True:
                        raw_message = await ws.recv()
                        
                        # Обработка ответа на pong
                        if raw_message == "pong":
                            continue
                            
                        message = json.loads(raw_message)
                        topic = message.get("topic", "")
                        
                        if "orderbook" in topic:
                            recorder.process_orderbook(message)
                        elif "publicTrade" in topic:
                            recorder.process_trade(message)
                        elif topic == "" and message.get("success"):
                            pass # Ответ на подписку
                            
                        # Периодический сброс данных
                        recorder.flush_to_disk()
                        
                finally:
                    # Остановка ping task при выходе из внутреннего цикла
                    ping_task.cancel()
                    try:
                        await ping_task
                    except asyncio.CancelledError:
                        pass
                        
        except websockets.exceptions.ConnectionClosed as e:
            logger.warning(f"[{symbol}] Connection closed: {e.code} {e.reason}. Reconnecting in {RECONNECT_DELAY_SEC}s...")
            await asyncio.sleep(RECONNECT_DELAY_SEC)
        except Exception as e:
            logger.error(f"[{symbol}] Critical Worker Error: {e}. Reconnecting in {RECONNECT_DELAY_SEC}s...")
            await asyncio.sleep(RECONNECT_DELAY_SEC)

async def send_ping(ws, symbol: str):
    """Фоновая задача для отправки heartbeat."""
    try:
        while True:
            await asyncio.sleep(PING_INTERVAL_SEC)
            await ws.send(json.dumps({"op": "ping"}))
            # logger.debug(f"[{symbol}] Sent Ping")
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"[{symbol}] Ping Task Error: {e}")

async def main():
    logger.info("="*60)
    logger.info(" STARTING BACKGROUND RECORDERS v2.0")
    logger.info(f" Symbols: {', '.join(SYMBOLS)}")
    logger.info(f" Output Dir: {BASE_OUTPUT_DIR}")
    logger.info("="*60)
    
    tasks = [run_symbol_worker(sym) for sym in SYMBOLS]
    
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.info("Received interrupt signal. Shutting down gracefully...")
        # Финальный сброс буферов
        for task in tasks:
            # Так как задачи запущены внутри gather, нужно обратиться к ним напрямую сложно.
            # Но так как у нас есть глобальные объекты recorder внутри функций, 
            # проще всего полагаться на то, что flush происходит каждые 5 сек.
            pass
    except Exception as e:
        logger.critical(f"Main Loop Crashed: {e}")

if __name__ == "__main__":
    # Windows fix for ProactorEventLoop
    if hasattr(asyncio, 'WindowsProactorEventLoopPolicy'):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        
    asyncio.run(main())