import asyncio
import websockets
import json
import csv
import os
from datetime import datetime, timezone
from pathlib import Path

# ================= CONFIGURATION =================
SYMBOLS = [
    "SOLUSDT", "BTCUSDT", "ETHUSDT", 
    "BNBUSDT", "XRPUSDT", "LINKUSDT", "AVAXUSDT"
]

BASE_OUTPUT_DIR = r"C:\Users\user\YandexDisk\data21"
WS_URL_SPOT = "wss://stream.bybit.com/v5/public/spot" # Или linear, если фьючерсы
FLUSH_INTERVAL_SEC = 5  

class DualDataRecorder:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.dir_path = Path(BASE_OUTPUT_DIR) / symbol
        self.dir_path.mkdir(parents=True, exist_ok=True)
        
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        # Файл 1: Стакан (L1)
        self.ob_file_path = self.dir_path / f"ob_snapshot_{today_str}.csv"
        self.ob_buffer = []
        if not self.ob_file_path.exists():
            with open(self.ob_file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(["timestamp_utc", "bid_p_1", "bid_v_1", "ask_p_1", "ask_v_1"])

        # Файл 2: Лента Сделок (Trades) - КРИТИЧЕСКИ ВАЖНО ДЛЯ ДЕЛЬТЫ
        self.trade_file_path = self.dir_path / f"trades_raw_{today_str}.csv"
        self.trade_buffer = []
        if not self.trade_file_path.exists():
            with open(self.trade_file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(["timestamp_ms", "price", "qty", "side"]) # side: Buy/Sell

        self.last_flush_time = datetime.now()
        print(f"📝 Recorder initialized for {symbol}")

    def process_orderbook(self, message: dict):
        """Обрабатывает сообщения orderbook"""
        data = message.get("data", {})
        bids = data.get("b", [])
        asks = data.get("a", [])
        
        if not bids or not asks: return
        
        best_bid_p = float(bids[0][0])
        best_bid_q = float(bids[0][1])
        best_ask_p = float(asks[0][0])
        best_ask_q = float(asks[0][1])
        
        row = [datetime.now(timezone.utc).isoformat(), best_bid_p, best_bid_q, best_ask_p, best_ask_q]
        self.ob_buffer.append(row)

    def process_trade(self, message: dict):
        """Обрабатывает сообщения trade"""
        # Bybit Spot Trade format: {"topic":"publicTrade.SOLUSDT","ts":...,"type":"snapshot","data":[{"T":..., "p":..., "v":..., "S":"Buy"}]}
        data_list = message.get("data", [])
        if not isinstance(data_list, list): return
        
        for t in data_list:
            ts_ms = int(t.get("T", 0))
            price = float(t.get("p", 0))
            qty = float(t.get("v", 0))
            side = t.get("S", "Unknown") # Buy or Sell
            
            if price > 0 and qty > 0:
                row = [ts_ms, price, qty, side]
                self.trade_buffer.append(row)

    def flush_to_disk(self):
        now = datetime.now()
        if (now - self.last_flush_time).total_seconds() > FLUSH_INTERVAL_SEC or len(self.ob_buffer) > 100 or len(self.trade_buffer) > 500:
            
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
                
            self.last_flush_time = now

async def run_symbol_worker(symbol: str):
    recorder = DualDataRecorder(symbol)
    
    # Подписка на ОБА потока: Orderbook L1 и Public Trades
    topics = [f"orderbook.1.{symbol}", f"publicTrade.{symbol}"]
    
    async with websockets.connect(WS_URL_SPOT) as ws:
        sub_msg = {"op": "subscribe", "args": topics}
        await ws.send(json.dumps(sub_msg))
        print(f"✅ Subscribed to {symbol} (OB + Trades)")
        
        while True:
            try:
                raw_message = await ws.recv()
                if raw_message == "pong": continue
                message = json.loads(raw_message)
                
                topic = message.get("topic", "")
                if "orderbook" in topic:
                    recorder.process_orderbook(message)
                elif "publicTrade" in topic:
                    recorder.process_trade(message)
                    
                # Периодический сброс
                recorder.flush_to_disk()
                
            except websockets.exceptions.ConnectionClosed:
                print(f"🔄 Connection closed for {symbol}. Reconnecting...")
                await asyncio.sleep(2)
                break
            except Exception as e:
                print(f"❌ Error for {symbol}: {e}")
                await asyncio.sleep(1)

async def main():
    tasks = [run_symbol_worker(sym) for sym in SYMBOLS]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())