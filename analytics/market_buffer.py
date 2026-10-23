"""
Аналитический агрегатор рыночных данных.
Собирает стакан, трейды, свечи → выдаёт снапшот Features раз в секунду.
V2: Добавлен независимый расчет ATR(14) по 5-минутным свечам.
"""

import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from core.event_bus import EventBus

logger = logging.getLogger("plato.market_buffer")


class OrderbookAnalyzer:
    """Анализ стакана — поиск крупных заявок (стен). Ведёт ПОЛНЫЙ стакан (snapshot + delta)."""
    
    def __init__(self, min_wall_size: float = 100.0):
        self.min_wall_size = min_wall_size
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.walls_bid: List[Dict[str, float]] = []
        self.walls_ask: List[Dict[str, float]] = []
    
    def update(self, info: Any):
        if isinstance(info, dict) and "data" in info:
            msg_type = info.get("type", "snapshot")
            payload = info.get("data")
        else:
            msg_type = "snapshot"
            payload = info
        
        if isinstance(payload, list):
            payload = payload[0] if payload else {}
        if not isinstance(payload, dict):
            return
        
        bids_raw = payload.get("b", [])
        asks_raw = payload.get("a", [])
        
        if msg_type == "snapshot":
            self.bids = {}
            self.asks = {}
        
        for p, s in bids_raw:
            price, size = float(p), float(s)
            if size == 0: self.bids.pop(price, None)
            else: self.bids[price] = size
        
        for p, s in asks_raw:
            price, size = float(p), float(s)
            if size == 0: self.asks.pop(price, None)
            else: self.asks[price] = size
        
        self.walls_bid = [
            {"price": round(p, 2), "size": s}
            for p, s in sorted(self.bids.items(), key=lambda x: -x[1])
            if s >= self.min_wall_size
        ]
        self.walls_ask = [
            {"price": round(p, 2), "size": s}
            for p, s in sorted(self.asks.items(), key=lambda x: -x[1])
            if s >= self.min_wall_size
        ]
    
    def get_max_bid_wall(self) -> float:
        return self.walls_bid[0]["size"] if self.walls_bid else 0.0
    
    def get_max_ask_wall(self) -> float:
        return self.walls_ask[0]["size"] if self.walls_ask else 0.0


class DeltaCalculator:
    def __init__(self):
        self.windows: Dict[int, List[tuple]] = {60: [], 300: [], 900: []}
    
    def add_trade(self, price: float, qty: float, side: str, timestamp_ms: int):
        ts = timestamp_ms / 1000.0
        sign = 1.0 if side == "Buy" else -1.0
        signed_qty = qty * sign
        
        for window, buffer in self.windows.items():
            buffer.append((ts, signed_qty))
            cutoff = ts - window
            while buffer and buffer[0][0] < cutoff:
                buffer.pop(0)
    
    def get_velocity(self, window: int) -> float:
        return sum(q for _, q in self.windows.get(window, []))


class ATRCalculator:
    """ATR(14) на свечах (универсальный для любого таймфрейма)."""
    def __init__(self, period: int = 14):
        self.period = period
        self.candles: List[Dict[str, float]] = []
        self.prev_close: Optional[float] = None
    
    def feed_history(self, high: float, low: float, close: float):
        if self.prev_close is None:
            tr = high - low
        else:
            tr = max(high - low, abs(high - self.prev_close), abs(low - self.prev_close))
        
        self.candles.append({"tr": tr, "close": close})
        self.prev_close = close
        
        if len(self.candles) > self.period * 2:
            self.candles = self.candles[-self.period * 2:]
    
    def update_kline(self, kline: Dict[str, Any]):
        confirm = kline.get("confirm", True)
        if confirm is False or confirm == "false":
            return
        
        high = float(kline.get("high", kline.get("h", 0)))
        low = float(kline.get("low", kline.get("l", 0)))
        close = float(kline.get("close", kline.get("c", 0)))
        
        if high == 0 or low == 0 or close == 0:
            return
        
        self.feed_history(high, low, close)
    
    def get_atr(self) -> float:
        if len(self.candles) < self.period:
            return 0.5
        return sum(c["tr"] for c in self.candles[-self.period:]) / self.period


class VolumeProfile:
    def __init__(self, bucket_size: float = 0.1, max_age_seconds: int = 3600 * 4):
        self.bucket_size = bucket_size
        self.max_age = max_age_seconds
        self.buckets: Dict[float, List[tuple]] = defaultdict(list)
    
    def add_trade(self, price: float, qty: float, timestamp_ms: int):
        bucket = round(price / self.bucket_size) * self.bucket_size
        ts = timestamp_ms / 1000.0
        self.buckets[bucket].append((ts, qty))
        
        cutoff = ts - self.max_age
        self.buckets[bucket] = [(t, v) for t, v in self.buckets[bucket] if t >= cutoff]
        if not self.buckets[bucket]:
            del self.buckets[bucket]
    
    def _aggregate(self) -> Dict[float, float]:
        return {round(price, 2): sum(v for _, v in trades) for price, trades in self.buckets.items()}
    
    def get_poc(self) -> Optional[Dict[str, float]]:
        agg = self._aggregate()
        if not agg: return None
        poc_price, poc_vol = max(agg.items(), key=lambda x: x[1])
        return {"price": poc_price, "volume": round(poc_vol, 2)}
    
    def get_hvn(self, top_n: int = 5, min_volume: float = 1.0) -> List[Dict[str, float]]:
        agg = self._aggregate()
        if not agg: return []
        filtered = [(p, v) for p, v in agg.items() if v >= min_volume]
        sorted_levels = sorted(filtered, key=lambda x: x[1], reverse=True)
        return [{"price": p, "volume": round(v, 2)} for p, v in sorted_levels[:top_n]]
    
    def get_value_area(self, pct: float = 0.70) -> Optional[Dict[str, float]]:
        agg = self._aggregate()
        if not agg: return None
        
        total_vol = sum(agg.values())
        target = total_vol * pct
        poc_price = max(agg.items(), key=lambda x: x[1])[0]
        prices = sorted(agg.keys())
        
        poc_idx = prices.index(poc_price)
        lower_idx, upper_idx = poc_idx, poc_idx
        acc_vol = agg[poc_price]
        
        while acc_vol < target and (lower_idx > 0 or upper_idx < len(prices) - 1):
            expand_down, expand_up = lower_idx > 0, upper_idx < len(prices) - 1
            if expand_down and expand_up:
                if agg[prices[lower_idx - 1]] >= agg[prices[upper_idx + 1]]:
                    lower_idx -= 1; acc_vol += agg[prices[lower_idx]]
                else:
                    upper_idx += 1; acc_vol += agg[prices[upper_idx]]
            elif expand_down:
                lower_idx -= 1; acc_vol += agg[prices[lower_idx]]
            elif expand_up:
                upper_idx += 1; acc_vol += agg[prices[upper_idx]]
            else:
                break
        
        return {"vah": prices[upper_idx], "val": prices[lower_idx], "poc": poc_price}


class MarketBuffer:
    def __init__(self, event_bus: EventBus, symbol: str):
        self.bus = event_bus
        self.symbol = symbol
        self.orderbook = OrderbookAnalyzer(min_wall_size=100.0)
        self.delta = DeltaCalculator()
        self.atr = ATRCalculator(period=14)
        self.atr_5m = ATRCalculator(period=14)  # ✅ V2: Отдельный калькулятор для 5m
        self.volume_profile = VolumeProfile(bucket_size=0.1, max_age_seconds=3600 * 4)
        self.current_price: float = 0.0
        
        self.bus.subscribe("PUBLIC:ORDERBOOK", self._on_orderbook)
        self.bus.subscribe("PUBLIC:TRADES", self._on_trades)
        self.bus.subscribe("PUBLIC:KLINE", self._on_kline)
        
        self._snapshot_task = None
        self._trades_count = self._ob_updates = self._klines_count = 0

    async def bootstrap_atr(self, rest, category: str, symbol: str):
        """✅ ЕДИНСТВЕННЫЙ REST-ЗАПРОС ДЛЯ РЫНКА: загрузка истории при старте."""
        try:
            # 1. Загружаем историю для 1m ATR
            result_1m = await rest._request("GET", "/v5/market/kline", {
                "category": "spot", "symbol": symbol, "interval": "1", "limit": "20"
            })
            rows_1m = list(reversed(result_1m.get("list", [])))
            for row in rows_1m:
                self.atr.feed_history(float(row[2]), float(row[3]), float(row[4]))
            
            # 2. ✅ V2: Загружаем историю для 5m ATR
            result_5m = await rest._request("GET", "/v5/market/kline", {
                "category": "spot", "symbol": symbol, "interval": "5", "limit": "20"
            })
            rows_5m = list(reversed(result_5m.get("list", [])))
            for row in rows_5m:
                self.atr_5m.feed_history(float(row[2]), float(row[3]), float(row[4]))
                
            logger.info(f"📚 [MarketBuffer] История загружена. ATR(1m): {self.atr.get_atr():.3f}, ATR(5m): {self.atr_5m.get_atr():.3f}")
        except Exception as e:
            logger.warning(f"⚠️ [MarketBuffer] Не удалось загрузить историю ATR (будет прогрев через WS): {e}")

    async def start(self):
        self._snapshot_task = asyncio.create_task(self._snapshot_loop())
        logger.info("🔬 [MarketBuffer] Запущен. Публикация снапшотов 1 раз/сек.")
    
    async def stop(self):
        if self._snapshot_task: self._snapshot_task.cancel()
    
    async def _on_orderbook(self, info):
        self.orderbook.update(info)
        self._ob_updates += 1
    
    async def _on_trades(self, payload):
        for trade in (payload if isinstance(payload, list) else [payload]):
            price, qty, side, ts = float(trade.get("p", 0)), float(trade.get("v", 0)), trade.get("S", "Buy"), int(trade.get("T", 0))
            if price > 0:
                self.current_price = price
                self.delta.add_trade(price, qty, side, ts)
                self.volume_profile.add_trade(price, qty, ts)
                self._trades_count += 1
    
    async def _on_kline(self, payload):
        topic, data = payload.get("topic", ""), payload.get("data", [])
        if isinstance(data, list) and data:
            kline = data[-1]
        elif isinstance(data, dict):
            kline = data
        else:
            return
        
        if not isinstance(kline, dict):
            return
        
        if topic.startswith("kline.1"):
            self.atr.update_kline(kline)  # ✅ Переименовано в универсальное
            self._klines_count += 1
            if self.current_price == 0:
                self.current_price = float(kline.get("close", kline.get("c", 0)))
        elif topic.startswith("kline.5"):
            # ✅ V2: Кормим 5-минутный калькулятор
            self.atr_5m.update_kline(kline)
    
    async def _snapshot_loop(self):
        tick = 0
        try:
            while True:
                await asyncio.sleep(1.0)
                tick += 1
                await self.bus.publish("MARKET:SNAPSHOT", self.get_snapshot())
                
                if tick % 30 == 0:
                    poc = self.volume_profile.get_poc()
                    hvn = self.volume_profile.get_hvn(3)
                    poc_str = f"{poc['price']:.2f}" if poc else "-"
                    logger.info(
                        f"🔍 [MarketBuffer] price={self.current_price:.2f} | ATR(1m)={self.atr.get_atr():.3f} | ATR(5m)={self.atr_5m.get_atr():.3f} | "
                        f"Δ5m={self.delta.get_velocity(300):+.1f} | BidWall={self.orderbook.get_max_bid_wall():.0f} "
                        f"AskWall={self.orderbook.get_max_ask_wall():.0f} | POC={poc_str}"
                    )
                    if hvn:
                        logger.info(f"   🔷 HVN: " + ", ".join(f"{h['price']:.2f}:{h['volume']:.0f}" for h in hvn))
                    logger.info(f"   📊 Счётчики: трейдов={self._trades_count}, OB={self._ob_updates}, klines={self._klines_count}")
        except asyncio.CancelledError:
            pass

    def get_snapshot(self) -> Dict[str, Any]:
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "current_price": self.current_price,
            "atr": self.atr.get_atr(),
            "atr_5m": self.atr_5m.get_atr(),  # ✅ V2: Добавлено в снапшот
            "walls": {
                "walls_bid": self.orderbook.walls_bid, "walls_ask": self.orderbook.walls_ask,
                "bid_v1_max": self.orderbook.get_max_bid_wall(), "ask_v1_max": self.orderbook.get_max_ask_wall(),
            },
            "delta": {
                "windows": {
                    60: {"velocity": self.delta.get_velocity(60)},
                    300: {"velocity": self.delta.get_velocity(300)},
                    900: {"velocity": self.delta.get_velocity(900)},
                }
            },
            "volume_profile": {
                "poc": self.volume_profile.get_poc(),
                "hvn": self.volume_profile.get_hvn(5),
                "value_area": self.volume_profile.get_value_area(),
            },
        }