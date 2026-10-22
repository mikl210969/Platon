"""
Bybit Public WebSocket Client.
Подключается к публичным топикам (стакан, трейды, свечи) без API-ключей.
"""

import asyncio
import json
import logging
import aiohttp
from typing import Optional
from core.event_bus import EventBus

logger = logging.getLogger("plato.public_ws")


class BybitPublicWSClient:
    def __init__(self, symbol: str, event_bus: EventBus):
        self.symbol = symbol
        self.bus = event_bus
        self._running = False
        self.ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self.ws_url = "wss://stream.bybit.com/v5/public/spot"
        
        self.topics = [
            f"orderbook.200.{symbol}",
            f"publicTrade.{symbol}",
            f"kline.1.{symbol}",
            f"kline.5.{symbol}",
            f"kline.15.{symbol}",
        ]

    async def connect(self):
        self._running = True
        session = aiohttp.ClientSession()
        
        while self._running:
            try:
                async with session.ws_connect(self.ws_url) as ws:
                    self.ws = ws
                    logger.info("📡 [PublicWS] Подключено к публичному WS")
                    
                    sub_msg = {"op": "subscribe", "args": self.topics}
                    await ws.send_json(sub_msg)
                    logger.info(f"📡 [PublicWS] Подписка: {self.topics}")
                    
                    ping_task = asyncio.create_task(self._ping_loop(ws))
                    
                    try:
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)
                                topic = data.get("topic", "")
                                payload = data.get("data")
                                
                                if not payload:
                                    continue
                                
                                if topic.startswith("orderbook"):
                                    # ✅ ПЕРЕДАЁМ ТИП СООБЩЕНИЯ (snapshot/delta)
                                    await self.bus.publish("PUBLIC:ORDERBOOK", {
                                        "type": data.get("type", "snapshot"),
                                        "data": payload
                                    })
                                elif topic.startswith("publicTrade"):
                                    await self.bus.publish("PUBLIC:TRADES", payload)
                                elif topic.startswith("kline"):
                                    await self.bus.publish("PUBLIC:KLINE", {"topic": topic, "data": payload})
                                    
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                    finally:
                        ping_task.cancel()
                        
            except Exception as e:
                logger.warning(f"⚠️ [PublicWS] Ошибка соединения: {e}. Переподключение через 5 сек...")
                await asyncio.sleep(5)

    async def _ping_loop(self, ws):
        try:
            while True:
                await asyncio.sleep(20)
                await ws.send_json({"op": "ping"})
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"❌ [PublicWS] Ошибка ping: {e}")

    async def stop(self):
        self._running = False
        if self.ws and not self.ws.closed:
            await self.ws.close()