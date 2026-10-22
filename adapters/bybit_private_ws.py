"""
Bybit Private WebSocket Client (UDS).
Слушает обновления ордеров, позиций и кошелька.
"""

import asyncio
import json
import time
import hmac
import hashlib
import aiohttp
import logging
from typing import Optional
from core.event_bus import EventBus

logger = logging.getLogger("plato.uds")

class BybitPrivateWSClient:
    def __init__(self, api_key: str, api_secret: str, ws_url: str, event_bus: EventBus):
        self.api_key = api_key
        self.api_secret = api_secret
        self.ws_url = ws_url
        self.bus = event_bus
        self._running = False
        self.ws: Optional[aiohttp.ClientWebSocketResponse] = None

    def _sign_auth(self, expires: int) -> str:
        val = f"GET/realtime{expires}"
        return hmac.new(self.api_secret.encode("utf-8"), val.encode("utf-8"), hashlib.sha256).hexdigest()

    async def connect(self):
        from core.metrics import metrics
        
        self._running = True
        session = aiohttp.ClientSession()
        
        while self._running:
            try:
                async with session.ws_connect(self.ws_url) as ws:
                    self.ws = ws
                    logger.info("✅ [UDS] Подключено к приватному WS")
                    
                    metrics.uds.record_connection_attempt(True)
                    
                    # Авторизация
                    expires = int((time.time() * 1000)) + 10000
                    signature = self._sign_auth(expires)
                    auth_msg = {"op": "auth", "args": [self.api_key, expires, signature]}
                    await ws.send_json(auth_msg)
                    
                    auth_resp = await ws.receive_json()
                    if auth_resp.get('success'):
                        logger.info("✅ [UDS] Авторизация успешна")
                        
                        # Подписка на топики
                        sub_msg = {"op": "subscribe", "args": ["order", "position", "wallet"]}
                        await ws.send_json(sub_msg)
                        logger.info("✅ [UDS] Подписка на топики: order, position, wallet")
                        
                        # Запускаем задачу ping/pong для keepalive
                        ping_task = asyncio.create_task(self._ping_loop(ws))
                        
                        try:
                            # Цикл чтения сообщений
                            async for msg in ws:
                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    metrics.uds.record_message()
                                    data = json.loads(msg.data)
                                    if data.get("topic") == "order":
                                        await self.bus.publish("UDS:ORDER_UPDATE", data.get("data", []))
                                    elif data.get("topic") == "position":
                                        await self.bus.publish("UDS:POSITION_UPDATE", data.get("data", []))
                                    elif data.get("topic") == "wallet":
                                        await self.bus.publish("UDS:WALLET_UPDATE", data.get("data", []))
                                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                    break
                        finally:
                            ping_task.cancel()
                    else:
                        logger.error(f"❌ [UDS] Ошибка авторизации: {auth_resp}")
                        metrics.uds.record_connection_attempt(False)
                        await asyncio.sleep(5)
                        
            except Exception as e:
                logger.warning(f"⚠️ [UDS] Ошибка соединения: {e}. Переподключение через 5 сек...")
                metrics.uds.record_connection_attempt(False)
                await asyncio.sleep(5)

    async def _ping_loop(self, ws):
        """Отправляет ping каждые 20 секунд для поддержания соединения."""
        try:
            while True:
                await asyncio.sleep(20)
                await ws.send_json({"op": "ping"})
                logger.debug("🏓 [UDS] Ping отправлен")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"❌ [UDS] Ошибка ping: {e}")

    async def stop(self):
        self._running = False
        if self.ws and not self.ws.closed:
            await self.ws.close()