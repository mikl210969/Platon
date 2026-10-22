"""
Bybit REST Client.
Отвечает за приватные запросы: баланс, ордера, позиции.
"""

import time
import hmac
import hashlib
import json
import aiohttp
from typing import Optional, Any, Dict
from urllib.parse import urlencode

class BybitRestClient:
    def __init__(self, api_key: str, api_secret: str, base_url: str, recv_window: int = 5000):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url
        self.recv_window = str(recv_window)
        self.session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    def _sign(self, timestamp: str, method: str, params: Optional[Dict]) -> str:
        param_str = f"{timestamp}{self.api_key}{self.recv_window}"
        if params:
            if method == 'GET':
                param_str += urlencode(params)
            else:
                param_str += json.dumps(params)
        
        return hmac.new(
            self.api_secret.encode('utf-8'),
            param_str.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

    async def _request(self, method: str, path: str, params: Optional[Dict] = None, signed: bool = False) -> Dict:
        from core.metrics import metrics  # Импортируем здесь, чтобы избежать циклических зависимостей
        
        start_time = time.time()
        try:
            session = await self._get_session()
            timestamp = str(int(time.time() * 1000))
            
            headers: Dict[str, Any] = {
                "X-BAPI-API-KEY": self.api_key,
                "X-BAPI-TIMESTAMP": timestamp,
                "X-BAPI-RECV-WINDOW": self.recv_window,
                "Content-Type": "application/json",
                "User-Agent": "PlatoV2/1.0"
            }
            
            if signed:
                headers["X-BAPI-SIGN"] = self._sign(timestamp, method, params or {})
            
            url = f"{self.base_url}{path}"
            kwargs: Dict[str, Any] = {'headers': headers}
            
            if method == 'GET' and params:
                url += '?' + urlencode(params)
            else:
                kwargs['json'] = params
                
            async with session.request(method, url, **kwargs) as resp:
                data = await resp.json()
                response_time = time.time() - start_time
                
                if data.get('retCode') != 0:
                    error_msg = f"Bybit API Error {data.get('retCode')}: {data.get('retMsg')}"
                    metrics.rest.record_request(False, response_time, error_msg)
                    raise Exception(error_msg)
                
                metrics.rest.record_request(True, response_time)
                return data.get('result', {})
        except Exception as e:
            response_time = time.time() - start_time
            metrics.rest.record_request(False, response_time, str(e))
            raise

    async def get_order(self, category: str, symbol: str, order_id: str) -> Dict:
        params = {"category": category, "symbol": symbol, "orderId": order_id}
        result = await self._request("GET", "/v5/order/realtime", params, signed=True)
        return result.get("list", [{}])[0] if result.get("list") else {}

    async def place_order(self, category, symbol, side, qty, price,
                          order_type="Limit", time_in_force="GTC",
                          stop_loss=None, take_profit=None,
                          trigger_price=None, trigger_direction=None,
                          reduce_only=None, extra_params=None) -> Dict:   # ✅ +extra_params
        params = {
            "category": category, "symbol": symbol, "side": side,
            "orderType": order_type, "qty": qty, "price": price, "timeInForce": time_in_force
        }
        if stop_loss: params["stopLoss"] = stop_loss
        if take_profit: params["takeProfit"] = take_profit
        if trigger_price: params["triggerPrice"] = trigger_price
        if trigger_direction is not None: params["triggerDirection"] = str(trigger_direction)
        if reduce_only is not None: params["reduceOnly"] = str(reduce_only).lower()
        if extra_params: params.update(extra_params)   # ✅ orderLinkId и прочее

        return await self._request("POST", "/v5/order/create", params, signed=True)

    async def place_market_order(self, category: str, symbol: str, side: str, qty: str) -> Dict:
        return await self.place_order(category=category, symbol=symbol, side=side, qty=qty, price="0", order_type="Market", time_in_force="IOC")

    async def cancel_order(self, category: str, symbol: str, order_id: str) -> Dict:
        return await self._request("POST", "/v5/order/cancel", {"category": category, "symbol": symbol, "orderId": order_id}, signed=True)

    async def get_ticker(self, category: str, symbol: str) -> Dict:
        return await self._request("GET", "/v5/market/tickers", {"category": category, "symbol": symbol})

    async def get_balance(self, account_type: str = "UNIFIED") -> Dict:
        return await self._request("GET", "/v5/account/wallet-balance", {"accountType": account_type}, signed=True)

    # ============================================================
    # RECON-методы (источник истины для сверки биржа<->паспорт)
    # ============================================================
    async def get_positions(self, category: str = "linear", symbol: Optional[str] = None) -> list:
        """Открытые позиции. Возвращает список (фильтруем size>0 на стороне RECON)."""
        params = {"category": category}
        if symbol:
            params["symbol"] = symbol
        result = await self._request("GET", "/v5/position/list", params, signed=True)
        return result.get("list", [])

    async def get_open_orders(self, category: str = "linear", symbol: Optional[str] = None) -> list:
        """Все активные ордера (лимитные + условные/триггерные)."""
        params = {"category": category, "openOnly": 0}
        if symbol:
            params["symbol"] = symbol
        result = await self._request("GET", "/v5/order/realtime", params, signed=True)
        return result.get("list", [])

    async def get_closed_pnl_history(self, category: str = "linear", symbol: Optional[str] = None,
                                     limit: int = 20) -> list:
        """История закрытых PnL — для финализации паспорта, чей выход мы пропустили."""
        params = {"category": category, "limit": str(limit)}
        if symbol:
            params["symbol"] = symbol
        result = await self._request("GET", "/v5/position/closed-pnl", params, signed=True)
        return result.get("list", [])

    async def get_trade_history(self, category: str = "linear", symbol: Optional[str] = None,
                                limit: int = 20) -> list:
        """История исполнений (fills) — точные цены/комиссии по плечам."""
        params = {"category": category, "limit": str(limit)}
        if symbol:
            params["symbol"] = symbol
        result = await self._request("GET", "/v5/execution/list", params, signed=True)
        return result.get("list", [])

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()