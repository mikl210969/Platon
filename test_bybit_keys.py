"""
Тест ключей Bybit DEMO.
Открывает лимитный ордер, ждет 30 секунд, проверяет статус, отменяет.
"""

import asyncio
import time
import hmac
import hashlib
import json
import aiohttp
from typing import Optional, Any
from urllib.parse import urlencode

# ============================================================================
# НАСТРОЙКИ ОКРУЖЕНИЯ
# ============================================================================
USE_TESTNET = False  
USE_DEMO = True      

API_KEY = "rUr5nHDI14reL5iUcX".strip()
API_SECRET = "K4qQjqdfNp7ft2Obgf2TaNFRcIAq7ml3O81A".strip()

if USE_TESTNET:
    BASE_URL = "https://api-testnet.bybit.com"
elif USE_DEMO:
    BASE_URL = "https://api-demo.bybit.com"
else:
    BASE_URL = "https://api.bybit.com"

RECV_WINDOW = 5000


def sign_request(timestamp: str, method: str, params: Optional[dict]) -> str:
    """Создает подпись для Bybit V5 API."""
    param_str = f"{timestamp}{API_KEY}{RECV_WINDOW}"
    if params:
        if method == 'GET':
            param_str += urlencode(params)
        else:
            param_str += json.dumps(params)
            
    return hmac.new(
        API_SECRET.encode('utf-8'),
        param_str.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()


async def send_request(method: str, path: str, params: Optional[dict] = None, signed: bool = False) -> dict:
    """Отправляет REST-запрос на Bybit."""
    timestamp = str(int(time.time() * 1000))
    
    headers: dict[str, Any] = {
        "X-BAPI-API-KEY": API_KEY,
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": str(RECV_WINDOW),
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    if not USE_TESTNET and USE_DEMO:
        headers["X-BAPI-DEMO-TRADING"] = "1"
    
    if signed:
        headers["X-BAPI-SIGN"] = sign_request(timestamp, method, params or {})
    
    url = f"{BASE_URL}{path}"
    
    request_kwargs: dict[str, Any] = {'headers': headers}
    
    if method == 'GET' and params:
        url += '?' + urlencode(params)
    else:
        request_kwargs['json'] = params
    
    async with aiohttp.ClientSession() as session:
        async with session.request(method, url, **request_kwargs) as resp:  # type: ignore
            content_type = resp.headers.get('Content-Type', '')
            if 'application/json' not in content_type:
                text = await resp.text()
                raise Exception(f"Биржа вернула не JSON (HTTP {resp.status}): {text[:200]}")
            
            data = await resp.json()
            return data


async def get_server_time() -> int:
    result = await send_request("GET", "/v5/market/time")
    if result.get("retCode") == 0:
        return int(result["result"]["timeNano"])
    else:
        raise Exception(f"Ошибка времени: {result}")


async def get_balance() -> dict:
    result = await send_request("GET", "/v5/account/wallet-balance", {"accountType": "UNIFIED"}, signed=True)
    if result.get("retCode") == 0:
        return result["result"]["list"][0]
    else:
        raise Exception(f"Ошибка баланса: {result}")


async def place_order(symbol: str, side: str, qty: float, price: float) -> dict:
    params = {
        "category": "linear",
        "symbol": symbol,
        "side": side,
        "orderType": "Limit",
        "qty": str(qty),
        "price": str(price),
        "timeInForce": "GTC"
    }
    result = await send_request("POST", "/v5/order/create", params, signed=True)
    if result.get("retCode") == 0:
        return result["result"]
    else:
        raise Exception(f"Ошибка ордера: {result}")


async def get_order(symbol: str, order_id: str) -> dict:
    params = {"category": "linear", "symbol": symbol, "orderId": order_id}
    result = await send_request("GET", "/v5/order/realtime", params, signed=True)
    if result.get("retCode") == 0:
        return result["result"]["list"][0]
    else:
        raise Exception(f"Ошибка статуса: {result}")


async def cancel_order(symbol: str, order_id: str) -> dict:
    params = {"category": "linear", "symbol": symbol, "orderId": order_id}
    result = await send_request("POST", "/v5/order/cancel", params, signed=True)
    if result.get("retCode") == 0:
        return result["result"]
    else:
        raise Exception(f"Ошибка отмены: {result}")


async def main():
    print("=" * 60)
    env_name = "TESTNET" if USE_TESTNET else ("DEMO" if USE_DEMO else "MAINNET")
    print(f"ТЕСТ КЛЮЧЕЙ BYBIT ({env_name})")
    print("=" * 60)
    
    print("\n[1/5] Проверка связи...")
    try:
        server_time = await get_server_time()
        print(f"✅ Связь установлена. Время: {server_time}")
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return
    
    print("\n[2/5] Проверка баланса...")
    try:
        balance = await get_balance()
        print(f"✅ Баланс получен. Total Equity: {balance.get('totalEquity', 'N/A')} USDT")
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return
    
    print("\n[3/5] Размещение ордера...")
    try:
        order_result = await place_order("SOLUSDT", "Buy", 0.1, 50.0)
        order_id = order_result.get("orderId")
        if not order_id:
            raise Exception("orderId не возвращен")
        print(f"✅ Ордер размещен. Order ID: {order_id}")
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return
    
    print("\n[4/5] Ожидание 30 секунд...")
    await asyncio.sleep(30)
    
    print("Проверка статуса...")
    try:
        order_status = await get_order("SOLUSDT", order_id)  # type: ignore
        print(f"✅ Статус: {order_status.get('orderStatus')}")
    except Exception as e:
        print(f"❌ Ошибка: {e}")
    
    print("\n[5/5] Отмена ордера...")
    try:
        await cancel_order("SOLUSDT", order_id)  # type: ignore
        print(f"✅ Ордер {order_id} отменен")
    except Exception as e:
        print(f"❌ Ошибка: {e}")
    
    print("\n" + "=" * 60)
    print("ТЕСТ ЗАВЕРШЕН")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())