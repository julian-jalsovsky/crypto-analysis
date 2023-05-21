from typing import Optional, Dict
import aiohttp
import websockets
import json

from utils import create_timestamp, create_signature


class AsyncClient:

    # API_URL = "https://api.binance.com/api"
    API_URL = "https://testnet.binance.vision"
    # WEBSOCKET_URL = "wss://stream.binance.com:9443/ws"
    WEBSOCKET_URL = "wss://testnet.binance.vision/ws"

    def __init__(self, api_key: Optional[str] = None,
                 api_secret: Optional[str] = None):

        self.API_KEY = api_key
        self.API_SECRET = api_secret
        self.headers = {"X-MBX-APIKEY": api_key}

    async def _async_request(self, method: str, url: str,
                             params: Optional[Dict] = None,
                             data: Optional[Dict] = None,
                             headers: Optional[Dict] = None):
        async with aiohttp.ClientSession() as session:

            if method == 'GET':
                async with session.get(url, params=params,
                                       headers=headers) as resp:
                    return await resp.json()

            if method == 'POST':
                async with session.post(url, data=data,
                                        headers=headers) as resp:
                    return await resp.json()

            if method == 'PUT':
                async with session.put(url, data=data,
                                       headers=headers) as resp:
                    return await resp.json()

            if method == 'DELETE':
                async with session.delete(url, data=data,
                                          headers=headers) as resp:
                    return await resp.json()

    async def ws_connect(self):
        return websockets.connect(self.WEBSOCKET_URL)

    async def ws_subscribe(self, connection, streams: list):
        msg = {
            "method": "SUBSCRIBE",
            "params": streams,
            "id": 1
        }
        await connection.send(json.dumps(msg))

    async def ws_unsubscribe(self, connection, streams: list):
        msg = {
            "method": "UNSUBSCRIBE",
            "params": streams,
            "id": 312
        }
        await connection.send(json.dumps(msg))

    async def ws_listen(self, connection, queue):
        async for message in connection:
            await queue.put(json.loads(message))

    async def exchange_info(self):
        '''
        Binance API: Exchange Information
        Current exchange trading rules and symbol information
        '''
        endpoint = "/api/v3/exchangeInfo"
        return await self._async_request('GET', self.API_URL + endpoint)

    async def get_prices(self, symbol: Optional[str] = None):
        '''
        Binance API: Symbol Price Ticker
        Latest price for a symbol or symbols.

        TO DO: add functionality for multiple symbols
        '''
        endpoint = "/api/v3/ticker/price"

        if symbol:
            params = {"symbol": symbol}
            return await self._async_request('GET', self.API_URL + endpoint, params)
        else:
            return await self._async_request('GET', self.API_URL + endpoint)

    async def recent_trades(self, symbol: str, limit: Optional[int] = 1000):
        '''
        Binance API: Recent Trades List
        Get recent trades.
        '''
        endpoint = "/api/v3/trades"

        params = {
            "symbol": symbol,
            "limit": limit
        }

        return await self._async_request('GET', self.API_URL + endpoint, params)

    async def old_trade_lookup(self, symbol: str, limit: Optional[int] = 1000,
                               fromId: Optional[int] = None):
        '''
        Binance API: Old Trade Lookup (MARKET_DATA)
        Get older market trades.

        tradeId: Trade id to fetch from. Default gets most recent trades.
        '''
        endpoint = "/api/v3/historicalTrades"

        params = {
            "symbol": symbol,
            "limit": limit,
            "fromId": fromId
        }
        params = {k: v for k, v in params.items() if v is not None}

        return await self._async_request('GET', self.API_URL + endpoint, params,
                                         headers=self.headers)

    async def aggregate_trades_list(self, symbol: str,
                                    fromId: Optional[int] = None,
                                    startTime: Optional[int] = None,
                                    endTime: Optional[int] = None,
                                    limit: int = 1000):
        '''
        Binance API: Compressed/Aggregate Trades List
        Get compressed, aggregate trades.
        Trades that fill at the time, from the same order,
        with the same price will have the quantity aggregated.
        '''
        endpoint = "/api/v3/aggTrades"

        params = {
            "symbol": symbol,
            "fromId": fromId,
            "startTime": startTime,
            "endTime": endTime,
            "limit": limit
        }
        params = {k: v for k, v in params.items() if v is not None}

        return await self._async_request('GET', self.API_URL + endpoint, params)

    async def candlestick_data(self, symbol: str, interval: str,
                               startTime: Optional[int] = None,
                               endTime: Optional[int] = None,
                               limit: int = 1000):
        '''
        Binance API: Kline/Candlestick Data
        Kline/candlestick bars for a symbol.
        Klines are uniquely identified by their open time.
        '''
        endpoint = "/api/v3/klines"

        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": startTime,
            "endTime": endTime,
            "limit": limit
        }
        params = {k: v for k, v in params.items() if v is not None}

        return await self._async_request('GET', self.API_URL + endpoint, params)

    async def new_order(self, symbol: str, side: str, type: str, quantity: float,
                        limit_price: Optional[float] = None,
                        time_in_force: Optional[str] = None):
        '''
        Binance API: New Order (TRADE)
        Send in a new order.
        '''
        # Add /test for test order
        endpoint = "/api/v3/order"

        payload = {
            "symbol": symbol,
            "side": side,
            "type": type,
            "timeInForce": time_in_force,
            "quantity": quantity,
            "price": limit_price,
            "timestamp": create_timestamp()
        }

        # Removes 'None' values from dictionary
        payload = {k: v for k, v in payload.items() if v is not None}
        payload["signature"] = create_signature(self.API_SECRET, payload)

        return await self._async_request('POST', self.API_URL + endpoint,
                                         data=payload, headers=self.headers)

    async def cancel_order(self, symbol: str, orderId: int):
        '''
        Binance API: Cancel Order (TRADE)
        Cancel an active order.
        '''
        endpoint = "/api/v3/order"

        payload = {
            "symbol": symbol,
            "orderId": orderId,
            "timestamp": create_timestamp()
        }

        payload["signature"] = create_signature(self.API_SECRET, payload)

        return await self._async_request('DELETE', self.API_URL + endpoint,
                                         data=payload, headers=self.headers)

    async def cancel_all_open(self, symbol: str):
        '''
        Binance API: Cancel all Open Orders on a Symbol (TRADE)
        Cancels all active orders on a symbol.

        TO DO: test if hmac sha256 is needed
        '''
        endpoint = "/api/v3/openOrders"

        payload = {
            "symbol": symbol,
            "timestamp": create_timestamp()
        }

        return await self._async_request('DELETE', self.API_URL + endpoint,
                                         data=payload)

    async def query_order(self, symbol, orderId):
        '''
        Binance API: Query Order (USER_DATA)
        Check an order's status.
        '''
        endpoint = "/api/v3/order"

        params = {
            "symbol": symbol,
            "orderId": orderId,
            "timestamp": create_timestamp()
        }
        params["signature"] = create_signature(self.API_SECRET, params)

        return await self._async_request('GET', self.API_URL + endpoint,
                                         params=params, headers=self.headers)

    async def current_open_orders(self, symbol: Optional[str] = None):
        '''
        Binance API: Current Open Orders (USER_DATA)
        Get all open orders on a symbol.
        Careful when accessing this with no symbol. (IP weight 40)
        '''
        endpoint = "/api/v3/openOrders"

        payload = {
            "symbol": symbol,
            "timestamp": create_timestamp()
        }

        payload = {k: v for k, v in payload.items() if v is not None}
        payload["signature"] = create_signature(self.API_SECRET, payload)

        return await self._async_request('GET', self.API_URL + endpoint,
                                         params=payload, headers=self.headers)

    async def all_orders(self, symbol: str, orderId: Optional[int] = None,
                         limit: Optional[int] = 500):
        '''
        Binance API: All Orders (USER_DATA)
        Get all account orders; active, canceled, or filled.

        TO DO: add startTime and endTime parameters
        '''
        endpoint = "/api/v3/allOrders"

        payload = {
            "symbol": symbol,
            "orderId": orderId,
            "limit": limit,
            "timestamp": create_timestamp()
        }

        payload = {k: v for k, v in payload.items() if v is not None}
        payload["signature"] = create_signature(self.API_SECRET, payload)

        return await self._async_request('GET', self.API_URL + endpoint,
                                         params=payload, headers=self.headers)

    async def create_listen_key(self):
        '''
        Binance API: Create a ListenKey (USER_STREAM) SPOT
        Start a new user data stream. The stream will close
        after 60 minutes unless a keepalive is sent.
        If the account has an active listenKey, that listenKey will be returned
        and its validity will be extended for 60 minutes.
        '''
        endpoint = "/api/v3/userDataStream"

        return await self._async_request('POST', self.API_URL + endpoint,
                                         headers=self.headers)

    async def ping_listen_key(self, listen_key: str):
        '''
        Ping/Keep-alive a ListenKey (USER_STREAM) SPOT
        Keepalive a user data stream to prevent a time out.
        User data streams will close after 60 minutes.
        It's recommended to send a ping about every 30 minutes.
        '''
        endpoint = "/api/v3/userDataStream"
        data = {"listenKey": listen_key}

        return await self._async_request('PUT', self.API_URL + endpoint,
                                         data=data, headers=self.headers)

    async def close_listen_key(self, listen_key: str):
        '''
        Binance API: Close a ListenKey (USER_STREAM) SPOT
        Close out a user data stream.
        '''
        endpoint = "/api/v3/userDataStream"
        data = {"listenKey": listen_key}

        return await self._async_request('DELETE', self.API_URL + endpoint,
                                         data=data, headers=self.headers)
