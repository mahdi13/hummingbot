#!/usr/bin/env python
import asyncio
import logging
import time
import aiohttp
import pandas as pd
import hummingbot.connector.exchange.farhadmarket.farhadmarket_constants as constants

from typing import Optional, List, Dict, Any
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.logger import HummingbotLogger
from . import farhadmarket_utils
from .farhadmarket_active_order_tracker import FarhadmarketActiveOrderTracker
from .farhadmarket_order_book import FarhadmarketOrderBook
from .farhadmarket_websocket import FarhadmarketWebsocket
from .farhadmarket_utils import ms_timestamp_to_s


class FarhadmarketAPIOrderBookDataSource(OrderBookTrackerDataSource):
    MAX_RETRIES = 20
    MESSAGE_TIMEOUT = 30.0
    SNAPSHOT_TIMEOUT = 10.0

    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, trading_pairs: List[str] = None):
        super().__init__(trading_pairs)
        self._trading_pairs: List[str] = trading_pairs
        self._snapshot_msg: Dict[str, any] = {}

    @classmethod
    async def get_last_traded_prices(cls, trading_pairs: List[str]) -> Dict[str, float]:
        result = {}
        async with aiohttp.ClientSession() as client:
            resp = await client.get(f"{constants.REST_URL}/markets/all/lasts")
            resp_json = await resp.json()
            exchange_trading_pairs = [farhadmarket_utils.convert_to_exchange_trading_pair(p) for p in trading_pairs]
            result = {farhadmarket_utils.convert_from_exchange_trading_pair(pair): float(price)
                      for pair, price in resp_json.items() if pair in exchange_trading_pairs}
        return result

    @staticmethod
    async def fetch_trading_pairs() -> List[str]:
        async with aiohttp.ClientSession() as client:
            async with client.get(f"{constants.REST_URL}/markets/all/lasts", timeout=10) as response:
                if response.status == 200:
                    from hummingbot.connector.exchange.farhadmarket.farhadmarket_utils import \
                        convert_from_exchange_trading_pair
                    try:
                        data: Dict[str, Any] = await response.json()
                        return [convert_from_exchange_trading_pair(item) for item in data.keys()]
                    except Exception:
                        pass
                        # Do nothing if the request fails -- there will be no autocomplete for kucoin trading pairs
                return []

    @staticmethod
    async def get_order_book_data(trading_pair: str) -> Dict[str, any]:
        """
        Get whole orderbook
        """
        async with aiohttp.ClientSession() as client:
            orderbook_response = await client.request(
                "DEPTH",
                f"{constants.REST_URL}/markets/{farhadmarket_utils.convert_to_exchange_trading_pair(trading_pair)}",
                params={"interval": "0",
                        "limit": 100
                        }
            )

            if orderbook_response.status != 200:
                raise IOError(
                    f"Error fetching OrderBook for {trading_pair} at {constants.EXCHANGE_NAME}. "
                    f"HTTP status is {orderbook_response.status}."
                )

            orderbook_data = await safe_gather(orderbook_response.json())
            orderbook_data = orderbook_data[0]
            orderbook_data["t"] = farhadmarket_utils.get_ms_timestamp()

        return orderbook_data

    async def get_new_order_book(self, trading_pair: str) -> OrderBook:
        snapshot: Dict[str, Any] = await self.get_order_book_data(trading_pair)
        snapshot_timestamp: float = time.time()
        snapshot_msg: OrderBookMessage = FarhadmarketOrderBook.snapshot_message_from_exchange(
            snapshot,
            snapshot_timestamp,
            metadata={"trading_pair": trading_pair}
        )
        order_book = self.order_book_create_function()
        active_order_tracker: FarhadmarketActiveOrderTracker = FarhadmarketActiveOrderTracker()
        self.logger().error(f"Snapshot: {snapshot_msg}")
        bids, asks = active_order_tracker.convert_snapshot_message_to_order_book_row(snapshot_msg)
        order_book.apply_snapshot(bids, asks, snapshot_msg.update_id)
        return order_book

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """
        Listen for trades using websocket trade channel
        """
        while True:
            try:
                ws = FarhadmarketWebsocket()
                await ws.connect()

                await safe_gather(*[ws.subscribe(
                    "market.deals",
                    {"market": farhadmarket_utils.convert_to_exchange_trading_pair(pair)}
                ) for pair in self._trading_pairs])

                async for response in ws.on_message():
                    self.logger().info(f"WS response: {response}")
                    if response.get("channel") != "market.deals" or response.get("event") != "update":
                        continue
                    pair = response["body"]["market"]
                    trades = response["body"]["deals"]

                    for trade in trades:
                        trade: Dict[Any] = trade
                        trade_timestamp: int = farhadmarket_utils.get_ms_timestamp()
                        trade_msg: OrderBookMessage = FarhadmarketOrderBook.trade_message_from_exchange(
                            trade,
                            trade_timestamp,
                            metadata={"trading_pair": farhadmarket_utils.convert_from_exchange_trading_pair(pair)}
                        )
                        output.put_nowait(trade_msg)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error.", exc_info=True)
                await asyncio.sleep(5.0)
            finally:
                await ws.disconnect()

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """
        Listen for orderbook diffs using websocket book channel
        """
        while True:
            try:
                ws = FarhadmarketWebsocket()
                await ws.connect()

                await safe_gather(*[ws.subscribe(
                    "market.depth",
                    {"market": farhadmarket_utils.convert_to_exchange_trading_pair(pair),
                     "interval": "0"
                     }
                ) for pair in self._trading_pairs])

                async for response in ws.on_message():
                    if response.get("channel") != "market.depth" or response.get("event") not in ["update", "init"]:
                        continue

                    pair = response["body"]["market"]
                    asks = [{"price": x[0], "amount": x[1]} for x in response["body"]["asks"]]
                    bids = [{"price": x[0], "amount": x[1]} for x in response["body"]["bids"]]

                    order_book_data = response
                    order_book_data["t"] = farhadmarket_utils.get_ms_timestamp()
                    timestamp: int = ms_timestamp_to_s(order_book_data["t"])
                    # data in this channel is not order book diff but the entire order book (up to depth 150).
                    # so we need to convert it into a order book snapshot.
                    # Crypto.com does not offer order book diff ws updates.
                    if response.get("event") == "init":
                        orderbook_msg: OrderBookMessage = FarhadmarketOrderBook.snapshot_message_from_exchange(
                            {"asks": asks, "bids": bids},
                            timestamp,
                            metadata={"trading_pair": farhadmarket_utils.convert_from_exchange_trading_pair(pair)}
                        )
                    elif response.get("event") == "update":
                        orderbook_msg: OrderBookMessage = FarhadmarketOrderBook.diff_message_from_exchange(
                            {"asks": asks, "bids": bids},
                            timestamp,
                            metadata={"trading_pair": farhadmarket_utils.convert_from_exchange_trading_pair(pair)}
                        )
                    output.put_nowait(orderbook_msg)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    "Unexpected error with WebSocket connection.",
                    exc_info=True,
                    app_warning_msg="Unexpected error with WebSocket connection. Retrying in 30 seconds. "
                                    "Check network connection."
                )
                await asyncio.sleep(30.0)
            finally:
                await ws.disconnect()

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """
        Listen for orderbook snapshots by fetching orderbook
        """
        while True:
            try:
                for trading_pair in self._trading_pairs:
                    try:
                        snapshot: Dict[str, any] = await self.get_order_book_data(trading_pair)
                        snapshot_timestamp: int = ms_timestamp_to_s(snapshot["t"])
                        snapshot_msg: OrderBookMessage = FarhadmarketOrderBook.snapshot_message_from_exchange(
                            snapshot,
                            snapshot_timestamp,
                            metadata={"trading_pair": trading_pair}
                        )
                        output.put_nowait(snapshot_msg)
                        self.logger().debug(f"Saved order book snapshot for {trading_pair}")
                        # Be careful not to go above API rate limits.
                        await asyncio.sleep(5.0)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        self.logger().network(
                            "Unexpected error with WebSocket connection.",
                            exc_info=True,
                            app_warning_msg="Unexpected error with WebSocket connection. Retrying in 5 seconds. "
                                            "Check network connection."
                        )
                        await asyncio.sleep(5.0)
                this_hour: pd.Timestamp = pd.Timestamp.utcnow().replace(minute=0, second=0, microsecond=0)
                next_hour: pd.Timestamp = this_hour + pd.Timedelta(hours=1)
                delta: float = next_hour.timestamp() - time.time()
                await asyncio.sleep(delta)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error.", exc_info=True)
                await asyncio.sleep(5.0)
