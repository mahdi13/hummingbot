#!/usr/bin/env python

import asyncio
import logging
from typing import (
    Optional,
    List,
)

from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.user_stream_tracker import (
    UserStreamTracker
)
from hummingbot.core.utils.async_utils import (
    safe_ensure_future,
    safe_gather,
)
from hummingbot.connector.exchange.farhadmarket.farhadmarket_api_user_stream_data_source import \
    FarhadmarketAPIUserStreamDataSource
from hummingbot.connector.exchange.farhadmarket.farhadmarket_auth import FarhadmarketAuth
import hummingbot.connector.exchange.farhadmarket.farhadmarket_constants as constants


class FarhadmarketUserStreamTracker(UserStreamTracker):
    _fmust_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._fmust_logger is None:
            cls._fmust_logger = logging.getLogger(__name__)
        return cls._fmust_logger

    def __init__(self,
                 farhadmarket_auth: Optional[FarhadmarketAuth] = None,
                 trading_pairs: Optional[List[str]] = []):
        super().__init__()
        self._farhadmarket_auth: FarhadmarketAuth = farhadmarket_auth
        self._trading_pairs: List[str] = trading_pairs
        self._ev_loop: asyncio.events.AbstractEventLoop = asyncio.get_event_loop()
        self._data_source: Optional[UserStreamTrackerDataSource] = None
        self._user_stream_tracking_task: Optional[asyncio.Task] = None

    @property
    def data_source(self) -> UserStreamTrackerDataSource:
        if not self._data_source:
            self._data_source = FarhadmarketAPIUserStreamDataSource(
                farhadmarket_auth=self._farhadmarket_auth, trading_pairs=self._trading_pairs)
        return self._data_source

    @property
    def exchange_name(self) -> str:
        return constants.EXCHANGE_NAME

    async def start(self):
        self._user_stream_tracking_task = safe_ensure_future(
            self.data_source.listen_for_user_stream(self._ev_loop, self._user_stream)
        )
        await safe_gather(self._user_stream_tracking_task)
