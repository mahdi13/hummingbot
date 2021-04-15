import math

from hummingbot.core.utils.tracking_nonce import get_tracking_nonce, get_tracking_nonce_low_res

from hummingbot.client.config.config_var import ConfigVar
from hummingbot.client.config.config_methods import using_exchange

CENTRALIZED = True

EXAMPLE_PAIR = "ETH-USDT"

DEFAULT_FEES = [0.2, 0.4]

HBOT_BROKER_ID = "HBOT-"


# convert milliseconds timestamp to seconds
def ms_timestamp_to_s(ms: int) -> int:
    return math.floor(ms / 1e3)


# Request ID class
class RequestId:
    """
    Generate request ids
    """
    _request_id: int = 0

    @classmethod
    def generate_request_id(cls) -> int:
        return get_tracking_nonce()


def get_new_client_order_id(is_buy: bool, trading_pair: str) -> str:
    side = "B" if is_buy else "S"
    return f"{HBOT_BROKER_ID}{side}-{trading_pair}-{get_tracking_nonce()}"


# get timestamp in milliseconds
def get_ms_timestamp() -> int:
    return get_tracking_nonce_low_res()


def convert_from_exchange_trading_pair(exchange_trading_pair: str) -> str:
    return exchange_trading_pair.replace("_", "-")


def convert_to_exchange_trading_pair(hb_trading_pair: str) -> str:
    return hb_trading_pair.replace("-", "_")


KEYS = {
    "farhadmarket_api_key":
        ConfigVar(key="farhadmarket_api_key",
                  prompt="Enter your farhadmarket.com API key >>> ",
                  required_if=using_exchange("farhadmarket"),
                  is_secure=True,
                  is_connect_key=True),
    "farhadmarket_secret_key":
        ConfigVar(key="farhadmarket_secret_key",
                  prompt="Enter your farhadmarket.com secret key >>> ",
                  required_if=using_exchange("farhadmarket"),
                  is_secure=True,
                  is_connect_key=True),
}
