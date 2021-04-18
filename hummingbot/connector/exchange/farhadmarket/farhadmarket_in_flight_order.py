from decimal import Decimal
from typing import (
    Any,
    Dict,
    Optional,
)
import asyncio
from hummingbot.core.event.events import (
    OrderType,
    TradeType
)
from hummingbot.connector.in_flight_order_base import InFlightOrderBase, s_decimal_0


class FarhadmarketInFlightOrder(InFlightOrderBase):
    def __init__(self,
                 client_order_id: str,
                 exchange_order_id: Optional[str],
                 trading_pair: str,
                 order_type: OrderType,
                 trade_type: TradeType,
                 price: Decimal,
                 amount: Decimal,
                 initial_state: str = "OPEN"):
        super().__init__(
            client_order_id,
            exchange_order_id,
            trading_pair,
            order_type,
            trade_type,
            price,
            amount,
            initial_state,
        )
        self.trade_id_set = set()
        self.cancelled_event = asyncio.Event()

    @property
    def is_done(self) -> bool:
        return self.last_state in {"FILLED", "CANCELED", "REJECTED", "EXPIRED"}

    @property
    def is_failure(self) -> bool:
        return self.last_state in {"REJECTED"}

    @property
    def is_cancelled(self) -> bool:
        return self.last_state in {"CANCELED", "EXPIRED"}

    @classmethod
    def parse_status(cls, order_response: dict):
        status = "OPEN"
        if order_response["filledStock"] == order_response["amount"]:
            status = "FILLED"
        elif order_response["finishedAt"]:
            status = "CANCELED"
        return status

    # @property
    # def order_type_description(self) -> str:
    #     """
    #     :return: Order description string . One of ["limit buy" / "limit sell" / "market buy" / "market sell"]
    #     """
    #     order_type = "market" if self.order_type is OrderType.MARKET else "limit"
    #     side = "buy" if self.trade_type == TradeType.BUY else "sell"
    #     return f"{order_type} {side}"

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> InFlightOrderBase:
        """
        :param data: json data from API
        :return: formatted InFlightOrder
        """
        retval = FarhadmarketInFlightOrder(
            data["client_order_id"],
            data["exchange_order_id"],
            data["trading_pair"],
            getattr(OrderType, data["order_type"]),
            getattr(TradeType, data["trade_type"]),
            Decimal(data["price"]),
            Decimal(data["amount"]),
            data["last_state"]
        )
        retval.executed_amount_base = Decimal(data["executed_amount_base"])
        retval.executed_amount_quote = Decimal(data["executed_amount_quote"])
        retval.fee_asset = data["fee_asset"]
        retval.fee_paid = Decimal(data["fee_paid"])
        retval.last_state = data["last_state"]
        return retval

    def update_with_trade_update(self, trade_update: Dict[str, Any]) -> bool:
        """
        Updates the in flight order with trade update (from self.deals ws channel)
        return: True if the order gets updated otherwise False

        {
                    "id": 1521457,
                    "time": "2020-04-03T22:37:32.397871Z",
                    "user": 1,
                    "role": "taker",
                    "side": "buy",
                    "amount": "0.00012475",
                    "price": "46391.65",
                    "deal": "5.79",
                    "fee": "0.00000000",
                    "orderId": 4258768,
                    "dealOrderId": 4258767,
                    "market": "BTC_USD"
        }
        """
        trade_id = str(trade_update["id"])
        # trade_update["orderId"] is type int
        if str(trade_update["orderId"]) != self.exchange_order_id or trade_id in self.trade_id_set:
            # trade already recorded
            return False
        self.trade_id_set.add(trade_id)
        self.executed_amount_base += Decimal(str(trade_update["amount"]))
        self.fee_paid += Decimal(str(trade_update["fee"]))
        self.executed_amount_quote += (Decimal(str(trade_update["price"])) *
                                       Decimal(str(trade_update["amount"])))
        return True

    def update_with_order_update(self, order_update: Dict[str, Any]) -> bool:
        """
        Updates the in flight order with order update (from orders end point)
        return: True if the order gets updated otherwise False
        Example Orders:
            REST request
            {
                "id" : "977f82aa-23dc-4c8b-982c-2ee7d2002882",
                "clientOrderId" : null,
                "symbol" : "BTC/USD",
                "orderType" : "LIMIT",
                "orderSide" : "BUY",
                "quantity" : 0.1,
                "price" : 54570,
                "payFeesWithZoomToken" : false,
                "orderStatus" : "PARTIALLY_FILLED",
                "timestamp" : "2021-03-24T04:07:26.260253Z",
                "executions" :
                [
                    {
                        "id" : "38761582-2b37-4e27-a561-434981d21a96",
                        "executionType" : "PARTIAL_FILL",
                        "orderStatus" : "PARTIALLY_FILLED",
                        "lastPrice" : 54570,
                        "averagePrice" : 54570,
                        "lastQuantity" : 0.01,
                        "leavesQuantity" : 0.09,
                        "cumulativeQuantity" : 0.01,
                        "rejectReason" : null,
                        "timestamp" : "2021-03-24T04:07:44.503222Z"
                    }
                ]
            }
            WS request
            {
                "event": "insert",
                "channel": "self.deals",
                "body": {
                    "id": 1521457,
                    "time": "2020-04-03T22:37:32.397871Z",
                    "user": 1,
                    "role": "taker",
                    "side": "buy",
                    "amount": "0.00012475",
                    "price": "46391.65",
                    "deal": "5.79",
                    "fee": "0.00000000",
                    "orderId": 4258768,
                    "dealOrderId": 4258767,
                    "market": "BTC_USD"
                }
            }
        """
        # Update order execution status
        self.last_state = order_update["orderStatus"]

        if 'cumulativeQuantity' not in order_update and 'executions' not in order_update:
            return False

        trades = order_update.get('executions')
        if trades is not None:
            new_trades = False
            for trade in trades:
                trade_id = str(trade["timestamp"])
                if trade_id not in self.trade_id_set:
                    self.trade_id_set.add(trade_id)
                    order_update["exchange_trade_id"] = trade.get("id")
                    # Add executed amounts
                    executed_price = Decimal(str(trade.get("lastPrice", "0")))
                    self.executed_amount_base += Decimal(str(trade["lastQuantity"]))
                    self.executed_amount_quote += executed_price * self.executed_amount_base
                    # Set new trades flag
                    new_trades = True
            if not new_trades:
                # trades already recorded
                return False
        else:
            trade_id = str(order_update["id"])
            if trade_id in self.trade_id_set:
                # trade already recorded
                return False
            self.trade_id_set.add(trade_id)
            # Set executed amounts
            executed_price = Decimal(str(order_update.get("averagePrice", order_update.get("price", "0"))))
            self.executed_amount_base = Decimal(str(order_update["cumulativeQuantity"]))
            self.executed_amount_quote = executed_price * self.executed_amount_base
        if self.executed_amount_base <= s_decimal_0:
            # No trades executed yet.
            return False
        self.fee_paid += order_update.get("trade_fee") * self.executed_amount_base
        if not self.fee_asset:
            self.fee_asset = self.quote_asset
        return True
