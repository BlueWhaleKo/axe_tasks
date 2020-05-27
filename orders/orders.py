from abc import ABC, abstractmethod
import json


class Order:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def is_success(self):
        if not hasattr(self, "response_code"):
            return True

        return getattr(self, "response_code") == "0"

    def json(self, indent=None):
        return json.dumps(self.__dict__, indent=indent)

    def __str__(self):
        return self.json(indent=4)

    def __repr__(self):
        return self.json(indent=4)


class NewOrder(Order):
    MSG_TYPE = "0"

    def __init__(self, msg_type, order_no, ticker, price, qty, response_code, **kwargs):
        super().__init__(
            msg_type=msg_type,
            order_no=order_no,
            ticker=ticker,
            price=price,
            qty=qty,
            response_code=response_code,
            **kwargs
        )


class CancelOrder(Order):
    MSG_TYPE = "1"

    def __init__(self, msg_type, order_no, ticker, price, qty, response_code, **kwargs):
        super().__init__(
            msg_type=msg_type,
            order_no=order_no,
            ticker=ticker,
            price=price,
            qty=qty,
            response_code=response_code,
            **kwargs
        )


class ReceivedOrder(Order):
    MSG_TYPE = "2"

    def __init__(self, msg_type, order_no, response_code, **kwargs):
        super().__init__(
            msg_type=msg_type, order_no=order_no, response_code=response_code, **kwargs
        )


class ExecutedOrder(Order):
    MSG_TYPE = "3"

    def __init__(self, msg_type, order_no, qty, **kwargs):
        super().__init__(msg_type=msg_type, order_no=order_no, qty=qty, **kwargs)


class UnexecutedOrder(NewOrder):
    MSG_TYPE = None


class OrderFactory:
    def create(self, **kwargs) -> Order:
        msg_type = kwargs.get("msg_type", None)
        if msg_type == "0":
            return NewOrder(**kwargs)

        elif msg_type == "1":
            return CancelOrder(**kwargs)

        elif msg_type == "2":
            return ReceivedOrder(**kwargs)

        elif msg_type == "3":
            return ExecutedOrder(**kwargs)
