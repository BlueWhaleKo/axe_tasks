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

    @property
    def class_name(self):
        """ 원활한 Query Building을 위해 추가한 속성 """
        return self.__class__.__name__


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

        setattr(self, "unex_qty", qty)  # 신규 메시지 생성 시점에는 미체결 수량과 주문 수량이 일치함

    def subtract_unex_order_count(self, qty):
        self.unex_qty = str(int(self.unex_qty) - qty).zfill(5)


class CancelOrder(NewOrder):
    MSG_TYPE = "1"


"""
class UnexecutedOrder(NewOrder):
    MSG_TYPE = None

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

        self.unex_qty = qty
"""


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
