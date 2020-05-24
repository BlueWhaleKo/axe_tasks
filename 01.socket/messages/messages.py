from abc import ABC, abstractmethod, abstractproperty
import json
from typing import Tuple, Dict, List

from .exceptions import MessageTypeNotSupported


class Message(ABC):
    SIZE = None

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def encode(self) -> bytes:
        """ convert attributes into bytes, sequence is important! """
        return "".join(self.__dict__.values()).encode()

    @abstractproperty
    def SIZE():
        pass

    def json(self, indent=4):  # json 직렬화로 로깅 및 메타 데이터 관리 개선
        return json.dumps(self.__dict__, indent=4)

    def __str__(self):
        return self.json()


class ClientMessage(Message):
    """ Client Side Message """

    SIZE = 22

    def __init__(self, msg_type: str, order_no: str, ticker: str, price: str, qty: str):
        super().__init__(msg_type=msg_type, order_no=order_no, ticker=ticker, price=price, qty=qty)


class NewOrderMessage(ClientMessage):
    pass


class CancelOrderMessage(ClientMessage):
    pass


class OrderReceivedMessage(Message):
    """ Server Side Message """

    SIZE = 7
    SUCCESS = "0"
    FAIL = "1"

    def __init__(self, msg_type, order_no, is_success):
        super().__init__(msg_type=msg_type, order_no=order_no, is_success=is_success)


class OrderExecutedMessage(Message):
    """ Server Side Message """

    SIZE = 11

    def __init__(self, msg_type, order_no, qty):
        super().__init__(msg_type=msg_type, order_no=order_no, qty=qty)


class MessageFactory:
    def create(self, msg_type: str, *args, **kwargs):
        msg_cls = MessageConfig.TYPE_TO_CLS.get(msg_type)

        if msg_cls is None:
            raise MessageTypeNotSupported()

        return msg_cls(msg_type, *args, **kwargs)


""" Config """


class MessageConfig:
    TYPE_TO_CLS = {
        "0": NewOrderMessage,
        "1": CancelOrderMessage,
        "2": OrderReceivedMessage,
        "3": OrderExecutedMessage,
    }

    CLS_TO_TYPE = {v: k for k, v in TYPE_TO_CLS.items()}
