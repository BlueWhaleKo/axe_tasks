from abc import ABC, abstractmethod, abstractproperty
from collections import deque
import json
from typing import Tuple, Dict, List

from exceptions import MessageTypeNotSupported, PacketDecodeError


""" Message Class 

※ 새로운 Message Class를 추가하는 경우 다음의 수정이 필요함
1. MessageFactory Class의 TYPE_TO_CLASS 속성과 create 메서드 수정
"""


class Message(ABC):
    """ Abstract Class for Message Classes """

    def __init__(self, packet: str):
        self.packet = packet

        kwargs = self.translate(packet)
        for k, v in kwargs.items():
            setattr(self, k, v)

    @abstractproperty
    def SIZE(self) -> int:  # number of bytes
        pass

    @abstractproperty
    def ENCODING_ATTRS(self) -> List[str]:  # Exchange 서버로 전송할 property 리스트
        pass

    @abstractproperty
    def MSG_TYPE(self) -> str:
        pass

    @staticmethod
    @abstractmethod
    def translate(packet: str) -> Dict:
        """ converte bytes to msg kwargs """
        pass

    def encode(self) -> bytes:
        """ convert attributes into bytes, sequence is important! """
        attrs = [v for k, v in self.__dict__.items() if k in self.ENCODING_ATTRS]
        return "".join(attrs).encode()

    def json(self, indent=None):
        return json.dumps(self.__dict__, indent=indent)

    def __str__(self):
        return self.json(indent=4)

    def __repr__(self):
        return self.json(indent=4)


class ClientMessage(Message):
    """ Client Side Message """

    SIZE = 22
    ENCODING_ATTRS = ["msg_type", "order_no", "ticker", "price", "qty"]

    @staticmethod
    def translate(packet: str):
        return {
            "msg_type": packet[0],
            "order_no": packet[1:6],
            "ticker": packet[6:12],
            "price": packet[12:17],
            "qty": packet[17:22],
        }


class NewOrderMessage(ClientMessage):
    MSG_TYPE = "0"
    pass


class CancelOrderMessage(ClientMessage):
    MSG_TYPE = "1"
    pass


class OrderReceivedMessage(Message):
    """ Server Side Message """

    MSG_TYPE = "2"
    SIZE = 7
    ENCODING_ATTRS = ["msg_type", "order_no", "response_code"]

    SUCCESS = "0"
    FAIL = "1"

    @staticmethod
    def translate(packet: str):
        return {
            "msg_type": packet[0],
            "order_no": packet[1:6],
            "response_code": packet[6],
        }


class OrderExecutedMessage(Message):
    """ Server Side Message """

    SIZE = 11
    ENCODING_ATTRS = ["msg_type", "order_no", "qty"]
    MSG_TYPE = "3"

    response_code = OrderReceivedMessage.SUCCESS  # executed message는 항상 성공

    @staticmethod
    def translate(packet: str):
        return {
            "msg_type": packet[0],
            "order_no": packet[1:6],
            "qty": packet[6:11],
        }


""" Factory """


class MessageFactory:
    TYPE_TO_CLS = {
        "0": NewOrderMessage,
        "1": CancelOrderMessage,
        "2": OrderReceivedMessage,
        "3": OrderExecutedMessage,
    }

    CLS_TO_TYPE = {v: k for k, v in TYPE_TO_CLS.items()}

    def create(self, packet: str) -> List[Message]:
        if isinstance(packet, bytes):
            packet = packet.decode()

        packets = self.split_packet(packet)
        return [self._create(p) for p in packets]

    def _create(self, packet: str) -> Message:
        msg_cls = self.get_msg_cls_from_packet(packet)
        if msg_cls is None:
            raise MessageTypeNotSupported()

        return msg_cls(packet)

    def split_packet(self, packet: str) -> List[str]:
        """ split stacked packets to each packets """
        result = []

        letters = list(packet)
        letter_queue = deque(letters)  # letters -> List[str]

        while len(letter_queue):
            msg_type = letter_queue[0]
            msg_cls = MessageFactory.get_msg_cls_from_msg_type(msg_type)
            if msg_cls is None:
                print(letter_queue)
                raise MessageTypeNotSupported(f"MSG TYPE {msg_type} is not supported")

            buffer = []
            for _ in range(msg_cls.SIZE):
                buffer.append(letter_queue.popleft())
            buffer = "".join(buffer)  # str copy 최소화

            result.append(buffer)

        return result

    @classmethod
    def get_msg_cls_from_packet(cls, packet: str):
        msg_type = packet[0]
        return cls.TYPE_TO_CLS.get(msg_type, None)

    @classmethod
    def get_msg_cls_from_msg_type(cls, msg_type: str):
        return cls.TYPE_TO_CLS.get(msg_type, None)
