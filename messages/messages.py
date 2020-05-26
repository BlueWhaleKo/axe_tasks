from abc import ABC, abstractmethod, abstractproperty
from collections import deque
import json
from typing import Tuple, Dict, List

from .exceptions import MessageTypeNotSupported, PacketDecodeError


""" Message Class 

※ 새로운 Message Class를 추가하는 경우 다음의 수정이 필요함
1. MessageFactory Class의 TYPE_TO_CLASS 속성과 create 메서드 수정
2. PacketDecoder Class의 _decode<ClassName> 매서드 확장

"""


class Message(ABC):
    """ Abstract Class for Message Classes """

    def __init__(self, **kwargs):
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

    def encode(self) -> bytes:
        """ convert attributes into bytes, sequence is important! """
        attrs = [v for k, v in self.__dict__.items() if k in self.ENCODING_ATTRS]
        return "".join(attrs).encode()

    def json(self, indent=4):
        return json.dumps(self.__dict__, indent=4)

    def __str__(self):
        return self.json()

    def __repr__(self):
        return self.json()


class ClientMessage(Message):
    """ Client Side Message """

    SIZE = 22
    ENCODING_ATTRS = ["msg_type", "order_no", "ticker", "price", "qty"]

    def __init__(self, msg_type: str, order_no: str, ticker: str, price: str, qty: str, **kwargs):
        self.time = None  # 주문 시간 (거래소 전송 x)
        self.response_code = None  # 주문 성공/실패 여부 (거래소 전송 x)

        super().__init__(
            msg_type=msg_type, order_no=order_no, ticker=ticker, price=price, qty=qty, **kwargs,
        )  # overwrite 가능


class NewOrderMessage(ClientMessage):
    MSG_TYPE = "0"
    pass


class CancelOrderMessage(ClientMessage):
    MSG_TYPE = "1"
    pass


class UnexecutedOrder(NewOrderMessage):
    """ Exchange 서버로 전송하지 않지만, 
        원활한 Message History 관리를 위해 임시로 사용하는 클래스 
    """

    MSG_TYPE = None

    pass


class OrderReceivedMessage(Message):
    """ Server Side Message """

    SIZE = 7
    ENCODING_ATTRS = ["msg_type", "order_no", "response_code"]
    MSG_TYPE = "2"

    SUCCESS = "0"
    FAIL = "1"

    def __init__(self, msg_type, order_no, response_code):
        super().__init__(msg_type=msg_type, order_no=order_no, response_code=response_code)

    def is_success(self):
        return self.response_code == self.SUCCESS

    def is_fail(self):
        return self.response_code == self.FAIL


class OrderExecutedMessage(Message):
    """ Server Side Message """

    SIZE = 11
    ENCODING_ATTRS = ["msg_type", "order_no", "qty"]
    MSG_TYPE = "3"

    response_code = OrderReceivedMessage.SUCCESS  # executed message는 항상 성공

    def __init__(self, msg_type, order_no, qty):
        super().__init__(msg_type=msg_type, order_no=order_no, qty=qty)


class MessageFactory:
    TYPE_TO_CLS = {
        NewOrderMessage.MSG_TYPE: NewOrderMessage,
        CancelOrderMessage.MSG_TYPE: CancelOrderMessage,
        OrderReceivedMessage.MSG_TYPE: OrderReceivedMessage,
        OrderExecutedMessage.MSG_TYPE: OrderExecutedMessage,
    }

    CLS_TO_TYPE = {v: k for k, v in TYPE_TO_CLS.items()}

    def __init__(self):
        self.packet_decoder = PacketDecoder()

    def create(self, msg_type: str, *args, **kwargs) -> Message:
        msg_cls = self.TYPE_TO_CLS.get(msg_type)
        if msg_cls is None:
            raise MessageTypeNotSupported()

        return msg_cls(msg_type, *args, **kwargs)

    def create_from_packet(self, packet: bytes) -> List[Message]:
        msg_kwargs = self.packet_decoder.decode(packet)
        return [self.create(**kw) for kw in msg_kwargs]


""" Packet Decoder Class """


class PacketDecoder:
    """ Decord server-side packet into kwargs for creating Message instance 
    서버에서 온 packet에서 메타 데이터를 추출하는 클래스
    추출된 메타 데이터는 MessageFactory에서 Message 인스턴스를 생성하는데 활용됨
    """

    def decode(self, bytes_: bytes or str) -> List[Dict]:
        """ type check """
        if isinstance(bytes_, bytes):
            letters = list(bytes_.decode())
        elif isinstance(bytes_, str):
            letters = list(bytes_)
        else:
            err_msg = f"argument for {self.__class__}.decode() must be str or bytes type"
            raise TypeError(err_msg)

        """ decode packet """
        result = []
        letter_queue = deque(letters)  # letters -> List[str]

        while len(letter_queue):
            msg_type = letter_queue[0]
            msg_cls = MessageFactory.TYPE_TO_CLS.get(msg_type, None)
            if msg_cls is None:
                raise MessageTypeNotSupported(f"MSG TYPE {msg_type} is not supported")

            buffer = []
            for _ in range(msg_cls.SIZE):
                buffer.append(letter_queue.popleft())
            buffer = "".join(buffer)  # str copy 최소화

            kwargs = self._decode(buffer)
            result.append(kwargs)

        return result

    def _decode(self, packet: str or bytes):
        """ decode packet into kwargs """
        msg_type = packet[0]
        msg_cls = MessageFactory.TYPE_TO_CLS.get(msg_type)
        decode_method = getattr(self, f"_decode{msg_cls.__name__}")
        return decode_method(packet)

    def _decodeNewOrderMessage(self, packet):
        return {
            "msg_type": packet[0],
            "order_no": packet[1:6],
            "ticker": packet[6:12],
            "price": packet[12:17],
            "qty": packet[17:22],
        }

    def _decodeCancelOrderMessage(self, packet):
        return {
            "msg_type": packet[0],
            "order_no": packet[1:6],
            "ticker": packet[6:12],
            "price": packet[12:17],
            "qty": packet[17:22],
        }

    def _decodeOrderReceivedMessage(self, packet):
        return {
            "msg_type": packet[0],
            "order_no": packet[1:6],
            "response_code": packet[6],
        }

    def _decodeOrderExecutedMessage(self, packet):
        return {
            "msg_type": packet[0],
            "order_no": packet[1:6],
            "qty": packet[6:11],
        }
