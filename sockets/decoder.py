from collections import deque
from typing import List, Dict

from messages.messages import MessageFactory
from exceptions import MessageTypeNotSupported

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
            err_msg = (
                f"argument for {self.__class__}.decode() must be str or bytes type"
            )
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
