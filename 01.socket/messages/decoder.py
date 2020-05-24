from collections import deque

from messages.messages import MessageConfig
from typing import List, Dict


class PacketDecoder:
    """ Decord server-side packet into kwargs for creating Message instance """

    def __init__(self):
        self.msg_config = MessageConfig()

    def decode(self, bytes_: bytes) -> List[Dict]:
        result = []

        str_ = bytes_.decode()
        packet_queue = deque(list(str_))

        while len(packet_queue):
            msg_type = packet_queue[0]
            msg_cls = self.msg_config.TYPE_TO_CLS.get(msg_type)

            buffer = []
            for _ in range(msg_cls.SIZE):
                buffer.append(packet_queue.popleft())
            buffer = "".join(buffer)  # str copy 최소화

            kwargs = self._decode(buffer)
            result.append(kwargs)

        # 주문 확인(msg_type=2) 메시지가 마지막에 나오므로 역순으로 sorting
        result.reverse()
        return result

    def _decode(self, pks: str or bytes):
        if isinstance(pks, bytes):
            pks = pks.decode()

        msg_type = pks[0]

        if msg_type == "2":
            return {
                "msg_type": pks[0],
                "order_no": pks[1:6],
                "is_success": pks[6],
            }
        elif msg_type == "3":
            return {
                "msg_type": pks[0],
                "order_no": pks[1:6],
                "qty": pks[6:11],
            }
