from abc import ABC, abstractmethod
from datetime import datetime
import logging
import socket
import time

from sockets import TCPSocket
from messages.messages import (
    Message,
    NewOrderMessage,
    OrderReceivedMessage,
    OrderExecutedMessage,
    MessageFactory,
)
from messages.decoder import PacketDecoder
import threading
import time


class MessageHistory:
    _history = []

    def __iter__(self):
        return iter(self._history)

    def __getitem__(self, idx):
        return self._history[idx]

    def __len__(self):
        return len(self._history)

    def append(self, msg: Message):
        self._history.append(
            {"msg": msg, "time": datetime.now(),}
        )

    def select_cls(self, cls: Message):
        return [x for x in self._history if isinstance(x, cls)]


class Client:
    def __init__(self, host, port):
        self.host = host
        self.port = port

        self.socket_idx = 0

        self.msg_factory = MessageFactory()
        self.packet_decoder = PacketDecoder()
        self.client_msg_history = MessageHistory()
        self.server_msg_history = MessageHistory()

    def create_msg(self, msg_type, order_no, ticker, price, qty):
        return self.msg_factory.create(msg_type, order_no, ticker, price, qty)

    def send_msg(self, order_msg: Message, connection_timeout=10):
        self.order_msg = order_msg
        order_packet = order_msg.encode()

        stime = time.time()
        order_flag = True  # 주문 메시지는 한번만 전송
        with TCPSocket(host=self.host, port=self.port, timeout=5) as s:
            while True:

                if order_flag:  # send order packet
                    s.sendall(order_packet)
                    order_flag = False

                packet = s.recv(1024, timeout=1)  # receive packet
                if not packet:
                    if time.time() - stime > connection_timeout:  # connection timeout
                        break
                    continue

                self._handle_server_packet(packet)

            self.client_msg_history.append(self.order_msg)  # 제출한 주문 정보 기록

    def _handle_server_packet(self, packet: bytes):
        msg_kwargs = self.packet_decoder.decode(packet)

        for kwargs in msg_kwargs:
            msg = self.msg_factory.create(**kwargs)

            if isinstance(msg, OrderReceivedMessage):  # 주문 접수 확인
                if msg.is_success == OrderReceivedMessage.SUCCESS:  # 주문 성공
                    order_no = msg.order_no
                    setattr(self.order_msg, "order_no", order_no)

            self.server_msg_history.append(msg)
            print(msg)
