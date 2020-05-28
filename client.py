from abc import ABC, abstractmethod
from datetime import datetime
import json
import logging
import inspect
import socket
import time
from typing import List, Dict

from cache.redis import Redis
from logger import LoggerMixin
from messages.messages import (
    Message,
    OrderReceivedMessage,
    MessageFactory,
)
from orders.orders import OrderFactory
from sockets import TCPSocket


class Client(LoggerMixin):
    RESET_PACKET = b"reset"

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.socket = TCPSocket(host=host, port=port)

        self.redis = Redis(host="127.0.0.1", port="6379")

        self.msg_factory = MessageFactory()
        self.order_factory = OrderFactory()

    def sendall(self, c_packet: bytes) -> bool:
        """ return True if succeed, False when failed """

        self.socket.sendall(c_packet)  # send packet

        s_packet = self.recv()
        if not s_packet:
            self.logger.error("No Message Received From Server")
            return False

        s_msgs = self.msg_factory.create(s_packet)
        order_no, response_code = self._inspect_s_msgs(s_msgs)
        is_success = response_code == OrderReceivedMessage.SUCCESS

        if c_packet == self.RESET_PACKET:
            return response_code

        # overwrite Order Message
        c_msg = self.msg_factory.create(c_packet).pop()

        setattr(c_msg, "response_code", response_code)
        if response_code == "0":  # 성공한 주문만 order_no을 덮어씀
            setattr(c_msg, "order_no", order_no)

        self.save_cache(c_msg)
        self.save_cache(*s_msgs)

        return is_success

    def recv(self, size=1024, timeout=0.3):
        s_packets = []  # copy 최소화
        while True:
            try:
                pk = self.socket.recv(size, timeout).decode()  # receive packet
            except AttributeError:  # no data
                break
            s_packets.append(pk)
        s_packet = "".join(s_packets)

        return s_packet

    def save_cache(self, *msg: Message) -> None:
        """ Save on Redis with key <Class Name> + leave log 
            as json format
        """
        for m in msg:
            setattr(m, "time", str(datetime.now()))  # time property 추가

            cls_name = m.__class__.__name__
            if cls_name == "NewOrderMessage":
                key = cls_name.replace("Message", "")  # NewOrderMessage -> NewOrder
            else:
                key = cls_name.replace("Message", "Order")  # xxxMessage -> xxxOrder

            self.redis.rpush(key, m.json())  # RAM
            self.logger.debug(f"{key}-{m.json()}")  # File

    def _inspect_s_msgs(self, s_msgs: List[Message]):
        """ 
        Client가 packet을 전송할 때는, 주문번호와 응답코드가 비어있음
        따라서, server에서 받은 packet에서 주문번호와 응답코드를 파싱함
        """

        for m in s_msgs:
            if isinstance(m, OrderReceivedMessage):
                order_no = getattr(m, "order_no")
                response_code = getattr(m, "response_code")

                return order_no, response_code
