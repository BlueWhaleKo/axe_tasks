from abc import ABC, abstractmethod
from datetime import datetime
import logging
import socket
import time

from sockets import TCPSocket
from messages.querent import MessageHistory
from messages.messages import (
    Message,
    NewOrderMessage,
    OrderReceivedMessage,
    OrderExecutedMessage,
    MessageFactory,
    PacketDecoder,
)


class Client:
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def sendall(self, packet: bytes, connection_timeout=3):
        stime = time.time()
        flag = True  # 메시지 전송은 한번만
        with TCPSocket(host=self.host, port=self.port, timeout=connection_timeout) as s:
            while True:

                if flag:
                    s.sendall(packet)
                    flag = False

                packet = s.recv(1024, timeout=1)  # receive packet
                if not packet:
                    if time.time() - stime > connection_timeout:  # connection timeout
                        break
                    continue
