from abc import ABC, abstractmethod
from datetime import datetime
import logging
import inspect
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

        self.socket = TCPSocket(host=host, port=port)
        self.packet_decoder = PacketDecoder()
        self.msg_factory = MessageFactory()

    def sendall(self, packet: bytes, timeout=None) -> bool:
        """ 
        Returns
        =======
        bool: True if succeed, False when failed
        """
        is_success = False

        stime = time.time()
        self.socket.sendall(packet)

        while True:
            s_packet = self.socket.recv(1024, timeout=0.5)  # receive packet
            if not s_packet:

                if timeout is None or time.time() - stime > timeout:
                    break
                continue

            # check if sending msg has succeeded
            msg_kwargs = self.packet_decoder.decode(s_packet)
            for kw in msg_kwargs:
                msg_type = kw.get("msg_type")
                res_code = kw.get("response_code")

                if (
                    msg_type == OrderReceivedMessage.MSG_TYPE
                    and res_code == OrderReceivedMessage.SUCCESS
                ):
                    is_success = True

        if is_success:
            self.socket.logger.debug("SUCCESS")
        else:
            self.socket.logger.warning(f"FAILED")
        return is_success
