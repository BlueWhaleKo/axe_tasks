from abc import ABC, abstractmethod
from datetime import datetime
import json
import logging
import inspect
import socket
import time

from logger import LoggerBuidler, LoggerMixin
from sockets import TCPSocket
from messages.querent import MessageHistory
from messages.messages import (
    Message,
    NewOrderMessage,
    OrderReceivedMessage,
    OrderExecutedMessage,
    MessageFactory,
)
from sockets.decoder import PacketDecoder


class Client(LoggerMixin):
    def __init__(self, host, port):
        self.host = host
        self.port = port

        self.socket = TCPSocket(host=host, port=port)

        self.packet_decoder = PacketDecoder()

    def sendall(self, packet: bytes) -> bool:
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
                break

            self.logger.debug(f"[SERVER] RECEIVED PACKET {s_packet}")

            # check if sending msg has succeeded
            msg_kwargs = self.packet_decoder.decode(s_packet)

            for kw in msg_kwargs:
                self.logger.debug(
                    f"[SERVER] RECEIVED DETAIL {json.dumps(kw, indent=4)}"
                )

                msg_type = kw.get("msg_type")
                res_code = kw.get("response_code")

                if (
                    msg_type == OrderReceivedMessage.MSG_TYPE
                    and res_code == OrderReceivedMessage.SUCCESS
                ):
                    is_success = True

        if is_success:
            self.logger.debug(f"[CLIENT] SENDING PACKET SUCCESSFUL {packet}")
        else:
            self.logger.warning(f"[CLIENT] SENDING PACKET FAILED {packet}")
        return is_success
