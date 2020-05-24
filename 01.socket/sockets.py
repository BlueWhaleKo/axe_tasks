import socket
from contextlib import contextmanager
import logging
import select

import inspect
from logger import LoggerBuidler

"""
logging.basicConfig(
    level=logging.DEBUG, format="[%(levelname)s] [%(asctime)s] %(message)s",
)"""


class TCPSocket(socket.socket):
    """ TCP/IP 소켓 """

    log_path = f"logger/.logs/{__name__}.log"

    def __init__(self, host, port, timeout=5, *args, **kwargs):
        super().__init__(socket.AF_INET, socket.SOCK_STREAM, *args, **kwargs)

        self.host = host
        self.port = port

        logger_builder = LoggerBuidler(__name__)
        logger_builder.addFileHandler(self.log_path)
        logger_builder.addStreamHandler()
        self.logger = logger_builder.build()

    def connect(self) -> None:
        return super().connect((self.host, self.port))

    def close(self) -> None:
        return super().close()

    def recv(self, bufsize, timeout=3):
        """ recv timeout added """
        ready = select.select([self], [], [], timeout)

        if ready[0]:
            packet = super().recv(bufsize)
            self.logger.debug(packet)
            return packet

    def sendall(self, data: bytes):
        self.logger.debug(data)
        return super().sendall(data)

    def __enter__(self):
        self.connect()
        return self


"""
@contextmanager
def connect_tcp_socket(host, port, timeout=5, *args, **kwargs):
    family = socket.AF_INET
    type_ = socket.SOCK_STREAM

    s = socket.socket(family, type_, *args, **kwargs)
    s.settimeout(0)
    s.connect((host, port))

    try:
        yield s
    finally:
        s.close()
"""
