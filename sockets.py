import socket
import logging
import select

from logger import LoggerBuidler

loggers = {}  # prevent duplicated handler


class TCPSocket(socket.socket):
    """ TCP/IP 소켓 """

    LOG_PATH = f"logger/.logs/packets.log"

    def __init__(self, host, port, timeout=5, *args, **kwargs):
        super().__init__(socket.AF_INET, socket.SOCK_STREAM, *args, **kwargs)

        self.host = host
        self.port = port

    @property
    def logger(self):
        global loggers

        logger = loggers.get(__name__)
        if logger:
            return logger

        # if not exists, build a new one
        logger_builder = LoggerBuidler(__name__)
        logger_builder.addFileHandler(self.LOG_PATH)
        logger_builder.addStreamHandler()
        logger = logger_builder.build()
        
        loggers[__name__] = logger
        return logger

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

    def __exit__(self, *args):
        self.close()
        return super().__exit__(*args)
