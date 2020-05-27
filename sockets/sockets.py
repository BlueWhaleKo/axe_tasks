import socket
import logging
import select

from logger import LoggerBuidler, LoggerMixin


class TCPSocket(socket.socket, LoggerMixin):
    """ TCP/IP 소켓 """

    def __init__(self, host, port, timeout=5, *args, **kwargs):
        super().__init__(socket.AF_INET, socket.SOCK_STREAM, *args, **kwargs)
        self.host = host
        self.port = port

    def connect(self, host=None, port=None) -> None:
        if host is not None:
            self.host = host
        if port is not None:
            self.port = port

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

    def sendall(self, data: bytes, auto_reconnect=True):
        try:
            super().sendall(data)
        except Exception as e:  # 정확한 에러명(connection error) 확인 후 수정
            if auto_reconnect:
                self.connect()
                super().sendall(data)
            else:
                raise e()
        self.logger.debug(data)  # log when success only
        return

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()
        return super().__exit__(*args)

    def __del__(self, *args, **kwargs):
        self.close()
        return super().__del__(*args, **kwargs)
