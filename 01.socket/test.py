import socket
import time

from messages.messages import MessageFactory
from messages.decoder import PacketDecoder
from sockets import TCPSocket
from account import Client


def create_msg():
    message_factory = MessageFactory()

    kwargs = {
        "msg_type": "0",
        "order_no": "".zfill(5),
        "ticker": "000660",
        "price": "60000",
        "qty": "20".zfill(5),
    }
    return message_factory.create(**kwargs)


def test_socket_server():
    HOST = "114.204.7.144"
    PORT = 12345

    msg = create_msg()
    packet = msg.encode()
    packet_decoder = PacketDecoder()

    order_flag = True
    # send & receive packet
    for _ in range(1):
        with TCPSocket(host=HOST, port=PORT, timeout=5) as s:
            while True:
                if order_flag:
                    s.sendall(packet)
                    order_flag = False

                packet = s.recv(1024, timeout=3)

                if packet:
                    print(packet)
                    msg_kwargs = packet_decoder.decode(packet)
                    print(msg_kwargs)
                else:
                    break


def test_daemon_thread():
    HOST = "114.204.7.144"
    PORT = 12345

    # create packet
    msg = create_msg()

    client = Client(host=HOST, port=PORT)
    client.send_msg(msg)


if __name__ == "__main__":
    # test_socket_server()
    test_daemon_thread()
