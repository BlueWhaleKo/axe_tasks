import os
import socket
import time

from messages.messages import MessageFactory, PacketDecoder
from messages.querent import AXETaskQuerent
from sockets import TCPSocket
from client import Client

import unittest


class ClientTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(ClientTest, self).__init__(*args, **kwargs)
        self.msg_factory = MessageFactory()
        self.packet_decoder = PacketDecoder()
        self.axe_querent = AXETaskQuerent()

        self.host = "114.204.7.144"
        self.port = 12345
        self.client = Client(self.host, self.port)
        self.socket_timeout = 1

    def test_senario(self):
        self._clear_log()
        self._send_reset_message()
        self._send_first_message()
        self._send_second_message()
        self._send_third_message()
        self._send_fourth_message()

    def _clear_log(self):
        if os.path.exists(TCPSocket.LOG_PATH):
            os.remove(TCPSocket.LOG_PATH)

    def _send_reset_message(self):
        packet = b"reset"
        self.client.sendall(packet, self.socket_timeout)

    def _send_first_message(self):
        kwargs = {
            "msg_type": "0",
            "order_no": "00000",
            "ticker": "000660",
            "price": "60000",
            "qty": "00020",
        }
        msg = self.msg_factory.create(**kwargs)
        packet = msg.encode()
        self.client.sendall(packet, self.socket_timeout)

    def _send_second_message(self):
        kwargs = {
            "msg_type": "0",
            "order_no": "00000",
            "ticker": "000660",
            "price": "61000",
            "qty": "00030",
        }
        msg = self.msg_factory.create(**kwargs)
        packet = msg.encode()
        self.client.sendall(packet, self.socket_timeout)

    def _send_third_message(self):
        unex_orders = self.axe_querent.get_unex_orders_by_ticker_and_price(
            ticker="000660", price="60000"
        )
        self.assertEqual(len(unex_orders), 1)

        unex_order = unex_orders.pop()
        order_no = getattr(unex_order, "order_no")
        qty = "10".zfill(5)

        kwargs = {
            "msg_type": "1",
            "order_no": order_no,
            "ticker": "000660",
            "price": "60000",
            "qty": "00010",
        }
        msg = self.msg_factory.create(**kwargs)
        packet = msg.encode()
        self.client.sendall(packet, self.socket_timeout)

    def _send_fourth_message(self):
        unex_orders = self.axe_querent.get_unex_orders_by_ticker_and_price(
            ticker="000660", price="61000"
        )
        self.assertEqual(len(unex_orders), 1)

        unex_order = unex_orders.pop()
        order_no = getattr(unex_order, "order_no")
        qty = getattr(unex_order, "qty")
        kwargs = {
            "msg_type": "1",
            "order_no": order_no,
            "ticker": "000660",
            "price": "60000",
            "qty": qty,
        }
        msg = self.msg_factory.create(**kwargs)
        packet = msg.encode()
        self.client.sendall(packet, self.socket_timeout)


if __name__ == "__main__":
    unittest.main()
