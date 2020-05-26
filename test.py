import inspect
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
        self.client.sendall(packet)

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
        self.client.sendall(packet)

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
        self.client.sendall(packet)

    def _send_third_message(self):
        unex_orders = self.axe_querent.get_unex_orders_by_ticker_and_price(
            ticker="000660", price="60000"
        )
        if not unex_orders:  # all executed
            print("all orders are execued")
            return

        unex_order = unex_orders.pop()
        print("Unexecuted Order Info", unex_order)

        order_no = getattr(unex_order, "order_no")
        unex_qty = getattr(unex_order, "unex_qty")
        qty = min(10, int(unex_qty))
        qty = str(qty).zfill(5)

        kwargs = {
            "msg_type": "1",
            "order_no": order_no,
            "ticker": "000660",
            "price": "60000",
            "qty": qty,
        }
        msg = self.msg_factory.create(**kwargs)
        packet = msg.encode()
        is_success = self.client.sendall(packet)

        if not is_success:
            method = getattr(self, inspect.stack()[0][3])  # recall
            print(f"{method.__name__}() has failed, retrying")
            method()

    def _send_fourth_message(self):
        unex_orders = self.axe_querent.get_unex_orders_by_ticker_and_price(
            ticker="000660", price="61000"
        )
        if not unex_orders:  # all executed
            print("all orders are execued")
            return

        unex_order = unex_orders.pop()
        print("Unexecuted Order Info", unex_order)

        order_no = getattr(unex_order, "order_no")
        unex_qty = getattr(unex_order, "unex_qty")
        qty = min(10, int(unex_qty))
        qty = str(qty).zfill(5)

        kwargs = {
            "msg_type": "1",
            "order_no": order_no,
            "ticker": "000660",
            "price": "60000",
            "qty": qty,
        }
        msg = self.msg_factory.create(**kwargs)
        packet = msg.encode()
        is_success = self.client.sendall(packet)

        if not is_success:
            method = getattr(self, inspect.stack()[0][3])
            print(f"{method.__name__}() has failed, retrying")
            method()


if __name__ == "__main__":
    unittest.main()
