import glob
import inspect
import os
import socket
import time

from logger import LoggerMixin
from messages.messages import MessageFactory
from orders.client import Client
from orders.querent import AXETaskQuerent

import unittest


class ClientTest(unittest.TestCase, LoggerMixin):
    @property
    def log_path(self):
        return "logger/.logs/test.log"

    def __init__(self, *args, **kwargs):
        super(ClientTest, self).__init__(*args, **kwargs)
        self.msg_factory = MessageFactory()
        self.axe_querent = AXETaskQuerent()

        self.host = "114.204.7.144"
        self.port = 12345
        self.client = Client(self.host, self.port)

    def test_senario(self):
        self._reset()
        self._send_reset_message()
        self._send_first_message()
        self._send_second_message()
        self._send_third_message()
        self._send_fourth_message()

    def _reset(self):
        self.client.redis.flushall()  # reset cache

        # reset log
        for dir_path, dir_names, f_names in os.walk("logger/.logs"):
            for f in f_names:
                if not f[-4:] == ".log":
                    continue

                f_path = os.path.join(dir_path, f)
                os.remove(f_path)

    def _send_reset_message(self):
        self.client.sendall(b"reset")

    def _send_first_message(self):
        kwargs = {
            "msg_type": "0",
            "order_no": "00000",
            "ticker": "000660",
            "price": "60000",
            "qty": "00020",
        }
        packet = "".join(kwargs.values()).encode()
        self.client.sendall(packet)

    def _send_second_message(self):
        kwargs = {
            "msg_type": "0",
            "order_no": "00000",
            "ticker": "000660",
            "price": "61000",
            "qty": "00030",
        }
        packet = "".join(kwargs.values()).encode()
        self.client.sendall(packet)

    def _send_third_message(self):
        unex_orders = self.axe_querent.get_unex_orders_by_ticker_and_price(
            ticker="000660", price="60000"
        )
        if not unex_orders:  # all executed
            self.logger.debug("all orders are execued")
            return

        unex_order = unex_orders.pop()
        print(unex_order)
        self.logger.debug("UnexecutedOrder Info" + str(unex_order))

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
        packet = "".join(kwargs.values()).encode()
        is_success = self.client.sendall(packet)

        if not is_success:  # retry
            method = getattr(self, inspect.stack()[0][3])  # recall
            self.logger.debug(f"{method.__name__}() has failed, retrying")
            method()

    def _send_fourth_message(self):
        unex_orders = self.axe_querent.get_unex_orders_by_ticker_and_price(
            ticker="000660", price="61000"
        )
        if not unex_orders:  # all executed
            self.logger.debug("all orders are execued")
            return

        unex_order = unex_orders.pop()
        self.logger.debug("UnexecutedOrder Info" + str(unex_order))

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
        packet = "".join(kwargs.values()).encode()
        is_success = self.client.sendall(packet)

        if not is_success:
            method = getattr(self, inspect.stack()[0][3])
            self.logger.debug(f"{method.__name__}() has failed, retrying")
            method()


if __name__ == "__main__":
    unittest.main()
