from abc import ABC, abstractmethod
from collections import defaultdict
import json
import os
import re
from typing import List, Set
import warnings

from cache.redis import Redis

from logger import LoggerMixin
from .orders import (
    Order,
    NewOrder,
    CancelOrder,
    ReceivedOrder,
    ExecutedOrder,
    OrderFactory,
)
from orders.history import OrderHisotryEnhanced
from sockets import TCPSocket


class QueryBuilder(ABC):
    buffer = []

    def reset_buffer(self):
        self.buffer = []

    def execute(self):
        """ main method """
        if not len(self.buffer):
            # raise ValueError("Please add queries before execution")
            result = None
        elif len(self.buffer) == 1:
            result = list(self.buffer)
        else:
            result = list(set.intersection(*self.buffer))  # and condition

        self.reset_buffer()
        return result

    @abstractmethod
    def add_query(self, **kwags):
        """ main method """
        # add something to buffer
        set_of_something = set("set of something")
        self._add_query_result_to_buffer(set_of_something)

    def _add_query_result_to_buffer(self, result: Set or List):
        """ support method """
        if not result:
            self.buffer.append(set())
        elif isinstance(result, list):
            self.buffer.append(set(result))
        elif isinstance(result, set):
            self.buffer.append(result)
        else:
            raise TypeError("result Type doens't match")


class OrderQueryBuilder(QueryBuilder, OrderHisotryEnhanced):
    def __init__(self, source="ram", *args, **kwargs):
        super(QueryBuilder, self).__init__(source=source, *args, **kwargs)

    def execute(self):
        result = super().execute()
        self.update()
        return result

    def _update_unex_qty(self, orders):
        """ overriden for faster unex_qty updates """

        for o in orders:
            # execution order or successful cancel order
            if isinstance(o, ExecutedOrder) or (
                isinstance(o, CancelOrder) and getattr(o, "response_code") == "0"
            ):
                order_no = getattr(o, "order_no")
                qty = int(getattr(o, "qty"))

                self.add_query(msg_type="0", order_no=order_no)
                try:
                    target_order = self.execute().pop()
                    target_order.subtract_unex_order_count(qty)

                except IndexError:  # empty
                    pass

    """ 
        Query Methods 
        
        low-level 
         - 쿼리를 조합한 후, execute() 매서드를 실행
         - 모든 쿼리는 and 조건으로 연결됨

        high-level
         - execute() 메서드 실행 x
         - 메서드 호출의 결과물로 쿼리 결과가 즉시 반영됨
    """

    # low-level query methods
    def add_query(self, orders=None, **kwargs):
        self.update()

        """ include orders that match key and value """
        for target_key, target_val in kwargs.items():
            self._validate_query_params(target_key, target_val)

            target = self._get_sorting_dict(sorting_key=target_key)
            result = target.get(target_val)
            self._add_query_result_to_buffer(result)

    def add_exclusive_query(self, **kwargs):
        """ exclude orders that match key and value """
        result = []

        for key, exclusive_value in kwargs.items():
            self._validate_query_params(key, exclusive_value)

            target = self._get_sorting_dict(sorting_key=key)

            for value, orders in target.items():
                if not value == exclusive_value:
                    result += orders

        self._add_query_result_to_buffer(result)

    def _validate_query_params(self, key, val):
        if not key in self.SORTING_KEYS:
            msg = f"sorting_key only support {self.SORTING_KEYS} but got {key}"
            raise SortingKeyNotSupported(msg)

    """ high-level query methods """

    def select_unexecuted_orders(self):
        self.add_query(msg_type="0", response_code="0")
        self.add_exclusive_query(unex_qty="00000")
        return self.execute()

    """ utility methods """

    def calc_ordered_qty_by_order_no(self, order_no: str) -> int:
        self.add_query(order_no=order_no, msg_type="0", response_code="0")
        orders = self.execute()
        return self.sum(orders, attr="qty")

    def calc_cancelled_qty_by_order_no(self, order_no: str) -> int:
        self.add_query(order_no=order_no, msg_type="1", response_code="0")
        orders = self.execute()
        return self.sum(orders, attr="qty")

    def calc_executed_qty_by_order_no(self, order_no: str) -> int:
        self.add_query(order_no=order_no, msg_type="3")
        orders = self.execute()
        return self.sum(orders, attr="qty")

    def calc_unexecuted_qty_by_order_no(self, order_no: str) -> int:
        order_qty = self.calc_ordered_qty_by_order_no(order_no)
        ex_qty = self.calc_executed_qty_by_order_no(order_no)  # int
        cancel_qty = self.calc_cancelled_qty_by_order_no(order_no)
        return order_qty - ex_qty - cancel_qty

    def sum(self, orders: List[Order], attr):
        if not len(orders):  # empty
            return None

        elif len(set(o.__class__ for o in orders)) > 1:  # all orders must be same type
            raise TypeError(f"Does not support mixed types ")

        result = sum([int(getattr(o, attr)) for o in orders])
        if result is None:
            return 0

        return result


class AXETaskQuerent(OrderQueryBuilder):
    def __init__(self, source="ram", *args, **kwargs):
        super().__init__(source=source, *args, **kwargs)

    """ 
    ================================
    ========== [구현 요소] =========
    ================================
    """

    def get_unex_qty_by_ticker(self, ticker: str):
        """ 1. 종목코드를 입력으로 해당 종목의 전체 미체결 수량을 반환하는 함수 """
        self.add_query(msg_type="0", response_code="0", ticker=ticker)
        self.add_exclusive_query(unex_qty="00000")
        unex_orders = self.execute()
        return self.sum(unex_orders, "unex_qty")

    def get_unex_qty_by_ticker_and_price(self, ticker: str, price: str):
        """ 2. 종목코드와 가격을 입력으로 전체 미체결 주문 목록을 반환하는 함수 """
        self.add_query(msg_type="0", response_code="0", ticker=ticker, price=price)
        self.add_exclusive_query(unex_qty="00000")
        unex_orders = self.execute()
        return self.sum(unex_orders, "unex_qty")

    def get_unex_orders_by_ticker(self, ticker: str):
        """ 3. 종목코드를 입력으로 전체 미체결 주문 목록을 반환하는 함수 """
        self.add_query(msg_type="0", response_code="0", ticker=ticker)
        self.add_exclusive_query(unex_qty="00000")
        return self.execute()

    def get_unex_orders_by_ticker_and_price(self, ticker: str, price: str):
        """ 4. 종목코드와 가격을 입력으로 특정 종목, 특정 가격의 미체결 주문 목록을 반환하는 함수"""
        self.add_query(msg_type="0", response_code="0", ticker=ticker, price=price)
        self.add_exclusive_query(unex_qty="00000")
        return self.execute()

    def get_unex_order_by_ticker_sorted(self, ticker: str):
        """ 5. 종목코드를 입력으로 가격을 첫번째 키로, 
        주문 시간을 두번째 키로 하여 정렬 된 미체결 주문 목록을 반환하는 함수
        """

        tree = defaultdict(lambda: {})  # TODO: message가 많아지면 Custom Tree Class로 대체

        # set in tree
        unex_orders = self.get_unex_orders_by_ticker(ticker)
        for order in unex_orders:
            price = getattr(order, "price")
            time = getattr(order, "time")

            tree[price][time] = order

        # sort
        result = []
        for p in sorted(tree.keys()):  # price로 정렬
            for t in sorted(tree.get(p).keys()):  # time으로 정렬
                result.append(tree[p][t])

        return result

    def get_order_by_ticker_and_order_no(self, ticker: str, order_no: str):
        """ 6. 종목코드와 주문번호를 입력으로 해당 주문을 리턴하는 함수 """
        self.add_query(msg_type="0", ticker=ticker, order_no=order_no)
        return self.execute()


if __name__ == "__main__":
    axe_qeurent = AXETaskQuerent()
    # print(axe_qeurent)
    print("1. 종목코드를 입력으로 해당 종목의 전체 미체결 수량을 반환하는 함수")
    print(axe_qeurent.get_unex_qty_by_ticker("000660"))
    print("=" * 100)

    print(""" 2. 종목코드와 가격을 입력으로 전체 미체결 주문 목록을 반환하는 함수 """)
    print(axe_qeurent.get_unex_qty_by_ticker_and_price("000660", "60000"))
    print("=" * 100)

    print(""" 3. 종목코드를 입력으로 전체 미체결 주문 목록을 반환하는 함수 """)
    print(axe_qeurent.get_unex_orders_by_ticker("000660"))
    print("=" * 100)

    print(""" 4. 종목코드와 가격을 입력으로 특정 종목, 특정 가격의 미체결 주문 목록을 반환하는 함수""")
    print(axe_qeurent.get_unex_orders_by_ticker_and_price("000660", "60000"))
    print("=" * 100)

    print(
        """ 5. 종목코드를 입력으로 가격을 첫번째 키로, 
        주문 시간을 두번째 키로 하여 정렬 된 미체결 주문 목록을 반환하는 함수
        """
    )
    print(axe_qeurent.get_unex_order_by_ticker_sorted("000660"))
    print("=" * 100)

    """ 6. 종목코드와 주문번호를 입력으로 해당 주문을 리턴하는 함수 """
    print(axe_qeurent.get_order_by_ticker_and_order_no("000660", "00001"))
    print("=" * 100)


class SortingKeyNotSupported(Exception):
    pass
