from collections import defaultdict
import json
import os
import re
from typing import List
import warnings

from cache.redis import Redis

from logger import LoggerMixin
from .orders import (
    Order,
    NewOrder,
    CancelOrder,
    ReceivedOrder,
    ExecutedOrder,
    UnexecutedOrder,
    OrderFactory,
)
from sockets import TCPSocket


class History:
    history = []

    def __iter__(self):
        return iter(self.history)

    def __getitem__(self, idx):
        return self.history[idx]

    def __len__(self):
        return len(self.history)

    def __str__(self):
        return str(self.history)

    def __repr__(self):
        return str(self.history)


class Singleton(object):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not isinstance(cls._instance, cls):
            cls._instance = object.__new__(cls, *args, **kwargs)
        return cls._instance


class OrderHistory(History, Singleton, LoggerMixin):
    @property
    def log_path(self):
        return "logger/.logs/orders.client.log"

    def __init__(self, source="ram"):

        """
        Parameters
        ==========
        source: str
            data source, 
            if "ram"  : read from Redis 
            elif "disk" : read from log file
        """

        self.source = source.lower()

        self._history = defaultdict(lambda: [])  # {ClassName: [OrderClass]}
        self._last_modified = None

        self._last_redis_idx = defaultdict(lambda: 0)  # idx per key for redis
        self._last_history_idx = 0  # last line idx for updating from DISK

        self.redis = Redis(host="127.0.0.1", port="6379")
        self.factory = OrderFactory()

    """ 
    methods for real-time message history updating 

    TODO: 매번 disk에서 새롭게 읽지 말고 in-memory DB(Redis)에 캐싱해서 속도 개선 
    """

    @property
    def history(self):
        self.update()
        return self._history

    def update(self) -> None:
        """ return updated history """

        if self._last_modified == os.path.getmtime(self.log_path):  # no need
            return

        update_method = getattr(self, f"_update_from_{self.source}")
        update_method()
        self._last_modified = os.path.getmtime(self.log_path)

    def _update_from_ram(self):
        key_pattern = "*Order"
        keys = self.redis.scan_iter(key_pattern)

        for k in keys:
            # get cache from last idx & update idx
            start_idx = self._last_redis_idx[k]
            cache = self.redis.lrange(key=k, start=start_idx)
            self._last_redis_idx[k] += len(cache)

            # translate cache into orders and update history
            order_kwargs = [json.loads(k) for k in cache]
            orders = [self.factory.create(**kw) for kw in order_kwargs]

            self._add_order(*orders)

    def _update_from_disk(self):
        try:  # read log
            with open(self.log_path, "r") as f:
                new_lines = f.readlines()[self._last_history_idx :]
                new_lines = [l.rstrip() for l in new_lines]

                self._last_history_idx += len(new_lines)

        except FileNotFoundError:
            warnings.warn(f"{self.log_path} doens't exists")
            return None

        # translate log
        order_kwargs = [self._parse_dict(l) for l in new_lines]
        orders = [self.factory.create(**kw) for kw in order_kwargs]
        self._add_order(*orders)

    def _add_order(self, *order: Order):
        for o in order:
            key = o.__class__.__name__  # class name
            self._history[key].append(order)

    def _parse_dict(self, str_: str):
        """ pattern: {....} """
        pattern = re.compile(r"[{].+[}]")  # data
        return json.loads(pattern.search(str_).group())


class OrderQuerent(OrderHistory):

    """ 
    query method를 조합하여 원하는 형태로 쿼리 가능
     -  low-level query(select_by_<....>)를 먼저 적용한 후, 
     -   high-level qyery(select_<...>_orders)를 적용하는 것을 권장
    """

    # high-level query methods
    """
    parameters 
    ==========
    orders: List[Order] 
        여러 쿼리를 중복으로 적용 가능
        >> 이전에 호출한 query method의 return 값을 orders argument로 넣어줄 수 있습니다.
    """

    def select_unexecuted_orders(self, orders: List[Order] = None):
        """ select <UnexecutedOrder> that are 
            - successful 
            - not cancelled 
            - not wholly executed
        
        Returns
        =======
        List[UnexecutedOrder]
            UnexecutedOrder 클래스는 NewOrder 클래스를 상속
            qty를 주문 수량에서 미체결 수랑으로 덮어쓴다
        """
        unexecuted_orders = []

        new_msgs = self.select_by_cls(NewOrder, orders)
        succ_new_msgs = self.select_by_response_code("0", new_msgs)  # 유효한 신규 주문

        for order in succ_new_msgs:
            order_no = getattr(order, "order_no")
            unex_qty = self.calc_unexecuted_qty_by_order_no(order_no)

            if unex_qty > 0:  # not 0
                unex_order = UnexecutedOrder(**order.__dict__)
                setattr(unex_order, "unex_qty", str(unex_qty).zfill(5))

                unexecuted_orders.append(unex_order)

        return unexecuted_orders

    # low-level qeury methods
    def select_by_cls(self, cls: Order, orders: List[Order] = None):
        if orders is None:
            orders = self.history
        return [m for m in orders if isinstance(m, cls)]

    def select_by_ticker(self, ticker: str, orders: List[Order] = None):
        if orders is None:
            orders = self.history
        return [m for m in orders if getattr(m, "ticker", "") == ticker]

    def select_by_order_no(self, order_no: str, orders: List[Order] = None):
        if orders is None:
            orders = self.history
        return [m for m in orders if getattr(m, "order_no", "") == order_no]

    def select_by_price(self, price: str, orders: List[Order] = None):
        if orders is None:
            orders = self.history
        return [m for m in orders if getattr(m, "price", "") == price]

    def select_by_response_code(self, response_code: str, orders: List[Order] = None):
        if orders is None:
            orders = self.history
        return [m for m in orders if getattr(m, "response_code", "") == response_code]

    """ utility methods """

    def _sum_qty(self, orders: List[Order]):
        """ 인자로 받은 Order Class의 qty 속성값 합을 반환
        미체결 주문(UnexecutedOrder)은 unex_qty 속성의 합을 반환
        """
        if not len(orders):  # empty
            return 0

        if len(set(m.__class__ for m in orders)) > 1:  # all orders must be same type
            raise TypeError(
                f"Mixed types not supported {set(m.__class__ for m in orders)}"
            )

        if isinstance(orders[0].__class__, UnexecutedOrder):
            return sum([int(m.unex_qty) for m in orders])
        return sum([int(m.qty) for m in orders])

    def calc_ordered_qty_by_order_no(self, order_no: str) -> int:
        orders = self.select_by_order_no(order_no)
        orders = self.select_by_cls(NewOrder, orders)
        succ_order_msgs = self.select_by_response_code("0", orders)
        return self._sum_qty(succ_order_msgs)

    def calc_cancelled_qty_by_order_no(self, order_no: str) -> int:
        orders = self.select_by_order_no(order_no)
        orders = self.select_by_cls(CancelOrder, orders)
        succ_cancel_msgs = self.select_by_response_code("0", orders)
        return self._sum_qty(succ_cancel_msgs)

    def calc_executed_qty_by_order_no(self, order_no: str) -> int:
        orders = self.select_by_order_no(order_no)
        orders = self.select_by_cls(ExecutedOrder, orders)
        return self._sum_qty(orders)

    def calc_unexecuted_qty_by_order_no(self, order_no: str) -> int:
        order_qty = self.calc_ordered_qty_by_order_no(order_no)
        ex_qty = self.calc_executed_qty_by_order_no(order_no)  # int
        cancel_qty = self.calc_cancelled_qty_by_order_no(order_no)
        return order_qty - ex_qty - cancel_qty


class AXETaskQuerent(OrderQuerent):

    """ 
    ================================
    ========== [구현 요소] =========
    ================================
    """

    def get_unex_qty_by_ticker(self, ticker: str):
        """ 1. 종목코드를 입력으로 해당 종목의 전체 미체결 수량을 반환하는 함수 """

        orders = self.select_by_ticker(ticker)  # default: ticker matching
        unex_orders = self.select_unexecuted_orders(orders)
        return self._sum_qty(unex_orders)

    def get_unex_qty_by_ticker_and_price(self, ticker: str, price: str or int):
        """ 2. 종목코드와 가격을 입력으로 전체 미체결 주문 목록을 반환하는 함수 """

        if isinstance(price, int):
            price = str(price).zfill(5)

        orders = self.select_by_ticker(ticker)
        orders = self.select_by_price(price, orders)  # default

        unex_orders = self.select_unexecuted_orders(orders)
        return self._sum_qty(unex_orders)

    def get_unex_orders_by_ticker(self, ticker: str):
        """ 3. 종목코드를 입력으로 전체 미체결 주문 목록을 반환하는 함수 """

        orders = self.select_by_ticker(ticker)
        return self.select_unexecuted_orders(orders)

    def get_unex_orders_by_ticker_and_price(self, ticker: str, price: str or int):
        """ 4. 종목코드와 가격을 입력으로 특정 종목, 특정 가격의 미체결 주문 목록을 반환하는 함수"""

        if isinstance(price, int):
            price = str(price).zfill(5)

        orders = self.select_by_ticker(ticker)
        orders = self.select_by_price(price, orders)
        return self.select_unexecuted_orders(orders)

    def get_unex_order_by_ticker_sorted(self, ticker: str):
        """ 5. 종목코드를 입력으로 가격을 첫번째 키로, 
        주문 시간을 두번째 키로 하여 정렬 된 미체결 주문 목록을 반환하는 함수
        """

        tree = defaultdict(lambda: {})  # TODO: message가 많아지면 Custom Tree Class로 대체

        unex_orders = self.get_unex_orders_by_ticker(ticker)
        for order in unex_orders:
            price = getattr(order, "price")
            time = getattr(order, "time")

            tree[price][time] = order

        result = []
        for p in sorted(tree.keys()):  # price로 정렬
            for t in sorted(tree.get(p).keys()):  # time으로 정렬
                result.append(tree[p][t])

        return result

    def get_order_by_ticker_and_order_no(self, ticker: str, order_no: str):
        """ 6. 종목코드와 주문번호를 입력으로 해당 주문을 리턴하는 함수 """

        orders = self.select_by_cls(NewOrder)
        orders = self.select_by_ticker(ticker, orders)
        return self.select_by_order_no(order_no, orders)


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
