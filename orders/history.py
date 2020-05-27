from abc import ABC, abstractmethod
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
    OrderFactory,
)

"""
class Singleton(object):
    _instance = None

    def __new__(cls, source, *args, **kwargs):  # source
        if not isinstance(cls._instance, cls):
            cls._instance = super(Singleton, cls).__new__(cls, *args, **kwargs)
        return cls._instance
"""


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


class OrderHistory(History, LoggerMixin):
    def __init__(self, source="ram", *args, **kwargs):
        super().__init__(*args, **kwargs)
        """
        Parameters
        ==========
        source: str
            data source, 
            if "ram"  : read from Redis 
            elif "disk" : read from log file
        """

        self.source = source.lower()

        self._history = []  # {ClassName: [OrderClass]}
        self._last_modified = None

        self._last_redis_idx = defaultdict(lambda: 0)  # idx per key for redis
        self._last_history_idx = 0  # last line idx for updating from DISK

        self.redis = Redis(host="127.0.0.1", port="6379")
        self.factory = OrderFactory()

        self.update()

    """ 
    methods for real-time message history updating 
    """

    @property
    def history(self):
        self.update()
        return self._history

    def update(self):
        new_orders = self.load_new_orders()
        if new_orders:
            self._update(new_orders)  # _update 사항 일괄 적용

    def _update(self, orders: List[Order]):
        self._history.extend(orders)

        self._update_unex_qty(orders)
        self._last_modified = os.path.getmtime(self.log_path)

    def _update_unex_qty(self, orders):
        """ 미체결 수량은 실시간으로 계산하여 업데이트 해야 함
            효율적인 계산을 위해 QueryBuild를 상속한 모듈에서 다시 구현
        """
        for o in orders:

            # execution order or successful cancel order
            if isinstance(o, ExecutedOrder) or (
                isinstance(o, CancelOrder) and getattr(o, "response_code") == "0"
            ):
                order_no = getattr(o, "order_no")
                qty = int(getattr(o, "qty"))

                for target_order in self._history:
                    if (
                        isinstance(target_order, NewOrder)
                        and getattr(target_order, "order_no") == order_no
                        and getattr(target_order, "response_code") == "0"
                    ):  # target matching
                        target_order.subtract_unex_order_count(qty)

    def load_new_orders(self) -> List[Order]:
        """ return new orders which is not in history """

        if self._last_modified == os.path.getmtime(self.log_path):  # no need
            return

        loading_method = getattr(self, f"_load_new_orders_from_{self.source}")
        return loading_method()

    """ load from RAM """

    def _load_new_orders_from_ram(self) -> List[Order]:
        new_orders = []

        key_pattern = "*Order"  #
        keys = self.redis.scan_iter(key_pattern)

        for k in keys:
            # get cache from last idx & _update idx
            start_idx = self._last_redis_idx[k]
            cache = self.redis.lrange(key=k, start=start_idx)
            self._last_redis_idx[k] += len(cache)

            # translate cache into orders and _update history
            order_kwargs = [json.loads(k) for k in cache]
            orders = [self.factory.create(**kw) for kw in order_kwargs]

            new_orders.extend(orders)

        return new_orders

    """ load from log file """

    @property
    def log_path(self):
        return "logger/.logs/client.log"

    def _load_new_orders_from_disk(self) -> List[Order]:
        new_orders = []

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
        new_orders = [self.factory.create(**kw) for kw in order_kwargs]
        return new_orders

    def _add_order_to_history_by_cls_name(self, *order: Order):
        for o in order:
            key = o.__class__.__name__  # class name
            self._history[key].append(order)

    def _parse_dict(self, str_: str):
        """ pattern: {....} """
        pattern = re.compile(r"[{].+[}]")  # data
        return json.loads(pattern.search(str_).group())


class OrderHisotryEnhanced(OrderHistory):
    """ Key Sorting Added for Faster Query """

    SORTING_KEYS = [
        "msg_type",
        "ticker",
        "price",
        "response_code",
        "order_no",
        "unex_qty",
    ]

    def __init__(self, source="ram", *args, **kwargs):
        super().__init__(source=source, *args, **kwargs)

    def _update(self, orders: List[Order]) -> None:
        """ has been overriden to add "sorting dicts" for faster query """
        super()._update(orders)

        for key in self.SORTING_KEYS:
            self._update_sorting_dict(orders, sorting_key=key)

    def _update_sorting_dict(self, orders: List[Order], sorting_key):
        target = self._get_sorting_dict(sorting_key)

        # _update
        for o in orders:
            try:
                value = getattr(o, sorting_key)
            except AttributeError:
                continue

            if value:
                target[value].append(o)

    def _get_sorting_dict(self, sorting_key):
        self.update()

        # get sorting dict
        property_name = f"_orders_sort_by_{sorting_key}"
        try:
            target = getattr(self, property_name)
        except AttributeError:
            target = defaultdict(lambda: [])
            setattr(self, property_name, target)

        return target
