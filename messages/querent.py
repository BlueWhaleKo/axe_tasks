from collections import defaultdict
from copy import deepcopy
import os
import re
from typing import List

from .messages import (
    Message,
    NewOrderMessage,
    CancelOrderMessage,
    OrderReceivedMessage,
    OrderExecutedMessage,
    MessageFactory,
    PacketDecoder,
    UnexecutedOrder,
)

from sockets import TCPSocket


class MessageHistory:
    def __init__(self):
        self.log_path = "logger/.logs/sockets.log"

        self.msg_factory = MessageFactory()
        self.packet_decoder = PacketDecoder()

        self._history = []
        self._last_modified = None

        self._last_client_msg = None

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

    """ 
    methods for real-time message history updating 

    TODO: 매번 disk에서 새롭게 읽지 말고 in-memory DB(Redis)에 캐싱해서 속도 개선 
    """

    @property
    def history(self):
        """ refresh per every call """
        if not os.path.exists(self.log_path):  # if log file doesn't exist
            return self._history

        if self._last_modified is None:  # initial call
            self._last_modified = os.path.getmtime(self.log_path)
            self._update_history()

        elif self._last_modified != os.path.getmtime(self.log_path):  # has changed
            self._update_history()  # refresh

        return self._history  # (not initial call) and (no change)

    def _update_history(self):
        self._history = []

        with open(self.log_path, "r") as f:
            lines = [line.rstrip() for line in f]

        for l in lines:
            pk = self._parse_packet(l)
            time = self._parse_time(l)

            if pk is None:  # no matching data
                continue

            msgs = self.msg_factory.create_from_packet(pk)

            for m in msgs:
                setattr(m, "time", time)  # 모든 메시지에 time property 추가

                if isinstance(m, (NewOrderMessage, CancelOrderMessage)):
                    self._last_client_msg = m  # NewOrderMessage는 order_no가 비어있음

                if isinstance(m, OrderReceivedMessage):
                    # 따라서, OrderReceivedMessage를 받은 이후에 order_no을 채워줘야 함
                    if isinstance(self._last_client_msg, NewOrderMessage):
                        setattr(self._last_client_msg, "order_no", m.order_no)

                    setattr(self._last_client_msg, "response_code", m.response_code)

                self._history.append(m)

    def _parse_packet(self, str_: str):
        """ pattern: b'00000202000020'"""
        pattern = re.compile(r"[b'][0-9]+[']")  # data
        try:
            packet = pattern.search(str_).group()
            packet = packet.replace("b", "").replace("'", "")
        except AttributeError:
            return None

        if not packet == "2000000":  # reset 확인 메시지는 parsing 하지 않음
            return packet

    def _parse_time(self, str_: str):
        """ pattern: 2020-05-25 17:07:22,166 """
        pattern = re.compile(r"\d+[-]\d+[-]\d+ \d+[:]\d+[:]\d+[,]\d+")
        time = pattern.search(str_).group()
        return time


class MessageQuerent(MessageHistory):

    """ 
    query method를 조합하여 원하는 형태로 쿼리 가능
     -  low-level query(select_by_<....>)를 먼저 적용한 후, 
     -   high-level qyery(select_<...>_orders)를 적용하는 것을 권장
    """

    # high-level query methods
    """
    parameters 
    ==========
    msgs: List[Message] 
        여러 쿼리를 중복으로 적용 가능
        >> 이전에 호출한 query method의 return 값을 msgs argument로 넣어줄 수 있습니다.
    """

    def select_unexecuted_orders(self, msgs: List[Message] = None):
        """ select <UnexecutedOrder> that are 
            - successful 
            - not cancelled 
            - not wholly executed
        
        Returns
        =======
        List[UnexecutedOrder]
            UnexecutedOrder 클래스는 NewOrderMessage 클래스를 상속
            qty를 주문 수량에서 미체결 수랑으로 덮어쓴다
        """
        unexecuted_orders = []

        new_msgs = self.select_by_cls(NewOrderMessage, msgs)
        succ_new_msgs = self.select_by_response_code("0", new_msgs)  # 유효한 신규 주문

        for order in succ_new_msgs:
            order_no = getattr(order, "order_no")
            unex_qty = self.calc_unexecuted_qty_by_order_no(order_no)

            if unex_qty:  # not 0
                unex_order = UnexecutedOrder(**(order.__dict__))
                unex_qty = str(unex_qty).zfill(5)  # int -> str
                setattr(unex_order, "unex_qty", unex_qty)
                unexecuted_orders.append(unex_order)

        return unexecuted_orders

    # low-level qeury methods
    def select_by_cls(self, cls_: Message, msgs: List[Message] = None):
        if msgs is None:
            msgs = self.history
        return [m for m in msgs if isinstance(m, cls_)]

    def select_by_ticker(self, ticker: str, msgs: List[Message] = None):
        if msgs is None:
            msgs = self.history
        return [m for m in msgs if getattr(m, "ticker", "") == ticker]

    def select_by_order_no(self, order_no: str, msgs: List[Message] = None):
        if msgs is None:
            msgs = self.history
        return [m for m in msgs if getattr(m, "order_no", "") == order_no]

    def select_by_price(self, price: str, msgs: List[Message] = None):
        if msgs is None:
            msgs = self.history
        return [m for m in msgs if getattr(m, "price", "") == price]

    def select_by_response_code(self, response_code: str, msgs: List[Message] = None):
        if msgs is None:
            msgs = self.history
        return [m for m in msgs if getattr(m, "response_code", "") == response_code]

    """ utility methods """

    def _sum_qty(self, msgs: List[Message]):
        """ 인자로 받은 Message Class의 qty 속성값 합을 반환
        미체결 주문(UnexecutedOrder)은 unex_qty 속성의 합을 반환
        """
        if not len(msgs):  # empty
            return 0

        if len(set(m.__class__ for m in msgs)) > 1:  # all msgs must be same type
            raise TypeError(f"Mixed types not supported {set(m.__class__ for m in msgs)}")

        if isinstance(msgs[0].__class__, UnexecutedOrder):
            return sum([int(m.unex_qty) for m in msgs])
        return sum([int(m.qty) for m in msgs])

    def calc_ordered_qty_by_order_no(self, order_no: str) -> int:
        msgs = self.select_by_order_no(order_no)
        msgs = self.select_by_cls(NewOrderMessage, msgs)
        succ_order_msgs = self.select_by_response_code(OrderReceivedMessage.SUCCESS, msgs)
        return self._sum_qty(succ_order_msgs)

    def calc_cancelled_qty_by_order_no(self, order_no: str) -> int:
        msgs = self.select_by_order_no(order_no)
        msgs = self.select_by_cls(CancelOrderMessage, msgs)
        succ_cancel_msgs = self.select_by_response_code(OrderReceivedMessage.SUCCESS, msgs)
        return self._sum_qty(succ_cancel_msgs)

    def calc_executed_qty_by_order_no(self, order_no: str) -> int:
        msgs = self.select_by_order_no(order_no)
        msgs = self.select_by_cls(OrderExecutedMessage, msgs)
        return self._sum_qty(msgs)

    def calc_unexecuted_qty_by_order_no(self, order_no: str) -> int:
        order_qty = self.calc_ordered_qty_by_order_no(order_no)
        ex_qty = self.calc_executed_qty_by_order_no(order_no)  # int
        cancel_qty = self.calc_cancelled_qty_by_order_no(order_no)
        return order_qty - ex_qty - cancel_qty


class AXETaskQuerent(MessageQuerent):

    """ 
    ================================
    ========== [구현 요소] =========
    ================================
    """

    def get_unex_qty_by_ticker(self, ticker: str):
        """ 1. 종목코드를 입력으로 해당 종목의 전체 미체결 수량을 반환하는 함수 """

        msgs = self.select_by_ticker(ticker)  # default: ticker matching
        unex_orders = self.select_unexecuted_orders(msgs)
        return self._sum_qty(unex_orders)

    def get_unex_qty_by_ticker_and_price(self, ticker: str, price: str or int):
        """ 2. 종목코드와 가격을 입력으로 전체 미체결 주문 목록을 반환하는 함수 """

        if isinstance(price, int):
            price = str(price).zfill(5)

        msgs = self.select_by_ticker(ticker)
        msgs = self.select_by_price(price, msgs)  # default

        unex_orders = self.select_unexecuted_orders(msgs)
        return self._sum_qty(unex_orders)

    def get_unex_orders_by_ticker(self, ticker: str):
        """ 3. 종목코드를 입력으로 전체 미체결 주문 목록을 반환하는 함수 """

        msgs = self.select_by_ticker(ticker)
        return self.select_unexecuted_orders(msgs)

    def get_unex_orders_by_ticker_and_price(self, ticker: str, price: str or int):
        """ 4. 종목코드와 가격을 입력으로 특정 종목, 특정 가격의 미체결 주문 목록을 반환하는 함수"""

        if isinstance(price, int):
            price = str(price).zfill(5)

        msgs = self.select_by_ticker(ticker)
        msgs = self.select_by_price(price, msgs)
        return self.select_unexecuted_orders(msgs)

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

        msgs = self.select_by_cls(NewOrderMessage)
        msgs = self.select_by_ticker(ticker, msgs)
        return self.select_by_order_no(order_no, msgs)


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
