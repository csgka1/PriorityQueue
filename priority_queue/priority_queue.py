import heapq
from typing import Dict, Any, Union
import time
import threading


_Number = Union[int, float]

__all__ = ['PriorityQueue', 'Priorities']


class Priorities:
    LOW = 0
    NORMAL = 1
    HIGH = 2


class QueueEmptyException(Exception):
    pass


class PriorityQueue:
    """
    >>> pq = PriorityQueue()
    >>> try:
    ...     pq.pop()
    ... except QueueEmptyException as e:
    ...     print("QueueEmptyException")
    QueueEmptyException
    >>> pq.push(10, Priorities.HIGH)
    >>> pq.push(20)
    >>> pq.push(30)
    >>> pq.get_length()
    3
    >>> pq.get_length(Priorities.LOW)
    0
    >>> pq.get_length(Priorities.NORMAL)
    2
    >>> pq.pop()
    10
    >>> pq.pop()
    20
    >>> pq.get_length()
    1
    >>> pq.get_length(Priorities.NORMAL)
    1
    >>> pq.get_length(Priorities.HIGH)
    0
    >>> pq.push(40, Priorities.LOW)
    >>> pq.push(50, Priorities.HIGH)
    >>> pq.push(60, Priorities.NORMAL)
    >>> while not pq.is_empty():
    ...     pq.pop()
    50
    30
    60
    40
    >>> try:
    ...     pq.pop()
    ... except QueueEmptyException as e:
    ...     print("QueueEmptyException")
    QueueEmptyException
    >>> pq.get_length()
    0
    """

    def __init__(self):
        self.heap = []
        self.length = 0
        self.priority_lentghs: Dict[_Number, int] = {}  # 每个优先级的队列长度
        self.priority_timestamp_order: Dict[_Number, _TimestampOrder] = {}
        self._mutex = threading.Lock()

    def __get_order(self, priority: int) -> '_TimestampOrder':
        """
        发号器，为每个优先级的队列提供order。
        基于时间戳实现。
        """
        if priority not in self.priority_timestamp_order:
            self.priority_timestamp_order[priority] = _TimestampOrder(int(time.time()), 0)

        current_timestamp = int(time.time())
        if current_timestamp > self.priority_timestamp_order[priority].timestamp:
            self.priority_timestamp_order[priority].timestamp = current_timestamp

        else:
            self.priority_timestamp_order[priority].number += 1
        return self.priority_timestamp_order[priority]

    def is_empty(self) -> bool:
        return self.length == 0

    def push(self, obj, priority: _Number = Priorities.NORMAL) -> None:
        with self._mutex:
            element = _Element(priority=priority, order=self.__get_order(priority), payload=obj)
            heapq.heappush(self.heap, element)

            self.length += 1
            if priority not in self.priority_lentghs.keys():
                self.priority_lentghs[priority] = 1
            else:
                self.priority_lentghs[priority] += 1

    def pop(self) -> Any:
        with self._mutex:
            if self.is_empty():
                raise QueueEmptyException
            self.length -= 1
            popped: _Element = heapq.heappop(self.heap)
            self.priority_lentghs[popped.priority] -= 1
            return popped.payload

    def get_length(self, priority=None) -> int:
        """
        获取指定优先级的队列长度。如果没有指定优先级，则返回整个队列的长度。
        :param priority: 优先级，默认为 None
        :return: 指定优先级的队列长度。若指定的优先级从未出现过，返回 0
        """
        if priority is None:
            return self.length
        else:
            if priority not in self.priority_lentghs.keys():
                return 0
            else:
                return self.priority_lentghs[priority]


class _TimestampOrder:
    """基于时间戳的编号。
    进行比较时，时间较早的一个较小。若时间相同，number更小的一个较小。
    >>> a = _TimestampOrder(1, 1)
    >>> b = _TimestampOrder(1, 2)
    >>> c = _TimestampOrder(2, 1)
    >>> d = _TimestampOrder(1, 2)
    >>> a < b
    True
    >>> a > b
    False
    >>> a == b
    False
    >>> b > a
    True
    >>> b < a
    False
    >>> b == a
    False
    >>> a > c
    False
    >>> a < c
    True
    >>> a == c
    False
    >>> d == b
    True
    >>> d > b
    False
    >>> d < b
    False
    """
    def __init__(self, timestamp: int, number: int):
        self.timestamp: int = timestamp
        self.number: int = number

    def __lt__(self, other: '_TimestampOrder'):
        if self.timestamp < other.timestamp:
            return True
        if self.timestamp == other.timestamp and self.number < other.number:
            return True
        return False

    def __eq__(self, other: '_TimestampOrder'):
        if self.timestamp == other.timestamp and self.number == other.number:
            return True
        else:
            return False

    def __gt__(self, other: '_TimestampOrder'):
        return not (self.__lt__(other) or self.__eq__(other))


class _Element:
    """
    插入到优先队列中的元素。优先级越高，越早被取出。若优先级相同，放入的顺序越靠前，越先被取出。
    在进行比较时，越优先的元素越小。

    >>> a = _Element(priority=100, order=_TimestampOrder(0, 1), payload=None)
    >>> b = _Element(priority=60.5, order=_TimestampOrder(0, 1), payload=None)
    >>> c = _Element(priority=60.5, order=_TimestampOrder(0, 3), payload=None)
    >>> a_same = _Element(priority=100, order=_TimestampOrder(0, 1), payload=None)
    >>> a < b
    True
    >>> a > b
    False
    >>> a == b
    False
    >>> b < a
    False
    >>> b > a
    True
    >>> b == a
    False
    >>> b < c
    True
    >>> b > c
    False
    >>> b == c
    False
    >>> c < b
    False
    >>> c > b
    True
    >>> c < a
    False
    >>> c > a
    True
    >>> c == b
    False
    >>> a == a_same
    True
    >>> a < a_same
    False
    >>> a > a_same
    False
    """

    def __init__(self, priority: _Number, order: _TimestampOrder, payload: Any):
        self.priority: _Number = priority
        self.order: _TimestampOrder = order
        self.payload: Any = payload

    # 由于使用了最小堆，所以更优先的对象需要更“小”
    def __lt__(self, other):
        if self.priority > other.priority:
            return True
        elif self.priority == other.priority:
            return self.order < other.order
        else:
            return False

    def __gt__(self, other):
        return not (self.__lt__(other) or self.__eq__(other))

    def __eq__(self, other):
        return self.priority == other.priority and self.order == other.order


if __name__ == "__main__":
    import doctest
    print(doctest.testmod())
