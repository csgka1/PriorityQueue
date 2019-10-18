import heapq
from typing import Dict, Any, Union

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
    >>> pq.pop()
    10
    >>> pq.pop()
    20
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
    """

    def __init__(self):
        self.heap = []
        self.length = 0
        self.priority_lentghs: Dict[_Number, int] = {}  # 每个优先级的队列长度
        self.priority_order: Dict[_Number, int] = {}  # 每个优先级的发号器存储

    def get_order(self, priority: int) -> int:
        """
        发号器，为每个优先级的队列提供order。
        现在单纯使用自增来实现。
        考虑基于时间戳来实现。
        """
        if priority not in self.priority_order:
            self.priority_order[priority] = 1
        else:
            self.priority_order[priority] += 1
        return self.priority_order[priority]

    def is_empty(self) -> bool:
        return self.length == 0

    def push(self, obj, priority: _Number = Priorities.NORMAL) -> None:
        element = _Element(priority=priority, order=self.get_order(priority), payload=obj)
        heapq.heappush(self.heap, element)
        self.length += 1

    def pop(self) -> Any:
        if self.is_empty():
            raise QueueEmptyException
        self.length -= 1
        return heapq.heappop(self.heap).payload


class _Element:
    """
    插入到优先队列中的元素。优先级越高，越早被取出。若优先级相同，放入的顺序越靠前，越先被取出。
    在进行比较时，越优先的元素越小。

    >>> a = _Element(priority=100, order=1, payload=None)
    >>> b = _Element(priority=60.5, order=1, payload=None)
    >>> c = _Element(priority=60.5, order=3, payload=None)
    >>> a_same = _Element(priority=100, order=1, payload=None)
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

    def __init__(self, priority: _Number, order: int, payload: Any):
        self.priority: _Number = priority
        self.order: int = order
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
