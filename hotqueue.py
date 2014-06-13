# -*- coding: utf-8 -*-

"""HotQueue is a Python library that allows you to use Redis as a message queue
within your Python programs.
"""

from functools import wraps
try:
    import cPickle as pickle
except ImportError:
    import pickle

from redis import Redis


__all__ = ['HotQueue']

__version__ = '0.2.7'


def key_for_name(name):
    """Return the key name used to store the given queue name in Redis."""
    return 'hotqueue:%s' % name


class HotQueue(object):
    
    """Simple FIFO message queue stored in a Redis list. Example:
    
    >>> from hotqueue import HotQueue
    >>> queue = HotQueue("myqueue", host="localhost", port=6379, db=0)
    
    :param name: name of the queue
    :param serializer: the class or module to serialize msgs with, must have
        methods or functions named ``dumps`` and ``loads``,
        `pickle <http://docs.python.org/library/pickle.html>`_ is the default,
        use ``None`` to store messages in plain text (suitable for strings,
        integers, etc)
    :param kwargs: additional kwargs to pass to :class:`Redis`, most commonly
        :attr:`host`, :attr:`port`, :attr:`db`
    """
    
    def __init__(self, name, serializer=pickle, **kwargs):
        self.name = name
        self.serializer = serializer
        self.__redis = Redis(**kwargs)
    
    def __len__(self):
        return self.__redis.llen(self.key)
    
    @property
    def key(self):
        """Return the key name used to store this queue in Redis."""
        return key_for_name(self.name)
    
    def clear(self):
        """Clear the queue of all messages, deleting the Redis key."""
        self.__redis.delete(self.key)
    
    def consume(self, **kwargs):
        """Return a generator that yields whenever a message is waiting in the
        queue. Will block otherwise. Example:
        
        >>> for msg in queue.consume(timeout=1):
        ...     print msg
        my message
        another message
        
        :param kwargs: any arguments that :meth:`~hotqueue.HotQueue.get` can
            accept (:attr:`block` will default to ``True`` if not given)
        """
        kwargs.setdefault('block', True)
        try:
            while True:
                msg = self.get(**kwargs)
                if msg is None:
                    break
                yield msg
        except KeyboardInterrupt:
            print; return
    
    def get(self, block=False, timeout=None):
        """Return a message from the queue. Example:
    
        >>> queue.get()
        'my message'
        >>> queue.get()
        'another message'
        
        :param block: whether or not to wait until a msg is available in
            the queue before returning; ``False`` by default
        :param timeout: when using :attr:`block`, if no msg is available
            for :attr:`timeout` in seconds, give up and return ``None``
        """
        if block:
            if timeout is None:
                timeout = 0
            msg = self.__redis.blpop(self.key, timeout=timeout)
            if msg is not None:
                msg = msg[1]
        else:
            msg = self.__redis.lpop(self.key)
        if msg is not None and self.serializer is not None:
            msg = self.serializer.loads(msg)
        return msg
    
    def put(self, *msgs):
        """Put one or more messages onto the queue. Example:
        
        >>> queue.put("my message")
        >>> queue.put("another message")
        
        To put messages onto the queue in bulk, which can be significantly
        faster if you have a large number of messages:
        
        >>> queue.put("my message", "another message", "third message")
        """
        if self.serializer is not None:
            msgs = map(self.serializer.dumps, msgs)
        self.__redis.rpush(self.key, *msgs)
    
    def worker(self, *args, **kwargs):
        """Decorator for using a function as a queue worker. Example:
        
        >>> @queue.worker(timeout=1)
        ... def printer(msg):
        ...     print msg
        >>> printer()
        my message
        another message
        
        You can also use it without passing any keyword arguments:
        
        >>> @queue.worker
        ... def printer(msg):
        ...     print msg
        >>> printer()
        my message
        another message
        
        :param kwargs: any arguments that :meth:`~hotqueue.HotQueue.get` can
            accept (:attr:`block` will default to ``True`` if not given)
        """
        def decorator(worker):
            @wraps(worker)
            def wrapper(*args):
                for msg in self.consume(**kwargs):
                    worker(*args + (msg,))
            return wrapper
        if args:
            return decorator(*args)
        return decorator


class HotBuffer(object):
    
    """In-memory buffer. Add items to the buffer indefinitely. Whenever it
    reaches the defined limit, the contents will be passed to the callback as
    arguments. Here's an example:
    
    >>> def print_items(*args):
    ...     print args
    >>> buffer = HotBuffer(3, print_items)
    >>> buffer.add("Mary")
    >>> buffer.add("had")
    >>> buffer.add("a")
    ('Mary', 'had', 'a')
    >>> buffer.add("little")
    >>> buffer.add("lamb,")
    >>> buffer.add("whose")
    ('little', 'lamb,', 'whose')
    >>> buffer.add("fleece")
    >>> buffer.add("was")
    >>> buffer.add("white")
    ('fleece', 'was', 'white')
    >>> buffer.add("as")
    >>> buffer.add("snow.")
    >>> buffer.flush()
    ('as', 'snow.')
    
    Here's an example of buffering items in-memory before adding them to a
    HotQueue queue, which can significantly improve performance on high volume
    HotQueues where delays are acceptable:
    
    >>> from hotqueue import HotBuffer
    >>> queue = HotQueue("myqueue")
    >>> buffer = HotBuffer(500, queue.put)
    >>> buffer.add("my message")
    
    The ``"my message"`` message would eventually be put onto the HotQueue
    queue in bulk with 499 other items.
    
    :param limit: the maximum number of items to buffer
    :param callback: the function to call once the buffer is full
    """
    
    def __init__(self, limit, callback):
        self.limit = limit
        self.callback = callback
        self.__items = []
    
    def __len__(self):
        return len(self.__items)
    
    def add(self, *args):
        """Add one or more items to the buffer. Example:
        
        >>> buffer.add("my message")
        >>> buffer.add("another message")
        
        To put items onto the buffer in bulk:
        
        >>> buffer.add("my message", "another message", "third message")
        
        It is possible to add more items than the defined limit when adding in
        bulk. If your buffer has a limit of 500 items, currently contains
        499 items, and you add 5 items in bulk, 504 items will be passed to the
        callback at once.
        """
        self.__items.extend(args)
        if len(self) >= self.limit:
            self.flush()
    
    def clear(self):
        """Clear the buffer of all items, discarding them. Example:
        
        >>> buffer.clear()
        """
        self.__items = []
    
    def flush(self):
        """Flush all items on the buffer to the callback. This method is used
        internally when the buffer reaches it's limit, but it can be called in
        any other instance when you want the buffer to flush. Example:
        
        >>> buffer.flush()
        """
        self.callback(*self.__items)
        self.clear()

