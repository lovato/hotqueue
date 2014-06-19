# -*- coding: utf-8 -*-
"""HotQueue is a Python library that allows you to use Redis as a message queue
within your Python programs.
"""

from __future__ import unicode_literals
from functools import wraps
# try:
#     import cPickle as pickle
# except ImportError:
#     import pickle
import dill as pickle

from redis import Redis
from uuid import uuid4, UUID
import time
import simplejson as json
from array import *

__all__ = ['HotQueue']

__version__ = '0.2.8.dev.lovato'


def key_for_name(name):
    """Return the key name used to store the given queue name in Redis."""
    return 'hotqueue:%s' % name

class HotQueueAdmin(object):

    def __init__(self, redis_connection=None, **kwargs):
        if redis_connection:
            self.__redis = redis_connection
        else:
            self.__redis = Redis(**kwargs)

    def get_hotqueues(self):
        return self.__redis.keys(key_for_name("*"))

    def get_unacked_hotqueues(self,wildcard="*"):
        return self.__redis.keys(key_for_name("unacked:"+wildcard))


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
    
    def __init__(self, name, serializer=pickle, redis_connection=None, **kwargs):
        self.group_name = kwargs.pop("group", "hotqueue")
        self.name = name
        self.serializer = serializer
        if redis_connection:
            self.__redis = redis_connection
        else:
            self.__redis = Redis(**kwargs)
    
    def __len__(self):
        return self.__redis.llen(self.key)
    
    @property
    def key(self):
        """Return the key name used to store this queue in Redis."""
        return "%s:%s" % (self.group_name, self.name)
    
    def clear(self):
        """Clear the queue of all messages, deleting the Redis key."""
        self.__redis.delete(self.key)

    def clear_value(self, value):
        """Removes any occurence of an item from queue"""
        self.__redis.lrem(self.key, 0, value)

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
        hq_message = HQMessage()
        if msg is not None and self.serializer is not None:
            msg = self.serializer.loads(msg)
        hq_message = msg
        return hq_message

    def put(self, *msgs):
        hq_message_list = []
        hq_message_list_raw = []
        """Put one or more messages onto the queue. Example:
        
        >>> queue.put("my message")
        >>> queue.put("another message")
        
        To put messages onto the queue in bulk, which can be significantly
        faster if you have a large number of messages:
        
        >>> queue.put("my message", "another message", "third message")
        """
        for msg in msgs:
            hq_message = HQMessage(msg,str(uuid4()),self.name)
            if self.serializer is not None:
                hq_message_list.append(self.serializer.dumps(hq_message))
            else:
                hq_message_list.append(hq_message)
            hq_message_list_raw.append(hq_message)

        # if self.serializer is not None:
        #     hq_message_list = map(self.serializer.dumps, *hq_message_list)
        self.__redis.rpush(self.key, *hq_message_list)
        return hq_message_list_raw
    
    def put_again(self, *msgs):
        """Put one or more messages onto the queue. Used to requeue an element if it fails. Example:

        >>> queue.put_again("my message")
        >>> queue.put_again("another message")

        To put messages onto the queue in bulk, which can be significantly
        faster if you have a large number of messages:

        >>> queue.put_again("my message", "another message", "third message")
        """
        # if self.serializer is not None:
        #     msgs = map(self.serializer.dumps, msgs)
        self.__redis.lpush(self.key, *msgs)

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


class HQMessage(object):

    def __init__(self, payload=None, sender=None, originalqueue=None, serializer=pickle):
        self.serializer = serializer
        self._timestamp = time.time()
        self._id = str(uuid4())
        self.set_payload(payload)
        self._sender = sender
        self._delivery_count = 1 #starts with the first delivery that will happen
        self._reservation_id = None
        self._expiration = 0
        self._originalqueue = originalqueue
        self._origin_ipaddr = None

    def __eq__(self,othermessage):
        try:
            a = self.to_json()
            b = othermessage.to_json()
            return a == b
        except:
            return False

    def get_payload(self):
        return self._payload

    def get_originalqueue(self):
        return self._originalqueue

    def get_id(self):
        return self._id

    def set_expiration(self):
        self._expiration = float(self._timestamp) + 60
        return True

    def get_expiration(self):
        return self._expiration

    def set_reservation_id(self, id):
        self._reservation_id = id
        return True

    def get_payload(self):
        # return self.serializer.loads(self._payload)
        return self._payload

    def set_payload(self,payload):
        # self._payload = self.serializer.dumps(payload)
        self._payload = payload

    def get_sender(self):
        return self._sender

    def get_timestamp(self):
        return self._timestamp

    def get_delivery_count(self):
        return self._delivery_count

    def get_delivery_count(self):
        return self._delivery_count

    def inc_delivery_count(self):
        self._delivery_count = self._delivery_count + 1
        return True

    def is_old(self):
        if (time.time() - self._timestamp) >= 60: #seconds
            return True
        return False

    def to_json(self):
        """
        "body": "<body of the message, as specified in the insert call>",
        "createdAt": <timestamp of when this message was inserted,
        "deliveryCount": <number of redeliveries of this message>,
        "expiration": <timestamp of when the current reservation expires>,
        "messageId": "<id of the message, same as returned by the insert call>",
        "queueName": "<name of the queue, same as specified in the reserve call>",
        "reservationId": "<id of this reservation, can be used in the return message call>"
        """
        result = {}
        result['body']=self._payload
        result['createdAt']=int(self._timestamp*1000) #millis
        result['deliveryCount']=self._delivery_count
        result['expiration']=int(self._expiration*1000) #millis
        result['messageId']=self._id
        result['queueName']=self._originalqueue
        result['reservationId']=self._reservation_id
        return json.dumps(result, ensure_ascii=False)
