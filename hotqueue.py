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
    name = name.replace('hotqueue:','')
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
    
    def __init__(self, name='adminaccess', serializer=pickle, redis_connection=None, **kwargs):
        self.group_name = kwargs.pop("group", "hotqueue")
        self.name = name
        self.serializer = serializer
        if redis_connection:
            self.__redis = redis_connection
        else:
            self.__redis = Redis(**kwargs)
    
    def __len__(self):
        return self.__redis.llen(self.key)

    def _get_hotqueues(self, wildcard="*"):
        return self.__redis.keys(key_for_name(wildcard))

    def _get_unacked_hotqueue(self,wildcard="*"):
        queues = self._get_hotqueues("unacked:"+wildcard)
        if queues:
            return queues[0].replace('hotqueue:','')
        else:
            return None
    
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
        processId = str(uuid4())
        process_queue = key_for_name("processing:"+processId+":"+str(time.time()+5))
        if block:
            if timeout is None:
                timeout = 0
            msg = self.__redis.brpop(self.key, timeout=timeout)
            if msg is not None:
                msg = msg[1]
        else:
            msg = self.__redis.rpop(self.key)
        self.__redis.lpush(process_queue, msg) # try *rpoplpush instead of this
        hq_message = HQMessage()
        msg_tmp = msg
        if msg is not None and self.serializer is not None:
            msg_tmp = self.serializer.loads(msg)
        hq_message = msg_tmp
        if msg:
            hq_message.reserve_message()
            self.__redis.rpush(key_for_name("unacked:"+hq_message.get_reservationId()+":"+str(hq_message.get_expiration())), msg)
        self.__redis.delete(process_queue)
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
            hq_message = HQMessage(msg,self.name)
            if self.serializer is not None:
                hq_message_list.append(self.serializer.dumps(hq_message))
            else:
                hq_message_list.append(hq_message)
            hq_message_list_raw.append(hq_message)

        # if self.serializer is not None:
        #     hq_message_list = map(self.serializer.dumps, *hq_message_list)
        self.__redis.lpush(self.key, *hq_message_list)
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
        self.__redis.rpush(self.key, *msgs)

    def _acknack(self, reservation_uuid, ack=False, nack=False):
        unackedqueue_name = self._get_unacked_hotqueue(str(reservation_uuid)+':*')
        if unackedqueue_name:
            msg = self.__redis.rpop(key_for_name(unackedqueue_name))
            hq_message = HQMessage()
            msg_tmp = msg
            if msg is not None and self.serializer is not None:
                msg_tmp = self.serializer.loads(msg)
            hq_message = msg_tmp

            if nack:
                print "passou aqui 1"
                self.__redis.rpush(key_for_name(hq_message.get_queueName()), msg)
                print "passou aqui 2"
            self.__redis.delete(key_for_name(unackedqueue_name))
            return hq_message
        else:
            return None

    def ack(self, reservation_uuid):
        print "ack called"
        return self._acknack(reservation_uuid, ack=True, nack=False)

    def nack(self, reservation_uuid):
        print "nack called"
        return self._acknack(reservation_uuid, ack=False, nack=True)

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

    _default_expiration = 60
    messageId = ''
    createdAt = 0
    body = ''
    reservationId = ''
    expiration = 0
    deliveryCount = 0
    queueName = ''
    senderId = ''
    originIPAddress = None
    senderName = None

    def __init__(self, body=None, originalQueue=None, senderName=None, originIPAddress=None):
        self.body = body
        self.createdAt = time.time()
        self.messageId = str(uuid4())
        self.reservationId = None
        self.expiration = 0
        self.deliveryCount = 0
        self.queueName = originalQueue
        self.senderId = str(uuid4())
        self.originIPAddress = originIPAddress
        self.senderName = senderName

    # def __iter__(self):
    #     for each in self.__dict__.keys():
    #         print each
    #         yield self.__getattribute__(each)


    def __eq__(self,othermessage):
        try:
            check_msgId = self.get_messageId() == othermessage.get_messageId()
            check_body = self.get_body() == othermessage.get_body()
            check_createdAt = self.get_createdAt() == othermessage.get_createdAt()
            return check_msgId and check_body and check_createdAt
        except:
            return False

    def get_body(self):
        return self.body

    def set_body(self,body):
        self.body = body

    def get_queueName(self):
        return self.queueName

    def get_id(self):
        return self.messageId
    def get_messageId(self):
        return self.messageId

    def set_expiration(self):
        self.expiration = time.time() + self._default_expiration
        return True

    def get_expiration(self):
        return self.expiration

    def set_reservationId(self):
        self.reservationId = str(uuid4())
        return True

    def get_reservationId(self):
        return self.reservationId

    def get_senderId(self):
        return self.senderId

    def get_createdAt(self):
        return self.createdAt

    def get_deliveryCount(self):
        return self.deliveryCount

    def inc_deliveryCount(self):
        self.deliveryCount = self.deliveryCount + 1
        return True

    def reserve_message(self):
        self.set_reservationId()        
        self.inc_deliveryCount()
        self.set_expiration()

    def is_old(self):
        if (time.time() - self.createdAt) >= self._default_expiration: #seconds
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
        result['body']=self.body
        result['createdAt']=int(self.createdAt*1000) #millis
        result['deliveryCount']=self.deliveryCount
        result['expiration']=int(self.expiration*1000) #millis
        result['messageId']=self.messageId
        result['queueName']=self.queueName
        result['reservationId']=self.reservationId
        return json.dumps(result, ensure_ascii=False)
