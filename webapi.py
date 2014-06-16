from flask import Flask, request, make_response, abort
from uuid import uuid4, UUID
from hotqueue import HotQueue
from flask import jsonify

from functools import wraps
import time

app = Flask(__name__)
app.secret_key = 'nhuta'

class Message(object):

    _timestamp = None
    _payload = None
    _originalqueue = None
    _id = None
    _sender = None
    _retries = 1
    _reservation_id = None
    _expiration = 0

    def __init__(self, payload=None, sender=None, originalqueue=None):
        if payload is not None:
            self._timestamp = time.time()
            self._id = str(uuid4())
            self._payload = payload
            self._sender = sender
            self._retries = 1
            self._originalqueue = originalqueue

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

    def get_sender(self):
        return self._sender

    def get_timestamp(self):
        return self._timestamp

    def get_retries(self):
        return self._retries

    def inc_retries(self):
        self._retries = self._retries + 1
        return True

    def is_old(self):
        if (time.time() - self._timestamp) >= 60: #seconds
            return True
        return False

    def json(self):
        # {
        #     "body": "<body of the message, as specified in the insert call>",
        #     "createdAt": <timestamp of when this message was inserted,
        #     "deliveryCount": <number of redeliveries of this message>,
        #     "expiration": <timestamp of when the current reservation expires>,
        #     "messageId": "<id of the message, same as returned by the insert call>",
        #     "queueName": "<name of the queue, same as specified in the reserve call>",
        #     "reservationId": "<id of this reservation, can be used in the return message call>"
        # }
        return jsonify(body=self._payload,createdAt=int(self._timestamp*1000),deliveryCount=self._retries,expiration=int(self._expiration*1000),
                       messageId=self._id, queueName=self._originalqueue, reservationId=self._reservation_id)


def check_uuid(fn):
    @wraps(fn)
    def decorated_view(*args, **kwargs):
        message_uuid = kwargs['reservation_uuid']
        try:
            UUID(message_uuid, version=4)
        except:
            abort(400)
        return fn(*args, **kwargs)
    return decorated_view


def is_unacked_old(queue):
    timestamp = queue.split(':')[-1]
    if (time.time() - float(timestamp)) >= 60: #seconds
        return True
    return False


@app.route("/queues/<queuename>", methods=['POST'])
@app.route("/queues/<queuename>/messages/<message>", methods=['PUT'])
def put(queuename,message=None):
    returncode = 200
    message_envelope = None
    if not message:
        message = str(request.form['body'])
    if message:
        consumer_id = str(uuid4())
        queue = HotQueue(queuename, host="localhost", port=6379, db=0)
        message_envelope = Message(message,consumer_id,queuename)
        queue.put(message_envelope)
    else:
        returncode = 400
    return message_envelope.json(), returncode

# @app.route("/queues", methods=['GET']) lista as queues
# @app.route("/queue/<queuename>", methods=['GET']) detalhes da queue
# @app.route("/queue/<queuename>", methods=['DELETE']) mata a queue


@app.route("/queues/<queuename>", methods=['GET'])
@app.route("/queues/<queuename>/messages", methods=['GET'])
def get(queuename):
    returncode = 200
    message_envelope = None
    reservation_id = str(uuid4())
    queue = HotQueue(queuename, host="localhost", port=6379, db=0)
    message_envelope = Message()
    message_envelope = queue.get()
    if message_envelope:
        timestamp = time.time()
        unacked_message = HotQueue("unacked:"+reservation_id+":"+str(timestamp), host="localhost", port=6379, db=0)
        unacked_message.put(message_envelope)
        message_envelope.set_reservation_id(reservation_id)
        message_envelope.set_expiration()
    else:
        returncode = 204
    return message_envelope.json(), returncode


@app.route("/messages/ack/<reservation_uuid>", methods=['DELETE'])
@app.route("/messages/nack/<reservation_uuid>", methods=['DELETE'])
@app.route("/messages/<reservation_uuid>", methods=['DELETE']) # ack
@app.route("/messages/<reservation_uuid>", methods=['PUT']) # nack
@check_uuid
def acknack(reservation_uuid):
    returncode = 200
    message_envelope = None
    access = HotQueue("access", host="localhost", port=6379, db=0)
    if access.get_unackedqueues(str(reservation_uuid)+':*'):
        unackedqueue_name = access.get_unackedqueues(str(reservation_uuid)+':*')[0].replace('hotqueue:','')
        unackedqueue = HotQueue(unackedqueue_name, host="localhost", port=6379, db=0)
        message_envelope = Message()
        message_envelope = unackedqueue.get()
        if message_envelope:
            nack = False
            if 'nack' in request.path:
                nack = True
            if 'PUT' in request.method:
                nack = True
            if nack:
                originalqueue = HotQueue(message_envelope.get_originalqueue(), host="localhost", port=6379, db=0)
                message_envelope.inc_retries()
                originalqueue.put_again(message_envelope)
        else:
            returncode = 204
        unackedqueue.clear()
    else:
        returncode = 400
    return message_envelope.json(), returncode


@app.route("/fixunacked")
def fixunacked():
    returncode = 200
    queue = HotQueue("access", host="localhost", port=6379, db=0)
    outputdata = ""
    for q in queue.get_unackedqueues():
        if is_unacked_old(q):
            unackedqueue = HotQueue(q.replace('hotqueue:',''), host="localhost", port=6379, db=0)
            message_envelope = Message()
            message_envelope = unackedqueue.get()
            originalqueue = HotQueue(message_envelope.get_originalqueue(), host="localhost", port=6379, db=0)
            originalqueue.put_again(message_envelope)
            unackedqueue.clear()
        outputdata = outputdata+str(q)+':'+str(is_unacked_old(q))+"\n"
    if outputdata == "":
        returncode = 204
    return outputdata, returncode


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5300, debug=True)
