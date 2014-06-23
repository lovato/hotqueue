from flask import Flask, request, make_response, abort
from hotqueue import HotQueue, HQMessage

from functools import wraps
from flask import jsonify
import simplejson as json

from uuid import uuid4, UUID
import time

app = Flask(__name__)
app.secret_key = 'hotqueue_secret'

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
    if not message:
        message = str(request.form['body'])
    if message:
        queue = HotQueue(queuename, host="localhost", port=6379, db=0)
        put_status = HQMessage()
        put_status = queue.put(message)[0]
    else:
        returncode = 400
    return put_status.to_json(), returncode

# @app.route("/queues", methods=['GET']) lista as queues
# @app.route("/queue/<queuename>", methods=['GET']) detalhes da queue
# @app.route("/queue/<queuename>", methods=['DELETE']) mata a queue


@app.route("/queues/<queuename>", methods=['GET'])
@app.route("/queues/<queuename>/messages", methods=['GET'])
def get(queuename):
    returncode = 200
    return_hqmessage = None
    reservation_id = str(uuid4())
    queue = HotQueue(queuename, host="localhost", port=6379, db=0)
    hqmessage = HQMessage()
    hqmessage = queue.get()
    if hqmessage:
        return_hqmessage = hqmessage.to_json()
    else:
        returncode = 204
        return_hqmessage = ''
    return return_hqmessage, returncode


@app.route("/messages/ack/<reservation_uuid>", methods=['DELETE'])
@app.route("/messages/nack/<reservation_uuid>", methods=['DELETE'])
@app.route("/messages/<reservation_uuid>", methods=['DELETE']) # ack
@app.route("/messages/<reservation_uuid>", methods=['PUT']) # nack
@check_uuid
def acknack(reservation_uuid):
    returncode = 200
    returnmsg = ''
    hq = HotQueue(host="localhost", port=6379, db=0)
    msg = None

    nack = False
    if 'nack' in request.path:
        nack = True
    if 'PUT' in request.method:
        nack = True

    print nack

    if nack:
        msg = hq.nack(reservation_uuid)
    else:
        msg = hq.ack(reservation_uuid)

    if msg:
        hqmessage = HQMessage()
        hqmessage = msg
        returnmsg = hqmessage.to_json()
    else:
        returncode = 400
    return returnmsg, returncode


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
