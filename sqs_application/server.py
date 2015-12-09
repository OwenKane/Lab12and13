import os
from flask import Flask, Response, request, redirect, url_for
from werkzeug import secure_filename
import urllib2
import boto
import boto.sqs
import boto.sqs.queue
from boto.sqs.message import Message
from boto.sqs.connection import SQSConnection
from boto.exception import SQSError
import sys
import json
from tempfile import mkdtemp
from subprocess import Popen, PIPE

app = Flask(__name__)

@app.route("/")
def index():
    return 

@app.route("/version", methods=['GET'])
def version():
	"""
	Print Boto version
	curl -s -X GET localhost:8080/version
	"""
	print("Boto version: "+boto.Version+ "\n")
	return "Boto version: "+boto.Version+ "\n"

@app.route("/queues", methods=['GET'])
def queues_index():
	"""
	List all queues
	curl -s -X GET -H 'Accept: application /json' http://localhost:8080/queues | python -mjson.tool
	"""
	all = []
	conn = get_conn()
	for q in conn.get_all_queues():
		all.append(q.name)
	resp = json.dumps(all)
	return Response(response=resp, mimetype="application/json") 

@app.route("/queues", methods=['POST'])
def queues_create():
	"""
	Create a queue
	curl -X POST -H 'Content-Type: application/json' http://localhost:8080/queues -d '{"name": "OwenKTest"}'	
	"""

	conn = get_conn()
	body = request.get_json(force=True)
	name = body['name']
	queue = conn.create_queue(name, 120)
	resp = "Queue, "+name+", created\n"
	return Response(response=resp, mimetype="application/json")

@app.route("/queues/<name>", methods=['DELETE'])
def queues_remove(name):
	"""
	Delete a queue
	curl -X DELETE -H 'Accept: application/json' http://localhost:8080/queues/OwenKTest
	"""

	conn = get_conn()
	queue = conn.get_queue(name)
	conn.delete_queue(queue)
	
	resp = "Queue ,"+name+" , deleted\n"
	return Response(response=resp, mimetype="application/json")

@app.route("/queues/<name>/msgs/count", methods=['GET'])
def messages_count(name):
	"""
	Get message count for a queue
	curl -X GET -H 'Accept: application/json' http://localhost:8080/queues/OwenKTest/msgs/count
	"""

	conn = get_conn()
	queue = conn.get_queue(name)
	count = queue.count()
	
	resp = "Queue "+name+" has "+str(count)+" messages\n"
	return Response(response=resp, mimetype="application/json")	

@app.route("/queues/<name>/msgs", methods=['POST'])
def messages_write(name):
	"""
	Write message to a queue
	curl -s -X POST -H 'Accept: application/json' http://localhost:8080/queues/OwenKTest/msgs -d '{"content": "Test"}' 
	"""

	body = request.get_json(force=True)
	messageText = body['content']
	
	conn = get_conn()
	queue = conn.get_queue(name)
	queue.set_message_class(Message)

	m = Message()
	m.set_body(messageText)
	queue.write(m)
	
	resp = "Message "+messageText+" has been written to queue "+name+"\n"
	return Response(response=resp, mimetype="application/json")	

@app.route("/queues/<name>/msgs", methods=['GET'])
def messages_read(name):
	"""
	Get message from a queue
	curl -X GET -H 'Accept: application/json' http://localhost:8080/queues/OwenKTest/msgs
	"""

	conn = get_conn()
	queue = conn.get_queue(name)
	messages = queue.get_messages()
	if len(messages) > 0:
		message = messages[0]
		resp = "Queue: "+name+". \nMessage: "+ message.get_body()+"\n"
	else:
		resp = "No messages in "+name+"\n"
	return Response(response=resp, mimetype="application/json")

@app.route("/queues/<name>/msgs", methods=['DELETE'])
def messages_consume(name):
	"""
	Consume message from a queue
	curl -X DELETE -H 'Accept: application/json' http://localhost:8080/queues/OwenKTest/msgs
	"""

	conn = get_conn()
	queue = conn.get_queue(name)
	messages = queue.get_messages()
	if len(messages) > 0:
		message = messages[0]
		resp = "Queue: "+name+" \n, Message removed" \n"
		queue.delete_message(message)
	else:
		resp = "No messages in, "+name+"\n"
	return Response(response=resp, mimetype="application/json")

def get_conn():
	key_id, secret_access_key = urllib2.urlopen("http://ec2-52-30-7-5.eu-west-1.compute.amazonaws.com:81/key").read().split(':')
	return boto.sqs.connect_to_region("eu-west-1", aws_access_key_id=key_id ,aws_secret_access_key=secret_access_key)

if __name__ == "__main__":
    app.run(host="0.0.0.0",port=5000, debug=True)
