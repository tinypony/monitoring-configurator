from pykafka import KafkaClient, SimpleConsumer
from subscription_store import SubscriptionStore
from uuid import uuid4
import sys, os
import argparse
import socket
from threading import Thread


class Pusher(object):
	def __init__(self, sub_host, bindings, zk):
		self.subscriber = { 'host': sub_host, bindings: bindings }
		self.zk_host = zk;
		self.consumer = self.createKafkaConsumer()
		self.runnable = False

	def createKafkaConsumer(self):
		client = KafkaClient(zookeeper_hosts=self.zk_host)
		client.update_cluster()


class Frontend(Thread):
	def __init__(self, store):
		self.store = store
		self.socket_addr = '/tmp/kafka-pusher'
		if os.path.exists( self.socket_addr ):
  			os.remove( self.socket_addr )

		self.input_socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
		self.input_socket.bind(self.socket_addr)

	def run(self):
		while True:
			datagram = self.input_socket.recv(1024)
			if not datagram:
				break
			elif datagram == 'exit':
				break 
			else:
				self.store.subscribe(datagram)



class Daemon(Thread):
	def __init__(self, zk_host):
		self.store = SubscriptionStore()
		self._consumers = []
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP
		self._kafka_client = self.create_kafka_client(zk_host)
		self.counter = 0

		for t in self._kafka_client.cluster.topics.values():
			self.create_kafka_consumer(t)


	def get_store(self):
		return self.store

	def create_kafka_client(self, zk_host):
		try:
			client = KafkaClient(zookeeper_hosts=zk_host)
			client.update_cluster()
			return client
		except:
			print 'Unexpected error: ' + sys.exc_info()[0]
			raise

	def create_kafka_consumer(self, topic):
		try:
			consumer_gid = 'python_kafka_forwarder_{}'.format(topic.name)
			consumer = SimpleConsumer(topic, self._kafka_client.cluster, consumer_group=consumer_gid, consumer_id=str(uuid4()), auto_commit_enable=True)
			consumer.start()
			self._consumers.append(consumer)
		except:
			print 'Unexpected error: ' + sys.exc_info()[0]
			raise

	def subscribe(self, subsription_string):
		self.store.subscribe(subsription_string)

	@property 
	def consumers(self):
		return self._consumers

	def send_to_subscribers(self, topic_name, msg):		
		endpoints = self.store.get_subscribed_endpoints(topic_name)
		for host, port in endpoints:
			self.sock.sendto(str(msg.value), (host, port))

			if self.counter < 2:
				print 'pushed {}'.format(self.counter)
			self.counter += 1

	def run(self):
		while True:
			for consumer in self._consumers:
				msg = consumer.consume(False)
				if msg is not None:
					self.send_to_subscribers(consumer.topic.name, msg)




parser = argparse.ArgumentParser('Process that forwards messages from kafka to subscribed consumers')
parser.add_argument('--zk', dest='zk', type=str, help='Zookeeper connection string', required=True)
parser.add_argument('--bindings', dest='bindings', type=str, nargs='+', required=True, help='Forwarding configuration in format topic1,topic2=host1:port1 topic3=host1:port2')
args = parser.parse_args()

if __name__ == '__main__':
	daemon = Daemon(args.zk)
	front = Frontend(daemon.get_store())

	front.run()
	daemon.run()

	front.join()
	daemon.join()

