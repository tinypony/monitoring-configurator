from pykafka import KafkaClient, Producer
from threading import Thread
import socket

class Forwarder(Thread):
	def __init__(self, local_port, topic, zookeeper_host='localhost:2181'):
		self.local_port = local_port
		self.topic = topic
		self.zk_host = zookeeper_host;
		self.producer = self.createKafkaProducer()
		self.runnable = False
		self.counter = 0
		

	def createKafkaProducer(self):
		client = KafkaClient(zookeeper_hosts=self.zk_host)
		client.update_cluster()
		producer = Producer(client.cluster, client.cluster.topics[self.topic])
		producer.start()
		return producer

	def operate(self):
		self.runnable = True

	def finish(self):
		self.runnable = False
		self.producer.stop()

	def run(self):
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP
		self.sock.bind(('localhost', self.local_port))

		while(self.runnable):
			data, addr = self.sock.recvfrom(4096)
			self.producer.produce(data)
			print 'forwarded {}'.format(self.counter++)

		self.sock.close()
