from pykafka import KafkaClient, Consumer
import argparse

class Pusher:
	def __init__(self, sub_host, bindings, zk):
		self.subscriber = { 'host': sub_host, bindings: bindings }
		self.zk_host = zk;
		self.consumer = self.createKafkaConsumer()
		self.runnable = False

	def createKafkaConsumer(self):
		client = KafkaClient(zookeeper_hosts=self.zk_host)
		client.update_cluster()
		


parser = argparse.ArgumentParser('Process that forwards messages from kafka to subscribed consumers')
parser.add_argument('--zk', dest='zk' type=str, help='Zookeeper connection string', required=True)
parser.add_argument('--ip', dest='ip', type=str, help='Subscriber ip adress', required=True)
parser.add_argument('--bindings', dest='bindings', type=str, nargs='+', required=True, help='Forwarding configuration in format topic1,topic2:port1 topic3:port2')
args = parser.parse_args()

if __name__ == '__main__':

