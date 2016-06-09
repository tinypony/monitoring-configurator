import atexit
import argparse
from forwarder import Forwarder
import signal
import sys, os

parser = argparse.ArgumentParser('Daemon that creates forwarders for each provided port-topic pair')
parser.add_argument('--bindings', required=True, dest='bindings', type=str, nargs='+', help='List of port-topic pair for forwarding. Format is port1:topic1 port2:topic2 port3:topic3 ...')
parser.add_argument('--zk', required=True, dest='zk', type=str, help='host:port pair of zookeeper endpoint. Example 127.0.0.1:2181')
args = parser.parse_args()

forwarders = []


def stop_forwarders():
	for f in forwarders:
		f.finish()

	print 'Exiting forwarder daemon'


if __name__ == '__main__':
	try:
		for binding in args.bindings:
			port_topic_pair = binding.split(':')
			forwarder = Forwarder(int(port_topic_pair[0]), port_topic_pair[1], zookeeper_host=args.zk)
			forwarders.append(forwarder)
			forwarder.operate()
			forwarder.run()

		for f in forwarders:
			f.join()

	except KeyboardInterrupt:
		print 'Interrupted'
		try:
			stop_forwarders()
			sys.exit(0)
		except SystemExit:
			os._exit(0)
