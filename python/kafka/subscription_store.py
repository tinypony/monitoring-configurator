import threading

def to_tuple(sub):
	return (sub.host, sub.port,)

class Subscription(object):
	def __init__(self, topic, host, port):
		self._topic = topics
		self._host = host
		self._port = port

	@property
	def topic(self):
		return self._topic

	@property
	def host(self):
		return self._host

	@property
	def port(self):
		return self._port

	@staticmethod
	def create_subscriptions(topics, host, port):
		retval = []
		
		for t in topics:
			retval.append(Subscription(t, host, port))

		return retval

class SubscriptionStore:
	
	def __init__(self):
		self.subscriptions = {};
		self._lock = threading.Lock()

	def _is_subscribed(self, subscribers, sub):
		for s in subscribers:
			if s.host == sub.host and s.port == sub.port:
				return True

		return False

	def _parse_subscription_string(self, subscription_string):
		subscription_string_list = subscription_string.split('=')
		topics = subscription_string_list[0]
		host = subscription_string_list[1].split(':')[0]
		port = int(subscription_string_list[1].split(':')[1])

		return Subscription.create_subscriptions(topics.split(','), host, port)

	def subscribe(self, subscription_string):
		new_subscriptions = self._parse_subscription_string(subscription_string)
		self._lock.acquire()
		
		for sub in new_subscriptions:
			topic_subscribers = self.subscriptions.get(sub.topic, [])

			if not self._is_subscribed(topic_subscribers, sub):
				topic_subscribers.append(sub)
		
		self._lock.release()

	def get_subscribed_endpoints(self, topic_name):
		self._lock.acquire()
		retval = map(to_tuple, self.subscriptions.get(topic_name, []))
		self._lock.release()
		return retval

