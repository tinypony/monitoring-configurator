import _ from 'underscore';
import Role from './roles';
import q from 'q';
import Dequeue from 'dequeue';

class Tracker extends Role {
	constructor(initId, config, sockets) {
		super(initId, config, sockets);
		this.producers = []; //Array<{port:int, host:String, topics: Array<String>}>
		this.consumers = {}; //Map<String, Array<{port:int, host:String}>>
		this.newDestinationFIFO = new Dequeue();
	}

	isMe() {
		return this.isTracker();
	}

	onStart() {
		var defer = q.defer();
		var message = this.getTrackerReconfigureMessage();
		this.logger.info('[Tracker] onStart()');
		this.broadcast(message).then(() => {
			this.logger.info('[Tracker] Broadcasted tracker config');
			defer.resolve();
		}, err => defer.reject(err));

		return defer.promise;
	}

	wasProducer(msg) {
		return msg.roles && _.contains(msg.roles, NODE_TYPE.PRODUCER);
	}

	wasConsumer(msg) {
		return msg.roles && _.contains(msg.roles, NODE_TYPE.CONSUMER);
	}

	handleHello(msg) {
		var defer = q.defer();

		if(this.wasProducer(msg)) {
			this.registerProducer(msg.host, msg.port, msg.publish);
		}

		if(this.wasConsumer(msg)) {
			this.registerConsumer(msg.subscribe);
		}
		
		d.resolve(msg);
		return defer.promise;
	}

	handleSubscribe(msg) {
		this.registerConsumer(msg.subscribe);
	}

	handlePublish(msg) {
		this.registerProducer(msg.host, msg.port, msg.publish);
	}

	registerProducer(host, port, writeTopics) {
		let source = {
			host,
			port,
			topics: writeTopics
		};

		this.producers.push(source);

		let consumersSubset = _.pick(this.consumers, writeTopics);
		_.each(consumersSubset, (endpoints, topic) => {
			_.each(endpoints, endpoint => {
				this.notifyProducer(source, topic, endpoint);
			});
		});
	}

	addTopicEndpointMapping(topic, endpoint, is_new = false) {
		if(is_new) {
			this.consumers[topic]= [endpoint];
		} else {
			this.consumers[topic].push(endpoint);
		}

		this.notifyProducers(topic, endpoint);
	}

	notifyProducers(topic, endpoint) {
		topicWriters = _.filter(this.producers, p => _.contains(p.topics, topic));

		_.each(topicWriters, source => {
			this.notifyProducer(source, topic, endpoint);
		});
	}

	notifyProducer(source, topic, dest) {
		this.newDestinationFIFO.push({
			dest,
			topic,
			source: {host: source.host, port: parseInt(source.port)}
		});

		if(this.newDestinationFIFO.length === 1) {
			setImmediate(this.flushQueue.bind(this));
		}
	}

	flushQueue() {
		while(this.newDestinationFIFO.length) {
			let item = this.newDestinationFIFO.shift();
			let msg = this.getNewDestinationMessage(item.topic, item.dest);
			this.sockets.unicast.send(
				new Buffer(msg),
				0,
				msg.length,
				item.source.port,
				item.source.host
			);
		}
	}

	/**
	 * subscription.topics
	 * subscription.host
	 * subscription.port
	 *
	 */
	registerConsumer(subscriptions) {
		_.each(subscriptions, sub => {
			let endpoint = { host: sub.host, port: parseInt(sub.port), protocol: sub.protocol ? sub.protocol: 'udp' };

			_.each(sub.topics, t => {
				if(!this.consumers[t] || !this.consumers[t].length) {
					this.addTopicEndpointMapping(t, endpoint, true);
					return;
				}

				let existing = _.findWhere(this.consumers[t], endpoint);
				if(!existing) {
					this.addTopicEndpointMapping(t, endpoint);
				} else {
					this.notifyProducers(t, endpoint);
				}
			});
		});
	}
}

export default Tracker;