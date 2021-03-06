import _ from 'underscore';
import Role from './roles';
import q from 'q';
import Dequeue from 'dequeue';
import NODE_TYPE from '../node-type';

const DEFAULT_CONSUMER_PROTOCOL = 'udp';

class Tracker extends Role {
	constructor(initId, config, sockets) {
		super(initId, config, sockets);
		this.producers = []; //Array<{port:int, host:String, topics: Array<String>}>
		this.consumers = {}; //Map<topic:String -> Array<{port:int, host:String}>>
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
		return msg.roles && _.contains(msg.roles, NODE_TYPE.P2PPRODUCER);
	}

	wasConsumer(msg) {
		return msg.roles && _.contains(msg.roles, NODE_TYPE.P2PCONSUMER);
	}

	enhanceWithHost(host, endpoints) {
		return _.map(endpoints, ep => {
			return _.extend({}, ep, { host });
		});
	}

	handleHello(msg) {
		var defer = q.defer();
		this.logger.info(`[Tracker] handleHello( ${JSON.stringify(msg)} )`);
		
		if(this.wasProducer(msg)) {
			this.registerProducer(msg.host, msg.port, msg.publish);
		}

		if(this.wasConsumer(msg)) {
			this.registerConsumer( this.enhanceWithHost(msg.host, msg.subscribe) );
		}
		
		defer.resolve(msg);
		return defer.promise;
	}

	handleSubscribe(msg) {
		this.registerConsumer(this.enhanceWithHost(msg.host, msg.subscribe));
		return super.handleSubscribe(msg);
	}

	handlePublish(msg) {
		this.registerProducer(msg.host, msg.port, msg.publish);
		return super.handlePublish(msg);
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
		this.logger.info(`[Tracker] addTopicEndpointMapping( ${JSON.stringify(topic)}, ${JSON.stringify(endpoint)})`);

		if(is_new) {
			this.consumers[topic] = [endpoint];
		} else {
			this.consumers[topic].push(endpoint);
		}

		this.notifyProducers(topic, endpoint);
	}

	notifyProducers(topic, dest) {
		this.logger.info(`[Tracker] notifyProducers( ${JSON.stringify(topic)}, ${JSON.stringify(dest)})`);
		this.logger.info(`[Tracker] producers = ${JSON.stringify(this.producers)}`);
		const topicWriters = _.filter(this.producers, p => _.contains(p.topics, topic));

		_.each(topicWriters, source => {
			this.notifyProducer(source, topic, dest);
		});
	}

	/**
	 * dest: {host, port}
	 * topic: string
	 * source: {host, port}
	 */
	notifyProducer(source, topic, dest) {
		this.logger.info(`[Tracker] notifyProducer( ${JSON.stringify(source)}, ${JSON.stringify(topic)}, ${JSON.stringify(dest)})`);

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
			this.logger.info(`Send new destination to ${item.source.host}:${item.source.port}`);
			this.send(item.source.host, item.source.port, msg).then(() => {
				this.logger.info(`Send topic to endpoint mapping ${JSON.stringify(item.topic)} -> ${JSON.stringify(item.dest)}`)
			});
		}
	}

	/**
	 * subscription.topics
	 * subscription.host
	 * subscription.port
	 *
	 */
	registerConsumer(subscriptions) {
		this.logger.info(`[Tracker] registerConsumer( ${JSON.stringify(subscriptions)} )`);

		_.each(subscriptions, sub => {
			let endpoint = { host: sub.host, port: parseInt(sub.port), protocol: sub.protocol ? sub.protocol: DEFAULT_CONSUMER_PROTOCOL };

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