var dgram = require('dgram');
var _ = require('underscore');
import winston from 'winston';
import { Client, HighLevelProducer } from 'kafka-node';

var firstMessageLogged = false;

var KAFKA_ERROR = {
	isNodeExists: function(err) {
		return _.isString(err.message) && err.message.indexOf('NODE_EXISTS') > -1;
	},
	isCouldNotFindBroker: function(err) {
		return _.isString(err.message) && err.message.indexOf('Could not find a broker') > -1;
	}
};

//forwards message from kafka to clients who subscribed to particular topics
class KafkaForwarder {
	constructor (config) {
		this.config = config;
		this.connections = [];
		this.ou_socket = dgram.createSocket('udp4');

		this.logger = new winston.Logger({
			transports: [new winston.transports.Console({leve: 'info'})]
		});

		if( config.logging && config.logging.disable ) {
			this.logger.remove(winston.transports.Console);
		}
	}

	getConnectionString() {
		return this.config.monitoring.host + ':' + this.config.monitoring.port;
	}

	send(msg, host, port) {
		this.ou_socket.send(
		 	new Buffer(msg), 
		 	0, 
		 	msg.length, 
		 	port,
		 	host, 
		 	function(err) {
		 		if (err) return this.logger.warn(err);
		 		//if (!firstMessageLogged) {
		 			this.logger.info('Sent message "%s" to subscribed client %s:%d', msg, host, port);
		 			firstMessageLogged = true;
		 		//}
		 	}.bind(this)
		);
	}

	getPingMessage() {
		var msg = {
			type: 'ping',
			host: 'self',
			port: this.config.unicast.port
		}

		return JSON.stringify(msg);
	}

	hasConnection(sub) {
		var existing = _.findWhere(this.connections, {host: sub.host, port: sub.port });

		if(!existing) {
			return false;
		}

		return this.getClientId(existing) === this.getClientId(sub); //a quick hack, as we basically need to ensure that IDs are unique
	}

	getClientId(sub) {
		return sub.host + "-" + sub.port + "-" + sub.topics.join('-');
	}

	/**
	 * sub.topics,
	 * sub.unicastport  //port for sending control signals
	 * sub.port,		//port to send subscribed data
	 * sub.host
	 */

	subscribe(sub) {
		if(this.hasConnection(sub)) {
			return;
		}
		this.logger.info('[KafkaForwarder] Subscribing %s:%d', sub.host, sub.port);
		var client = new Client(this.getConnectionString(), this.getClientId(sub));
		var payloads = _.map(sub.topics, function(topic) {
			return {
				topic: topic
			};
		});

		var consumer = new HighLevelConsumer(client, payloads, {
			autoCommit: true,
	    	autoCommitIntervalMs: 5000,
	    	encoding: 'utf8'
		});

		consumer.on("error", (err) => {
			this.logger.warn('[KafkaForwarder]');
			this.logger.warn(JSON.stringify(err));

			//Waiting for kafka to timeout and clear previous connection
			if( KAFKA_ERROR.isNodeExists(err) ) {
				this.logger.info('Waiting for kafka to clear previous connection');
				setTimeout(this.subscribe.bind(this, sub), 5000);
			} 
			//Waiting for KAFKA to spin up (possibly)
			else if(KAFKA_ERROR.isCouldNotFindBroker(err)) {
				this.logger.info('Waiting for kafka to spin up');
				setTimeout(this.subscribe.bind(this, sub), 5000);
			}
		});

		consumer.on('message', (msg) => {
			if(!msg.value) {
				return;
			}

			this.send(msg.value, sub.host, parseInt(sub.port));
		});

		consumer.on('connect', () => {
			this.connections.push({
				host: sub.host,
				port: sub.unicastport,
				topics: sub.topics,
				consumer: consumer,
				liveStatus: 1			// 0 - unresponsive, 1 - live, 2 - pending check
			});
			this.logger.info('Subscribed ' + this.getClientId(sub));
		});
	}
}

export default KafkaForwarder;
