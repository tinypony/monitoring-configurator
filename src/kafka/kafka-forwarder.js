import dgram from 'dgram'
import _ from 'underscore'
import winston from 'winston'
import { Client, HighLevelConsumer } from 'kafka-node'

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
		this.connections = {};
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

	handleRebalance() {
		_.each(this.connection, con => {
			//recreate consumer for all connections
			con.consumer.close(true, () => {
				this.createConsumer(con.subInfo);
			});
		});
	}

	send(msg, host, port) {
		this.ou_socket.send(
		 	new Buffer(msg), 
		 	0, 
		 	msg.length, 
		 	port,
		 	host, 
		 	err => {
		 		if (err) return this.logger.warn(`[KafkaForwarder.send()] ${JSON.stringify(err)}`);
		 		if (!firstMessageLogged) {
		 			this.logger.info('Sent message "%s" to subscribed client %s:%d', msg, host, port);
		 			firstMessageLogged = true;
		 		}
		 	}
		);
	}

	hasConnection(sub) {
		var existing = _.findWhere(_.values(this.connections), {host: sub.host, port: sub.port });

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
		this.createConsumer(sub);
	}

	createConsumer(sub) {
		var client = new Client(this.getConnectionString(), this.getClientId(sub));
		this.logger.info('[KafkaForwarder] created client');
		var payloads = _.map(sub.topics, function(topic) {
			return {
				topic: topic
			};
		});

		this.logger.info('[KafkaForwarder] creating consumer');
		var consumer = new HighLevelConsumer(client, payloads, {
			autoCommit: true,
	    	autoCommitIntervalMs: 5000,
	    	encoding: 'utf8'
		});
		this.logger.info('[KafkaForwarder] created consumer');

		//Handle consumer connection error
		consumer.on("error", err => {
		//	this.logger.warn('[KafkaForwarder]');
		//	this.logger.warn(JSON.stringify(err));

			//Waiting for kafka to timeout and clear previous connection
			if( KAFKA_ERROR.isNodeExists(err) ) {
				this.logger.info('Waiting for kafka to clear previous connection');
				setTimeout(this.createConsumer.bind(this, sub), 5000);
			} 
			//Waiting for KAFKA to spin up (possibly)
			else if(KAFKA_ERROR.isCouldNotFindBroker(err)) {
				this.logger.info('Waiting for kafka to spin up');
				setTimeout(this.createConsumer.bind(this, sub), 5000);
			}
		});

		consumer.on('message', msg => {
			if(!msg.value) {
				//this.logger.warn('[KafkaForwarder] message empty, drop');
				return;
			}
			
			this.send(msg.value, sub.host, parseInt(sub.port));
		});

		consumer.on('connect', () => {
			this.connections[this.getClientId(sub)] = {
				host: sub.host,
				port: sub.unicastport,
				topics: sub.topics,
				consumer: consumer,
				liveStatus: 1,			// 0 - unresponsive, 1 - live, 2 - pending check
				subInfo: sub
			};
			this.logger.info('Subscribed ' + this.getClientId(sub));
		});

		this.logger.info('[KafkaForwarder] Attached all required callbacks to consumer');
	}
}

export default KafkaForwarder;
