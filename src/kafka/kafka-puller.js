import dgram from 'dgram';
import q from 'q';
import _ from 'underscore';
import winston from 'winston';
import Dequeue from 'dequeue';
import uuid from 'node-uuid';
import { Client, HighLevelConsumer } from 'kafka-node';
import os from 'os';

var firstMessageLogged = false;
var latencyC = 0;

var KAFKA_ERROR = {
	isNodeExists: function(err) {
		return _.isString(err.message) && err.message.indexOf('NODE_EXISTS') > -1;
	},
	isCouldNotFindBroker: function(err) {
		return _.isString(err.message) && err.message.indexOf('Could not find a broker') > -1;
	}
};

//forwards message from kafka to clients who subscribed to particular topics
class KafkaPuller {
	constructor (config) {
		this.config = config;
		this.consumer;
		this.ou_socket = dgram.createSocket('udp4');
		this.latency_partitions = [];

		this.logger = new winston.Logger({
			transports: [new winston.transports.Console({leve: 'info'})]
		});

		if( config.logging && config.logging.disable ) {
			this.logger.remove(winston.transports.Console);
		}

		setInterval(() => {
			this.logger.info('Received messages from partitions ' + JSON.stringify(this.latency_partitions));
		});
	}

	getConnectionString(monitoring) {
		return monitoring.host + ':' + monitoring.port;
	}

	handleRebalance() {
		_.each(this.connection, con => {
			//recreate consumer for all connections
			con.consumer.close(true, () => {
				this.createConsumer(con.subInfo);
			});
		});
	}

	send(msg, port) {
		this.ou_socket.send(
		 	new Buffer(msg), 
		 	0, 
		 	msg.length, 
		 	port,
		 	'127.0.0.1', 
		 	err => {
		 		if (err) { return this.logger.warn(`[KafkaPuller.send()] ${JSON.stringify(err)}`); }
		 		if (!firstMessageLogged) {
		 			this.logger.info('[KafkaPuller] Passed message "%s" to subscribed client 127.0.0.1:%d', msg, port);
		 			firstMessageLogged = true;
		 		}
		 	}
		);
	}

	getClientId(sub) {
		return uuid.v4();
		//return os.hostname() + "-" + sub.port + "-" + sub.topics.join('-');
	}

	handleConsumerError(err, sub, monitoring) {
		if( KAFKA_ERROR.isNodeExists(err) ) {
			this.logger.info('[KafkaPuller] Waiting for kafka to clear previous connection');
			this.consumer = null;
			setTimeout(this.subscribe.bind(this, sub, monitoring), 5000); 			
		} else if(KAFKA_ERROR.isCouldNotFindBroker(err)) { //Waiting for KAFKA to spin up (possibly)
			this.logger.info('[KafkaPuller] Waiting for kafka to spin up');
			this.consumer =null;
			setTimeout(this.subscribe.bind(this, sub, monitoring), 5000);
		} else {
			this.logger.warn(JSON.stringify(err));
		}
	}

	/**
	 * sub.topics,
	 * sub.port			//port to send subscribed data
	 *
	 * monitoring.host 	//zk host
	 * monitoring.port  //zk port
	 */

	subscribe(sub, monitoring) {
		if(this.consumer) {
			this.consumer.close(() => {
				this.consumer = null;
				this.subscribe(sub, monitoring);
			});
		} else {
			this.logger.info('[KafkaPuller] Subscribing 127.0.0.1:%d', sub.port);
			this.createConsumer(sub, monitoring)


				.then( args => {
					let { consumer, FIFO, port } = args; 
					this.consumer = consumer;

					this.logger.info('[KafkaPuller] Attach message handler consumer');
					this.consumer.on('message', msg => {
						console.log(JSON.stringify(msg));
						if(!msg.value) {
							return;
						}

						if(msg.topic === 'latency' && !_.contains(this.latency_partitions, msg.partition)) {
							this.latency_partitions.push(msg.partition)
						}

						FIFO.push({
							port: port,
							msg: msg.value
						});

						if(FIFO.length === 1) {
							setImmediate(this.run.bind(this, FIFO));
						}
					});

					this.logger.info('[KafkaPuller] Attached all required callbacks to consumer');

				}).catch( err => {
					this.logger.warn('[KafkaPuller] Here we have error in catch ' + JSON.stringify(err));
					this.handleConsumerError(err, sub, monitoring);
				});
		}
	}

	createConsumer(sub, monitoring) {
		var connStr = this.getConnectionString(monitoring);

		this.logger.info(`[KafkaPuller] Creating consumer for ${connStr}, ${sub.topics.join(' ')} => ${sub.port}`);
		var defer = q.defer();
		var client = new Client(connStr, this.getClientId(sub));
		var FIFO = new Dequeue();

		let payloads = _.map(sub.topics, function(topic) {
			return { topic };
		});

		var consumer = new HighLevelConsumer(client, payloads, {
			autoCommit: true,
	    	autoCommitIntervalMs: 5000,
	    	encoding: 'utf8'
		});

		this.logger.info('[KafkaPuller] created consumer');

		//Handle consumer connection error
		consumer.on('error', err => {
			defer.reject(err);
		});

		defer.resolve({ 
			consumer, 
			FIFO, 
			port: parseInt(sub.port)
		});

		return defer.promise;
	}

	run(FIFO) {
		while(FIFO.length) {
			let item = FIFO.shift();
			this.send(item.msg, item.port);
		}
	}
}

export default KafkaPuller;
