import dgram from 'dgram';
import _ from 'underscore';
import q from 'q';
import {Client, HighLevelProducer} from 'kafka-node';
import uuid from 'node-uuid';
import winston from 'winston';
import Dequeue from 'dequeue';

function isValidPort(port) {
	return _.isNumber(port) && port > 0 && port < 65535;
}


class Forwarder {
	constructor(config) {
		this.id = uuid.v4();
		this.debug = true;
		this.config = config;
		this.count = 0;
		this.store = 0;
		this.logger = new winston.Logger({
			transports: [new winston.transports.Console()]
		});

		if( config.logging && config.logging.disable ) {
			this.logger.remove(winston.transports.Console);
		}

		this.FIFO = new Dequeue();

		/**
		 * fwd.port,
		 * fwd.topic
		 */
		this.forward_ports = _.map(config.producers, fwd => {
			this.logger.info("Forwarding configuration = %d => %s", fwd.port, fwd.topic);
			var skt = dgram.createSocket('udp4');
			skt.bind(fwd.port, '127.0.0.1');

			skt.on('error', er => {
				this.logger.warn(`[Forwarder.constructor()] ${er}`);
			});

			skt.on("message", this.storeInQueue.bind(this, fwd.topic));
			return skt;
		});

		// setInterval(() => {
		// 	console.log(this.count);
		// 	console.log(this.store);
		// }, 5000);
	}

	storeInQueue(topic, data_buf) {
		let data = data_buf.toString()
		this.FIFO.push({
			topic,
			data
		});
		this.store++;

		if(this.FIFO.length === 1) 
			setImmediate(this.run.bind(this));
	}

	/* Continuously polls the queue and forwards messages from it */
	run() {
		while(this.FIFO.length > 0) {
			let item = this.FIFO.shift();
			this.forward(item.topic, item.data);
		}
	}
	
	reconfig(config) {
		if(!isValidPort(config.monitoring.port)) {
			this.logger.info('trying to configure forwarder with an invalid port');
			return;
		}
		this.config = config;
		this.forwardToAddress = config.monitoring.host;
		this.forwardToPort = config.monitoring.port;
		this.logger.info('[Forwarder.reconfig()] Reconfiguring forwarder');
		this.reconnect();
	}

	getZK() {
		return this.forwardToAddress + ':' + this.forwardToPort;
	}

	createConnection() {
		var defer = q.defer();
		var connectionString = this.getZK();
		this.logger.info('Create zookeeper connection to %s', connectionString);
		var client = new Client(connectionString, this.id);
		var producer = new HighLevelProducer(client);
		
		producer.on('ready', () => {
			this.logger.info('Forwader is ready');
			this.producer = producer;
			this.client = client;
			defer.resolve();
		});

		producer.on('error', err => {
			this.logger.warn('[Forwarder.reconfig()] Error: %s', JSON.stringify(err));
			defer.reject(err);
		});

		this.logger.info('[Forwarder] Created new producer');
		return defer.promise;
	}

	reconnect() {
		var defer = q.defer();
		this.logger.info('[Forwarder.reconnect()] Using nodejs forwarder');

		if (this.producer) {
			this.producer.close(() => {
				this.logger.info('[Forwarder.reconnect()] Closed the producer, reconnecting');
				this.producer = null;
				this.createConnection()
					.then(defer.resolve, err => defer.reject(err));
			});
		} else {
			this.createConnection()
				.then(defer.resolve, err => defer.reject(err));
		}

		return defer.promise;
	}

	forward(topic, data) {
		var msgStr = data.toString();
	    var messages = msgStr.split('\n');

		messages = _.map(messages, (m) => {
			var val = m.replace(/\r$/g, '');
			return val;
		});
		
		if(!this.forwardToPort || !this.forwardToAddress || !this.producer) {
			return ;
		}
			
		//contain possible errors if datasink is temporarily down
		try {
			this.producer.send([{
				topic: topic,
				messages: messages
			}], err => {
				if(err) {
					return this.logger.warn(`[Forwarder.forward()] ${JSON.stringify(err)}`);
				}

				if(this.debug || topic === 'latency') {
					this.logger.info(`Forwarded ${messages}`);
					this.debug = false;
				}
			});
		} catch(e) {
			this.logger.warn(e); //carry on
		}
	}
}

export default Forwarder;
