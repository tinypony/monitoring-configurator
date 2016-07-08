import dgram from 'dgram';
import net from 'net';
import _ from 'underscore';
import q from 'q';
import {Client, HighLevelProducer} from 'kafka-node';
import uuid from 'node-uuid';
import winston from 'winston';
import Dequeue from 'double-ended-queue';

function isValidPort(port) {
	return _.isNumber(port) && port > 0 && port < 65535;
}


class Forwarder {
	constructor(config) {
		this.id = uuid.v4();
		this.debug = true;
		this.config = config;

		this.logger = new winston.Logger({
			transports: [new winston.transports.Console()]
		});

		if( config.logging && config.logging.disable ) {
			this.logger.remove(winston.transports.Console);
		}


		/**
		 * fwd.port,
		 * fwd.topic,
		 * fwd.protocol //udp or tcp
		 */
		this.forward_ports = [];
		_.each(config.producers, fwd => {
			if(fwd.protocol === 'tcp')
				this.createTcpSocket(fwd).done(binding => { this.forward_ports.push(binding) });
			else
				this.createUDPSocket(fwd).done(binding => { this.forward_ports.push(binding) });
		});
	}

	createUDPSocket(fwd) {
		var defer = q.defer();
		this.logger.info("Forwarding configuration = %d => %s", fwd.port, fwd.topic);
		var skt = dgram.createSocket('udp4');
		skt.bind(fwd.port, '127.0.0.1');

		skt.on('error', er => {
			this.logger.warn(`[Forwarder.constructor()] ${er}`);
		});

		var binding = {
			protocol: 'udp',
			socket: skt,
			port: fwd.port,
			topic: fwd.topic
		};

		skt.on("message", this.forward.bind(this, fwd.topic));
		defer.resolve(binding);

		return defer.promise;
	}

	createTcpSocket(fwd) {
		var defer = q.defer();
		this.logger.info("Forwarding configuration = %d => %s", fwd.port, fwd.topic);
		// Start a TCP Server
		net.createServer(socket => {
			// Identify this client
			socket.name = socket.remoteAddress + ":" + socket.remotePort;
			// Handle incoming messages from clients.
			socket.on('data', this.forward.bind(this, fwd.topic));
		}).listen(fwd.port, () => {
			var binding = {
				protocol: 'tcp',
				port: fwd.port,
				topic: fwd.topic
			};
			defer.resolve(binding);
		});

		return defer.promise;
	}

	// storeInQueue(topic, binding, data_buf) {
	// 	let data = data_buf.toString();
	// 	if(!data) return;

	// 	var { FIFO } = binding;

	// 	FIFO.push(data);

	// 	this.logger.info(`[Forwarder] Sotred in queue ${data}`);

	// 	if(binding.FIFO_flushed) {
	// 		binding.FIFO_flushed = false;
	// 		setImmediate(this.run.bind(this, binding));
	// 	}
	// }

	//  Continuously polls the queue and forwards messages from it 
	// run(binding) {
	// 	let { FIFO, topic } = binding;

	// 	while(FIFO.length) {
	// 		let messages = [];

	// 		for(let i=0; i<10; i++) {
	// 			let data = FIFO.shift();
	// 			if(data) messages.push(data);
	// 		}

	// 		this.forward(topic, messages.join('\n'));
	// 	}
		
	// 	binding.FIFO_flushed = true;
	// }
	
	reconfig(config) {
		if(!isValidPort(config.monitoring.port)) {
			this.logger.warn('trying to configure forwarder with an invalid port');
			return;
		}
		this.config = config;
		this.forwardToAddress = config.monitoring.host;
		this.forwardToPort = config.monitoring.port;
		this.logger.info('[Forwarder.reconfig()] Reconfiguring forwarder');

		this.reconnect()
			.catch( err => { 
				this.logger.warn(`[Forwarder.reconfig()] ${JSON.stringify(err)}`);
				this.reconnect();
			});
	}

	reconnect() {		
		this.logger.info('[Forwarder.reconnect()] Using nodejs forwarder');

		if (this.producer) {
			this.logger.info('[Forwarder.reconnect()] Close previous producer');
			var defer = q.defer();

			this.producer.close(() => {
				this.logger.info('[Forwarder.reconnect()] Closed the producer, reconnecting');
				this.producer = null;
				this.createConnection()
					.then(defer.resolve, err => defer.reject(err));
			});

			return defer.promise;
		} else {
			this.logger.info('[Forwarder.reconnect()] No existing producer, proceed');
			return this.createConnection();
		}
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
			defer.resolve();
		});

		producer.on('error', err => {
			this.logger.warn('[Forwarder.createConnection()] Error: %s', JSON.stringify(err));
			console.trace( "error");
		});

		this.logger.info('[Forwarder] Created new kafka producer and attached handlers');
		return defer.promise;
	}



	forward(topic, data) {
		var msgStr = data.toString();
	    var messages = msgStr.split('\n');

		messages = _.map(messages, (m) => {
			var val = m.replace(/\r$/g, '');
			return val;
		});
		
		if(!this.forwardToPort || !this.forwardToAddress || !this.producer || !msgStr) {
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

				if(this.debug) {
					this.logger.info(`Forwarded ${messages.length} messages`);
					this.debug = false;
				}
			});

			if(topic === 'latency') {
				//this.logger.info(`Forwarding ${messages.length} messages`);
			}
		} catch(e) {
			this.logger.warn(e); //carry on
		}
	}
}

export default Forwarder;
