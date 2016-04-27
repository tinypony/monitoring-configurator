var dgram = require('dgram');
var _ = require('underscore');
var Netmask = require('netmask').Netmask;
var NODE_TYPE = require('./node-type.js');
import Forwarder from './forwarder/forwarder.js';
var KafkaForwarder = require('./kafka/kafka-forwarder.js');
var uuid = require('node-uuid');
var q = require('q');
var winston = require('winston');

import DatasinkRole from './roles/datasink-role.js';
import ProducerRole from './roles/producer-role.js';
import ConsumerRole from './roles/consumer-role.js';


function isValidPort(port) {
	return _.isNumber(port) && port > 0 && port < 65535;
}

class ConfigurationDaemon {
	constructor(config, broadcastPort) {
		this.logger = new winston.Logger({
			transports: [new winston.transports.Console()]
		});

		if(config.logging && config.logging.disable) {
			this.logger.remove(winston.transports.Console);
		}

		this.config = config;
		this.config.broadcastPort = broadcastPort;
		this.address = null;
		this.initId = uuid.v4();

		if(this.isProducer()) {
			this.initProducer(config);
		}

		if(this.isDatasink()) {
			this.initDatasink(config);
		}

		if(this.isConsumer()) {
			this.initConsumer(config);
		}

		this.broadcastPort = broadcastPort;
		this.bc_socket = dgram.createSocket('udp4');
		this.uc_socket = dgram.createSocket('udp4');

		//Attach message handlers
		this.bc_socket.on('message', this.getMessageHandler(true).bind(this));
		this.uc_socket.on('message', this.getMessageHandler(false).bind(this));

		//bind sockets and attach on listen
		this.bc_socket.bind(this.broadcastPort, '0.0.0.0');
		this.uc_socket.bind(this.config.unicast.port, '0.0.0.0');
		this.bc_socket.on('listening', this.onStartListening.bind(this));

		var sockets = {
			unicast: this.uc_socket,
			broadcast: this.bc_socket
		};

		this.roles = [
			new DatasinkRole(this.initId, this.config, sockets),
			new ProducerRole(this.initId, this.config, sockets),
			new ConsumerRole(this.initId, this.config, sockets)
		];

		this.hasStartedDefer = q.defer();
		this.hasStarted = this.hasStartedDefer.promise;

	}

	initDatasink(config) {
		this.kafkaForwarder = new KafkaForwarder(config);
	}

	initConsumer(config) {

	}

	initProducer(config) {
		this.forwarder = new Forwarder(config);
	}


	onStartListening() {
		this.bc_socket.setBroadcast(true);
		var defer = q.defer();
		defer.resolve();
		var promise = defer.promise;

		_.each(this.roles, (r) => {
			promise = promise.then(r.onStart());
		});

		promise.then(() => {
			this.hasStartedDefer.resolve();
		});
	}

	getBroadcastAddress() {
		var block = new Netmask(this.config.monitoring.subnet);
		return block.broadcast;
	}

	isDatasink() {
		return _.contains(this.config.roles, NODE_TYPE.DATASINK);
	}

	isProducer() {
		return _.contains(this.config.roles, NODE_TYPE.PRODUCER);
	}

	isConsumer() {
		return _.contains(this.config.roles, NODE_TYPE.CONSUMER);
	}

	handleHello(msg) {
		var defer = q.defer();
		defer.resolve();
		var promise = defer.promise;

		_.each(this.roles, (r) => {
			promise = promise.then(r.handleHello(msg));
		});

		return promise;
	}

	handleSubscribe(msg) {
		var defer = q.defer();
		defer.resolve();
		var promise = defer.promise;

		_.each(this.roles, (r) => {
			promise = promise.then(r.handleSubscribe(msg));
		});

		return promise;
	}

	handleBroadcastMessage(msg) {
		if(msg.type === 'hello') {
			return this.handleHello(msg);
		}

		//Every type of node is being monitored and needs to be reconfigured
		if( msg.type === 'reconfig' && (this.isProducer() || this.isConsumer()) ) {
			return this.handleReconfig(msg);
		}
	}

	handleReconfig(msg) {
		var defer = q.defer();
		defer.resolve();
		var promise = defer.promise;

		_.each(this.roles, (r) => {
			promise = promise.then(r.handleReconfig(msg));
		});

		return promise;
	}

	handleConfig(msg) {
		var defer = q.defer();
		defer.resolve();
		var promise = defer.promise;

		_.each(this.roles, (r) => {
			promise = promise.then(r.handleConfig(msg));
		});

		return promise;
	}

	configureClient(msg) {
		var defer = q.defer();

		if( this.isProducer() || this.isConsumer() ) {
			this.logger.info('configure client with ' + JSON.stringify(msg));
		 	this.config.monitoring = _.extend(this.config.monitoring, msg.monitoring);
				
			if(this.isProducer()) {
				this.forwarder.reconfig(this.config);
				defer.resolve();
			}

			if(this.isConsumer()) {
				if( !isValidPort(msg.port) ) {
					this.logger.info('trying to send subscription message to an invalid port');
					return defer.reject();
				}
				var subscribeMsg = this.getSubscribeMessage();

				this.uc_socket.send(
					new Buffer(subscribeMsg),
					0,
					subscribeMsg.length,
					msg.port,
					msg.host, 
					(e) => {
						if(e) {
							this.logger.warn(JSON.stringify(e));
							return defer.reject(e);
						}
						defer.resolve();
						this.logger.info('Sent subscribe request to ' + msg.host + ":" + msg.port);
					}
				);
			}
		} else {
			defer.reject('nothing to configure');
		}

		return defer.promise;
	}

	close() {
		this.uc_socket.close();
		this.bc_socket.close();
	}

	//Client node is provided with configuration by a manager node
	handleUnicastMessage(msg) {
		if( msg.type === 'config' ) {
			return this.handleConfig(msg);
		}

		if( msg.type === 'subscribe' ) {
			return this.handleSubscribe(msg);
		}
	}

	preprocessMessage(msg, rinfo) {
		if(msg.monitoring) {
			msg.monitoring.host = rinfo.address;
		}

		if(msg.host) {
			msg.host = rinfo.address;
		}

		return msg;
	}

	getMessageHandler(isBroadcast) {
		return ( data, rinfo) => {
			var dataString = data.toString();

			try {
				var msg = JSON.parse(dataString);
				msg = this.preprocessMessage( msg, rinfo );
				
				if(isBroadcast)
					this.handleBroadcastMessage(msg);
				else 
					this.handleUnicastMessage(msg);
			} catch(e) {
				//silent skip
				this.logger.info("Could not parse incoming data, probably malformed");
			}
		}
	}

	getReconfigureMessage() {
		var msg = {
			type: 'reconfig',
			host: 'self',
			port: this.config.unicast.port,
			monitoring: {
				host: 'self',
				port: this.config.monitoring.port
			}
		};

		return JSON.stringify(msg);
	}

	getConfigureMessage() {
		var msg = {
			type: 'config',
			host: 'self',
			port: this.config.unicast.port,
			monitoring: {
				host: 'self',
				port: this.config.monitoring.port
			}
		};

		return JSON.stringify(msg);	
	}

	getHelloMessage() {
		var msg = {
			type: 'hello',
			uuid: this.initId,
			host: 'self',
			port: this.config.unicast.port
		};

		return JSON.stringify(msg);
	}

	getSubscribeMessage() {
		var msg = {
			type: 'subscribe',
			host: 'self',
			port: this.config.unicast.port,
			endpoints: this.config.consumers
		}

		return JSON.stringify(msg);
	}

	getPongMessage() {
		var msg = {
			type: 'pong',
			host: 'self',
			port: this.config.unicast.port
		}

		return JSON.stringify(msg);
	}
}

export default ConfigurationDaemon;
