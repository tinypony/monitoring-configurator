var dgram = require('dgram');
var Netmask = require('netmask').Netmask;
var NODE_TYPE = require('./node-type.js');
import Forwarder from './forwarder/forwarder.js';
var KafkaForwarder = require('./kafka/kafka-forwarder.js');
var uuid = require('node-uuid');
var q = require('q');

import _ from 'underscore'
import winston from 'winston'

import DatasinkRole from './roles/datasink-role.js'
import ProducerRole from './roles/producer-role.js'
import ConsumerRole from './roles/consumer-role.js'
import DatasinkSlaveRole from './roles/datasink-slave-role'


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
			new DatasinkSlaveRole(this.initId, this.config, sockets),
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

	getRoleFunctions(func) {
		var funcs = [];

		_.each(this.roles, (r) => {
			if(r.isMe()){
				funcs.push(r[func].bind(r));
			}
		});

		return funcs;
	}

	onStartListening() {
		this.bc_socket.setBroadcast(true);
		var funcs = this.getRoleFunctions('onStart');
		funcs.push(() => { this.hasStartedDefer.resolve(); });

		return funcs.reduce((promise, f) => {
			return promise.then(f);
		}, q());
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

	handleInChain(msg, func) {
		var funcs = this.getRoleFunctions(func);

		return funcs.reduce((promise, f) => {
			return promise.then(f);
		}, q(msg));
	}

	handleHello(msg) {
		return this.handleInChain(msg, 'handleHello');
	}

	handleSubscribe(msg) {
		return this.handleInChain(msg, 'handleSubscribe');
	}

	handleReconfig(msg) {
		return this.handleInChain(msg, 'handleReconfig');
	}

	handleConfig(msg) {
		return this.handleInChain(msg, 'handleConfig');
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

	handleBroadcastMessage(msg) {
		if(msg.type === 'hello') {
			return this.handleHello(msg);
		}

		//Every type of node is being monitored and needs to be reconfigured
		if( msg.type === 'reconfig' ) {
			return this.handleReconfig(msg);
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
}

export default ConfigurationDaemon;
