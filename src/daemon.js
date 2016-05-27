import dgram from 'dgram';
import { Netmask } from 'netmask';
import NODE_TYPE from './node-type';
import { MESSAGE_TYPE } from './message-type';
import Forwarder from './forwarder/forwarder';
import KafkaForwarder from './kafka/kafka-forwarder';
import uuid from 'node-uuid';
import q from'q';
import _ from 'underscore'
import winston from 'winston'

import DatasinkRole from './roles/datasink-role'
import ProducerRole from './roles/producer-role'
import ConsumerRole from './roles/consumer-role'
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

	handleRegslave(msg) {
		return this.handleInChain(msg, 'handleRegslave');
	}

	close() {
		this.uc_socket.close();
		this.bc_socket.close();
	}

	//Client node is provided with configuration by a manager node
	handleUnicastMessage(msg) {
		if( msg.type === MESSAGE_TYPE.CONFIG ) {
			return this.handleConfig(msg);
		}

		if( msg.type === MESSAGE_TYPE.SUBSCRIBE ) {
			return this.handleSubscribe(msg);
		}

		if( msg.type === MESSAGE_TYPE.REGISTER_SLAVE ) {
			return this.handleRegslave(msg);
		}
	}

	handleBroadcastMessage(msg) {
		if(msg.type === MESSAGE_TYPE.HELLO) {
			return this.handleHello(msg);
		}

		//Every type of node is being monitored and needs to be reconfigured
		if( msg.type === MESSAGE_TYPE.RECONFIG ) {
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
