 import _ from 'underscore';
import q from 'q';
import NODE_TYPE from '../node-type'
import { MESSAGE_TYPE } from '../message-type'
import { Netmask } from 'netmask'
import winston from 'winston'

class Role {
	constructor(initId, config, sockets) {
		this.initId = initId;
		this.config = config;
		this.sockets = sockets;
		this.logger = this.getLogger();
	}

	getLogger() {
		var logger = new winston.Logger({
			transports: [new winston.transports.Console({level: 'info'})]
		});

		if(this.config.logging && this.config.logging.disable) {
			logger.remove(winston.transports.Console);
		}

		return logger;
	}

	/**
	 * Method that is shared by all role subclasses to send unicast responses to the original sender of a message
	 */
	respondTo(receivedMessage, response) {
		let defer = q.defer();
		this.sockets.unicast.send(
			new Buffer(response),
			0,
			response.length,
			receivedMessage.port,
			receivedMessage.host,
			err => {
				if(err) { return defer.reject(err); } 
				else { return defer.resolve(); }
			}
		);
		return defer.promise;
	}

	/**
	 * Method that is shared by all role subclasses to send broadcast messages to the monitoring network
	 */
	broadcast(message) {
		let defer = q.defer();
		this.sockets.broadcast.send(
			new Buffer(message), 
			0, 
			message.length, 
			this.config.broadcastPort,
			this.getBroadcastAddress(), 
			err => {
				if (err) {
					this.logger.warn(err);
					defer.reject(err);
				} else {
					defer.resolve();
				}
			}
		);

		return defer.promise;
	}	

	isDatasink() {
		return _.contains(this.config.roles, NODE_TYPE.DATASINK);
	}

	isDatasinkSlave() {
		return _.contains(this.config.roles, NODE_TYPE.DATASINK_SLAVE);
	}

	isProducer() {
		return _.contains(this.config.roles, NODE_TYPE.PRODUCER);
	}

	isConsumer() {
		return _.contains(this.config.roles, NODE_TYPE.CONSUMER);
	}

	isValidPort(port) {
		return _.isNumber(port) && port > 0 && port < 65535;
	}

	onStart(prev) {
		var defer = q.defer();
		defer.resolve(prev);
		return defer.promise;
	}

	onStop() {
		var defer = q.defer();
		defer.resolve();
		return defer.promise;
	}

	handleHello(msg) {
		var defer = q.defer();
		defer.resolve(msg);
		return defer.promise;
	}

	handleConfig(msg) {
		var defer = q.defer();
		defer.resolve(msg);
		return defer.promise;
	}

	handleReconfig(msg) {
		var defer = q.defer();
		defer.resolve(msg);
		return defer.promise;
	}

	handleSubscribe(msg) {
		var defer = q.defer();
		defer.resolve(msg);
		return defer.promise;
	}

	handleRegslave(msg) {
		var defer = q.defer();
		defer.resolve(msg);
		return defer.promise;
	}

	getBroadcastAddress() {
		var block = new Netmask(this.config.monitoring.subnet);
		return block.broadcast;
	}

	getReconfigureMessage() {
		var msg = {
			type: MESSAGE_TYPE.RECONFIG,
			host: 'self',
			port: this.config.unicast.port,
			monitoring: {
				host: 'self',
				port: this.config.monitoring.port
			}
		};

		return JSON.stringify(msg);
	}

	getConfigureMessage(extension) {
		var msg = {
			type: MESSAGE_TYPE.CONFIG,
			host: 'self',
			port: this.config.unicast.port,
			monitoring: {
				host: 'self',
				port: this.config.monitoring.port
			}
		};

		msg = _.extend(msg, extension);

		return JSON.stringify(msg);	
	}

	getHelloMessage() {
		var msg = {
			type: MESSAGE_TYPE.HELLO,
			roles: this.config.roles,
			uuid: this.initId,
			host: 'self',
			port: this.config.unicast.port
		};

		return JSON.stringify(msg);
	}

	getSubscribeMessage() {
		var msg = {
			type: MESSAGE_TYPE.SUBSCRIBE,
			host: 'self',
			port: this.config.unicast.port,
			endpoints: this.config.consumers
		};

		return JSON.stringify(msg);
	}

	getSlaveRegisterMessage(brokerId, extension) {
		var msg = {
			type: MESSAGE_TYPE.REGISTER_SLAVE,
			host: 'self',
			port: this.config.unicast.port,
			brokerId: brokerId
		};
		
		msg = _.extend(msg, extension);
		return JSON.stringify(msg);
	}
}

export default Role;