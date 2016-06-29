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

	send(host, port, message) {
		var defer = q.defer();

		this.sockets.unicast.send(
			new Buffer(message),
			0,
			message.length,
			port,
			host,
			err => {
				if(err) { return defer.reject(err); } 
				else { return defer.resolve(); }
			}
		);

		return defer.promise;
	}

	/**
	 * Method that is shared by all role subclasses to send unicast responses to the original sender of a message
	 */
	respondTo(receivedMessage, response) {
		return this.send(receivedMessage.host, receivedMessage.port, response);
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

	isTracker() {
		return _.contains(this.config.roles, NODE_TYPE.TRACKER);
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

	isP2PProducer() {
		return _.contains(this.config.roles, NODE_TYPE.P2PPRODUCER);
	}

	isP2PConsumer() {
		return _.contains(this.config.roles, NODE_TYPE.P2PCONSUMER);
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

	handleTConfig(msg) {
		var defer = q.defer();
		defer.resolve(msg);
		return defer.promise;
	}

	handleTReconfig(msg) {
		var defer = q.defer();
		defer.resolve(msg);
		return defer.promise;
	}

	handlePublish(msg) {
		var defer = q.defer();
		defer.resolve(msg);
		return defer.promise;
	}

	handleRegslave(msg) {
		var defer = q.defer();
		defer.resolve(msg);
		return defer.promise;
	}

	handleNewDestination(msg) {
		var defer = q.defer();
		defer.resolve(msg);
		return defer.promise;
	}

	handleClusterResize(msg) {
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

	getTrackerReconfigureMessage() {
		var msg = {
			type: MESSAGE_TYPE.TRECONFIG,
			host: 'self',
			port: this.config.unicast.port
		};

		return JSON.stringify(msg);
	}

	getTrackerConfigureMessage(extension) {
		var msg = {
			type: MESSAGE_TYPE.TCONFIG,
			host: 'self',
			port: this.config.unicast.port
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
			publish: this.config.producers ? _.map(this.config.producers, p => p.topic) : [],
			subscribe: this.config.consumers ? this.config.consumers : [],
			port: this.config.unicast.port
		};

		return JSON.stringify(msg);
	}

	getSubscribeMessage() {
		var msg = {
			type: MESSAGE_TYPE.SUBSCRIBE,
			host: 'self',
			port: this.config.unicast.port,
			subscribe: this.config.consumers ? this.config.consumers : []
		};

		return JSON.stringify(msg);
	}

	getPublishMessage() {
		var msg = {
			type: MESSAGE_TYPE.PUBLISH,
			host: 'self',
			port: this.config.unicast.port,
			publish: this.config.producers ? _.map(this.config.producers, p => p.topic) : []
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

	getClusterResizeMessage() {
		var msg = {
			type: MESSAGE_TYPE.CLUSTER_RESIZE,
			host: 'self',
			port: this.config.unicast.port,
			monitoring: {
				host: 'self',
				port: this.config.monitoring.port
			}
		};

		return JSON.stringify(msg);
	}

	getNewDestinationMessage(topic, endpoint) {
		var msg = {
			type: MESSAGE_TYPE.NEW_DESTINATION,
			host: 'self',
			port: this.config.unicast.port,
			topic,
			dest: endpoint
		}

		return JSON.stringify(msg);
	}


}

export default Role;