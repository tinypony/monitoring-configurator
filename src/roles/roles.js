var _ = require('underscore');
var q = require('q');
import NODE_TYPE from '../node-type.js';
import {Netmask} from 'netmask';

class Role {

	constructor(initId, config, sockets) {
		this.initId = initId;
		this.config = config;
		this.sockets = sockets;
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

	isValidPort(port) {
		return _.isNumber(port) && port > 0 && port < 65535;
	}

	onStart() {
		var defer = q.defer();
		defer.resolve();
		return defer.promise;
	}

	shouldProcess(msg) {
		return false;
	}

	onStop() {
		var defer = q.defer();
		defer.resolve();
		return defer.promise;
	}

	handleHello(msg) {
		var defer = q.defer();
		defer.resolve();
		return defer.promise;
	}

	handleConfig(msg) {
		var defer = q.defer();
		defer.resolve();
		return defer.promise;
	}

	handleReconfig(msg) {
		var defer = q.defer();
		defer.resolve();
		return defer.promise;
	}

	handleSubscribe() {
		var defer = q.defer();
		defer.resolve();
		return defer.promise;
	}

	getBroadcastAddress() {
		var block = new Netmask(this.config.monitoring.subnet);
		return block.broadcast;
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
}

export default Role;