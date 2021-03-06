import Role from './roles';
import Forwarder from '../forwarder/forwarder.js';
import q from 'q';
import _ from 'underscore';


class ProducerRole extends Role {
	constructor(initId, config, sockets) {
		super(initId, config, sockets);
		
		if(this.isProducer()) {
			this.forwarder = new Forwarder(this.config, false);
		}
	}

	isMe() {
		return this.isProducer();
	}

	onStart(prev) {

		if( prev && prev.hello_sent ) {
			return super.onStart();
		}

		var defer = q.defer();

		let message = this.getHelloMessage();
		this.broadcast(message).then(() => {
			defer.resolve({
				hello_sent: true
			});
		}, err => defer.reject(err));

		return defer.promise;
	}

	handleClusterResize(msg) {
		// var defer = q.defer();
		// let done = defer.resolve.bind(defer, msg);

		// this.forwarder.reconnect()
		// 	.then(done, er => defer.reject(er));
		// return defer.promise;
		var defer = q.defer();
		defer.resolve(msg);
		return defer.promise;
	}

	configureClient(msg) {
		var defer = q.defer();
		this.config.monitoring = _.extend(this.config.monitoring, msg.monitoring);
		this.forwarder.reconfig(this.config);
		this.logger.info('[Producer] Producer has be configured with ' + JSON.stringify(msg));
		defer.resolve(msg);
		return defer.promise;
	}

	handleConfig(msg) {
		return this.configureClient(msg);
	}

	handleReconfig(msg) {
		return this.configureClient(msg);
	}
}

export default ProducerRole;