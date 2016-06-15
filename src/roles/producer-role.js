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
			return super.onStart(prev);
		}

		var defer = q.defer();
		let message = this.getHelloMessage();

		this.sockets.broadcast.send(
			new Buffer(message), 
			0, 
			message.length, 
			this.config.broadcastPort,
			this.getBroadcastAddress(), 
			(err) => {
				if (err) {
					this.logger.warn(err);
					return defer.reject(err);
				} else {
					defer.resolve({
						hello_sent: true
					});
				}
			}
		);

		return defer.promise;
	}

	handleClusterResize(msg) {
		var defer = q.defer();
		let done = defer.resolve.bind(defer, msg);

		this.forwarder.reconnect()
			.then(done, er => defer.reject(er));
		return defer.promise;
	}

	configureClient(msg) {
		var defer = q.defer();
		this.config.monitoring = _.extend(this.config.monitoring, msg.monitoring);
		this.forwarder.reconfig(this.config);
		this.logger.info('Producer has be configured with ' + JSON.stringify(msg));
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