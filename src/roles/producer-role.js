import Role from './roles';
import Forwarder from '../forwarder/forwarder.js';
import q from 'q';
import _ from 'underscore';


class ProducerRole extends Role {
	constructor(initId, config, sockets) {
		super(initId, config, sockets);

		if(this.isProducer()) {
			this.forwarder = new Forwarder(this.config);
		}
	}

	onStart(prev) {
		if(!this.isProducer() || (prev && prev.hello_sent) ) {
			return super.onStart();
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

	configureClient(msg) {
		var defer = q.defer();
		this.config.monitoring = _.extend(this.config.monitoring, msg.monitoring);
		this.forwarder.reconfig(this.config);

		defer.resolve(msg);
		return defer.promise;
	}

	handleConfig(msg) {
		if(!this.isProducer()) {
			return super.handleReconfig(msg);
		}
		return this.configureClient(msg);
	}

	handleReconfig(msg) {
		if(!this.isProducer()) {
			return super.handleReconfig(msg);
		}
		return this.configureClient(msg);
	}
}

export default ProducerRole;