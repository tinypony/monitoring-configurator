import Role from './roles';
import q from 'q';
import _ from 'underscore';

class ConsumerRole extends Role {
	constructor(initId, config, sockets) {
		super(initId, config, sockets);
	}

	onStart(prev) {
		if(!this.isConsumer() || (prev && prev.hello_sent) ) {
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

		if( !this.isValidPort(msg.port) ) {
			this.logger.warn('trying to send subscription message to an invalid port');
			defer.reject();
			return defer.promise;
		}

		var subscribeMsg = this.getSubscribeMessage();

		this.sockets.unicast.send(
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
				defer.resolve(msg);
			}
		);

		return defer.promise;
	}

	handleConfig(msg) {
		if(!this.isConsumer()) {
			return super.handleConfig();
		}
		return this.configureClient(msg);
	}

	handleReconfig(msg) {
		if(!this.isConsumer()) {
			return super.handleReconfig();
		}
		return this.configureClient(msg);
	}
}

export default ConsumerRole;