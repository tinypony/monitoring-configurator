import Role from './roles';
import q from 'q';
import _ from 'underscore';

class ConsumerRole extends Role {
	constructor(initId, config, sockets) {
		super(initId, config, sockets);
	}

	isMe() {
		return this.isConsumer();
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

	configureClient(msg) {
		var defer = q.defer();
		this.config.monitoring = _.extend(this.config.monitoring, msg.monitoring);

		if( !this.isValidPort(msg.port) ) {
			this.logger.warn('trying to send subscription message to an invalid port');
			defer.reject();
			return defer.promise;
		}
		this.logger.info('Consumer received configuration ' + JSON.stringify(msg));
		var subscribeMsg = this.getSubscribeMessage();

		this.respondTo(msg, subscribeMsg).then(() => { 
			this.logger.info('Subscribed with ' + subscribeMsg);
			defer.resolve(msg);
		}, err => {
			this.logger.warn(JSON.stringify(err));
			defer.reject(err);
		});

		return defer.promise;
	}

	handleConfig(msg) {
		return this.configureClient(msg);
	}

	handleReconfig(msg) {
		return this.configureClient(msg);
	}
}

export default ConsumerRole;