import Role from './roles';
import q from 'q';

import _ from 'underscore';
import KafkaPuller from '../kafka/kafka-puller';

class ConsumerRole extends Role {
	constructor(initId, config, sockets) {
		super(initId, config, sockets);
		if(this.isMe()) {
			this.puller =  new KafkaPuller(this.config);
		}
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

		this.puller.subscribe(this.config.consumers[0], this.config.monitoring); //only one consumer binding is used at the moment
		defer.resolve(msg);

		return defer.promise;
	}

	handleClusterResize(msg) {
		// this.logger.info(`[Consumer] handle cluster resize`);
		// return this.configureClient(msg);
		var defer = q.defer();
		defer.resolve(msg);
		return defer.promise;
	}

	handleConfig(msg) {
		this.logger.info(`[Consumer] handle config ${JSON.stringify(msg)}`);
		return this.configureClient(msg);
	}

	handleReconfig(msg) {
		this.logger.info(`[Consumer] handle reconfig ${JSON.stringify(msg)}`);
		return this.configureClient(msg);
	}
}

export default ConsumerRole;