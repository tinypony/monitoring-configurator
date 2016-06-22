import Role from './roles';
import q from 'q';

import _ from 'underscore';
import KafkaPuller from '../kafka/kafka-puller';

class P2PConsumerRole extends Role {
	constructor(initId, config, sockets) {
		super(initId, config, sockets);
	}

	isMe() {
		return this.isP2PConsumer();
	}

	onStart(prev) {
		if( prev && prev.hello_sent ) {
			return super.onStart();
		}

		var defer = q.defer();
		let message = this.getHelloMessage();

		this.broadcast(message).then(() => {
			this.logger.info('[p2p-Consumer] Broacasted hello');
			defer.resolve({
				hello_sent: true
			});
		}, err => defer.reject(err));

		return defer.promise;
	}

	handleTReconfig(msg) {
		this.logger.info(`[p2p-Consumer] handleTReconfig(${msg})`);
		var defer = q.defer();
		var response = this.getSubscribeMessage();
		this.logger.info(`[p2p-Consumer] subscribe with ${response}`);
		this.respondTo(msg, response).then(() => { defer.resolve(msg); }, err => defer.reject(err));
		return defer.promise;
	}
}

export default P2PConsumerRole;