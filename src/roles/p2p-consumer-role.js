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
		var defer = q.defer();
		var msg = this.getSubscribeMessage();
		this.respondTo(msg, msg).then(() => { defer.resolve(msg); }, err => defer.reject(err));
		return defer.promise;
	}
}

export default P2PConsumerRole;