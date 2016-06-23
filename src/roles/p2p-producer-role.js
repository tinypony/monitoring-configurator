import Role from './roles';
import Forwarder from '../forwarder/p2p-forwarder';
import q from 'q';
import NODE_TYPE from '../node-type';
import _ from 'underscore';


class P2PProducerRole extends Role {
	constructor(initId, config, sockets) {
		super(initId, config, sockets);
		
		if(this.isP2PProducer()) {
			this.forwarder = new Forwarder(this.config);
		}
	}

	isMe() {
		return this.isP2PProducer();
	}

	onStart(prev) {
		if( prev && prev.hello_sent ) {
			return super.onStart(prev);
		}

		let message = this.getHelloMessage();
		this.broadcast(message).then(() => {
			defer.resolve({
				hello_sent: true
			});
		}, err => defer.reject(err));

		return defer.promise;
	}

	handleNewDestination(msg) {
		this.logger.log(`[p2p-producer] handleNewDestination()`);
		this.forwarder.addForwaringInfo(msg.topic, msg.dest);
		var defer = q.defer();
		defer.resolve();
		return defer.promise;
	}

	handleTReconfig(msg) {
		var defer = q.defer();
		var publish = this.getSubscribeMessage();
		this.respondTo(msg, publish).then(() => { defer.resolve(msg); }, err => defer.reject(err));
		return defer.promise;
	}
}

export default P2PProducerRole;