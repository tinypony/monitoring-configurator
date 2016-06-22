import Role from './roles';
import Forwarder from '../forwarder/p2p-forwarder.js';
import q from 'q';
import _ from 'underscore';


class ProducerRole extends Role {
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

	handleNewDestination(msg) {
		this.forwarder.addForwaringInfo(msg.topic, msg.dest);
		var defer = q.defer();
		defer.resolve();
		return defer.promise;
	}

	handleTReconfig(msg) {
		var defer = q.defer();
		var msg = this.getSubscribeMessage();
		this.respondTo(msg, msg).then(() => { defer.resolve(msg); }, err => defer.reject(err));
		return defer.promise;
	}
}

export default ProducerRole;