import Role from './roles';
import q from 'q';

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
					console.log('sent');
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
	//	this.logger.info('configure client with ' + JSON.stringify(msg));
		this.config.monitoring = _.extend(this.config.monitoring, msg.monitoring);
		if( !isValidPort(msg.port) ) {
			//this.logger.info('trying to send subscription message to an invalid port');
			return defer.reject();
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
				defer.resolve();
				//this.logger.info('Sent subscribe request to ' + msg.host + ":" + msg.port);
			}
		);

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