import Role from './roles';
import KafkaForwarder from '../kafka/kafka-forwarder.js';
import q from 'q';


class DatasinkRole extends Role {
	constructor(initId, config, sockets) {
		super(initId, config, sockets);

		if(this.isDatasink()) {
			this.kafkaForwarder = new KafkaForwarder(config);
		}
	}

	onStart() {
		if(!this.isDatasink()) {
			return super.onStart();
		}

		var defer = q.defer();
		var message = this.getReconfigureMessage();

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
				}
				defer.resolve();
			}
		);
		return defer.promise;
	}

	handleHello(msg) {
		if(!this.isDatasink()) {
			return super.handleHello(msg); //does nothing just returns resolved promise
		}

		var defer = q.defer();

		if(this.initId && msg.uuid === this.initId) {
			this.address = msg.host;
			this.initId = null;
		}

		if( this.isDatasink() ) {
			var configMessage = this.getConfigureMessage();

			this.sockets.unicast.send(
				new Buffer(configMessage),
				0,
				configMessage.length,
				msg.port,
				msg.host,
				(err) => {
					if(err) {
						return defer.reject(err);
					} 
					defer.resolve(msg);
				}
			);
		}

		return defer.promise;
	}

	handleSubscribe(msg) {
		if(!this.isDatasink) {
			return super.handleSubscribe(msg); //does nothing just returns resolved promise
		}

		var defer = q.defer();
		this.logger.info('let\'s subscribe');

		if(_.isEmpty(msg.endpoints)) {
			defer.resolve(msg);
		}

		var callback = _.after(msg.endpoints.length, () => {
			defer.resolve(msg);
		});

		_.each(msg.endpoints, (ep) => {
			this.logger.info('wire %s to %s:%d', ep.topics.join(","), msg.host, ep.port);
			ep.host = msg.host;
			ep.unicastport = msg.port;
			this.kafkaForwarder.subscribe(ep);
			callback();
		});
		
		return defer.promise;
	}
}

export default DatasinkRole;