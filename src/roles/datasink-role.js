import Role from './roles';
import KafkaForwarder from '../kafka/kafka-forwarder.js';
import q from 'q';
import _ from 'underscore';

class DatasinkRole extends Role {
	constructor(initId, config, sockets) {
		super(initId, config, sockets);

		if(this.isDatasink()) {
			this.kafkaForwarder = new KafkaForwarder(config);
		}
	}

	isMe() {
		return this.isDatasink();
	}

	onStart() {
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
				this.logger.info('[Datasink] Broadcasted datasink config');

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
					this.logger.info('[Datasink] Responded to hello');
					defer.resolve(msg);
				}
			);
		}

		return defer.promise;
	}

	handleSubscribe(msg) {
		var defer = q.defer();
		this.logger.info('[Datasink] handle subscribe ' + JSON.stringify(msg));
		
		if(_.isEmpty(msg.endpoints)) {
			this.logger.info('[Datasink] no endpoints specified in subscribe request');
			defer.resolve(msg);
		}

		var callback = _.after(msg.endpoints.length, () => {
			this.logger.info('[Datasink] all endpoints subscribed');
			defer.resolve(msg);
		});

		_.each(msg.endpoints, (ep) => {
			this.logger.info('[Datasink] wire %s to %s:%d', ep.topics.join(","), msg.host, ep.port);
			ep.host = msg.host;
			ep.unicastport = msg.port;
			this.kafkaForwarder.subscribe(ep);
			callback();
		});
		
		return defer.promise;
	}
}

export default DatasinkRole;