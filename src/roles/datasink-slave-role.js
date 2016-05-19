import Role from './roles'
import _ from 'underscore'
import KafkaForwarder from '../kafka/kafka-forwarder.js'
import q from 'q'
import { exec } from 'child_process'

class DatasinkSlaveRole extends Role {
	constructor(initId, config, sockets) {
		super(initId, config, sockets);
	}

	isMe() {
		return this.isDatasinkSlave();
	}

	onStart(prev) {
		if(!this.isDatasinkSlave() || (prev && prev.hello_sent) ) {
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

	modifyKafkaConfig(zookeeper_host, zookeeper_port) {
		exec('/opt/monitoring-configurator/lifecycle/on_configuration_receive.sh ' + zookeeper_host + ' ' + zookeeper_port,
			(error, stdout, stderr) => {
			    if (error !== null) {
			      return;// this.logger.warn(error);
			    }
			    this.logger.info('Kafka reconfigured');
			});
	}

	configureClient(msg) {
		var defer = q.defer();
		this.config.monitoring = _.extend(this.config.monitoring, msg.monitoring);

		if( !this.isValidPort(msg.port) ) {
			this.logger.warn('trying to send subscription message to an invalid port');
			defer.reject();
			return defer.promise;
		}

		this.modifyKafkaConfig(msg.monitoring.host, msg.monitoring.port);

		defer.resolve();
		return defer.promise;
	}

	handleConfig(msg) {
		return this.configureClient(msg);
	}

	handleReconfig(msg) {
		return this.configureClient(msg);
	}
}

export default DatasinkSlaveRole;