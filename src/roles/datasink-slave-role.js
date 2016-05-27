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
		if( prev && prev.hello_sent ) {
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
			err => {
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

	registerSlave(broker_id, originalConfigMsg) {
		return () => {
			let registerMsg = this.getSlaveRegisterMessage(broker_id);
			return this.respondTo(originalConfigMsg, registerMsg);
		}
	}

	modifyKafkaConfig(broker_id, zookeeper_host, zookeeper_port) {
		var defer = q.defer();
		exec(`/opt/monitoring-configurator/lifecycle/on_configuration_receive.sh ${broker_id} ${zookeeper_host} ${zookeeper_port}`,
			error => {
			    if (error) {
			      this.logger.warn(error);
			      return defer.reject();
			    }
			    this.logger.info('Kafka reconfigured');
			    defer.resolve();
			});
		return defer.promise;
	}

	//Don't configure client here, but initialize hello-config-regslave sequence.
	reconfigureClient(msg) {
		var defer = q.defer();
		this.onStart().then(() => defer.resolve(msg), err => defer.reject(err));
		return defer.promise;
	}

	configureClient(msg) {
		var defer = q.defer();
		this.config.monitoring = _.extend(this.config.monitoring, msg.monitoring);

		if( !this.isValidPort(msg.port) ) {
			defer.reject();
			return defer.promise;
		}

		this.modifyKafkaConfig(msg.brokerId, msg.monitoring.host, msg.monitoring.port)
			.then(this.registerSlave(msg.brokerId, msg), err => defer.reject(err))
			.then(() => defer.resolve(msg), err => defer.reject(err));

		return defer.promise;
	}

	handleConfig(msg) {
		return this.configureClient(msg);
	}

	handleReconfig(msg) {
		return this.reconfigureClient(msg);
	}
}

export default DatasinkSlaveRole;