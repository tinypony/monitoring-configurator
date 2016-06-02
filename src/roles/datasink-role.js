import Role from './roles';
import KafkaForwarder from '../kafka/kafka-forwarder.js';
import q from 'q';
import _ from 'underscore';
import NODE_TYPE from '../node-type';
import { exec } from 'child_process'

class DatasinkRole extends Role {
	constructor(initId, config, sockets) {
		super(initId, config, sockets);

		if(this.isMe()) {
			this.kafkaForwarder = new KafkaForwarder(config);
			this.brokers = [0];
			this.nextBrokerId = 1;
		}
	}

	isMe() {
		return this.isDatasink();
	}

	onStart() {
		var defer = q.defer();
		var message = this.getReconfigureMessage();

		this.broadcast(message).then(() => {
			this.logger.info('[Datasink] Broadcasted datasink config');
			defer.resolve();
		}, err => defer.reject(err));
			
		return defer.promise;
	}

	isSlave(msg) {
		return msg.roles && _.contains(msg.roles, NODE_TYPE.DATASINK_SLAVE);
	}	

	handleHello(msg) {
		var defer = q.defer();

		if(this.initId && msg.uuid === this.initId) {
			this.address = msg.host;
			this.initId = null;
		}

		var configMessage = this.getConfigureMessage(this.isSlave(msg) ? { brokerId: this.nextBrokerId } : undefined);

		this.respondTo(msg, configMessage).then(() => {
			this.logger.info('[Datasink] Responded to hello');
			if(this.isSlave(msg)) {
				this.logger.info('[Datasink] Responded to hello from slave');
				this.nextBrokerId++;
			}
			defer.resolve(msg);
		}, err => defer.reject(err));
		

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

		_.each(msg.endpoints, ep => {
			this.logger.info('[Datasink] wire %s to %s:%d', ep.topics.join(","), msg.host, ep.port);
			ep.host = msg.host;
			ep.unicastport = msg.port;
			this.kafkaForwarder.subscribe(ep);
			callback();
		});
		
		return defer.promise;
	}

	rebalanceCluster() {
		var defer = q.defer();
		var command = `/opt/monitoring-configurator/lifecycle/on_cluster_expand.py --brokers "${this.brokers}"`;
		this.logger.info('Running command ' + command);

		exec(command, ( error, stdout, stderr ) => {
		    if (error) {
		      	this.logger.warn(error);
		      	this.logger.warn(stderr);
		      	return defer.reject(error);
		    }
		    this.logger.info('RebalanceCluster has finished');
		    this.logger.info(stdout);
		    this.kafkaForwarder.handleRebalance();
		    defer.resolve();
		});
		return defer.promise;
	}

	handleRegslave(msg) {
		var defer = q.defer();
		this.brokers.push(msg.brokerId);
		this.logger.info(`Registered brokers: ${this.brokers.join(',')}`);
		this.rebalanceCluster().then(()=>defer.resolve(msg), er => defer.reject(er));
		return defer.promise;
	}
}

export default DatasinkRole;