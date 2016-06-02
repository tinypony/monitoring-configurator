var dgram = require('dgram');
var _ = require('underscore');

import {Client, HighLevelProducer} from 'kafka-node';

var uuid = require('node-uuid');
var winston = require('winston');

function isValidPort(port) {
	return _.isNumber(port) && port > 0 && port < 65535;
}

class Forwarder {
	constructor(config) {
		this.ou_socket = dgram.createSocket('udp4');
		this.id = uuid.v4();

		this.logger = new winston.Logger({
			transports: [new winston.transports.Console()]
		});

		if( config.logging && config.logging.disable ) {
			this.logger.remove(winston.transports.Console);
		}

		/**
		 * fwd.port,
		 * fwd.topic
		 */
		this.forward_ports = _.map(config.producers, (fwd) => {
			this.logger.info("Forwarding configuration = %d => %s", fwd.port, fwd.topic);
			var skt = dgram.createSocket('udp4');
			skt.bind(fwd.port, '127.0.0.1');

			skt.on('error', (er) => {
				this.logger.warn(`[Forwarder.constructor()] ${er}`);
			});

			skt.on("message", this.forward.bind(this, fwd.topic));
			return skt;
		});
	}
	
	reconfig(config) {
		if(!isValidPort(config.monitoring.port)) {
			this.logger.info('trying to configure forwarder with an invalid port');
			return;
		}

		this.forwardToAddress = config.monitoring.host;
		this.forwardToPort = config.monitoring.port;
		this.logger.info('[Forwarder.reconfig()] Reconfiguring forwarder');


		var createConnection = () => {
			var connectionString = this.forwardToAddress + ':' + this.forwardToPort;
			this.logger.info('Create zookeeper connection to %s', connectionString);
			var client = new Client(connectionString, this.id);
			var producer = new HighLevelProducer(client);
			
			producer.on('ready', () => {
				this.logger.info('Forwader is ready');
				this.producer = producer;
				this.client = client;
			});

			producer.on('error', err => {
				this.logger.warn('[Forwarder.reconfig()] Error: %s', JSON.stringify(err));
			});

			this.logger.info('[Forwarder] Created producer');
		}

		if (this.client) {
			this.client.close(createConnection);
		} else {
			createConnection();
		}	
	}

	forward(topic, data) {
		var msgStr = data.toString();
	    var messages = msgStr.split('\n');

		messages = _.map(messages, (m) => {
			var val = m.replace(/\r$/g, '');
			return val;
		});
		
		if(!this.forwardToPort || !this.forwardToAddress || !this.producer) {
			return ;
		}
			
		//contain possible errors if datasink is temporarily down
		try {
			this.producer.send([{
				topic: topic,
				messages: messages
			}], err => {
				if(err) {
					return this.logger.warn(`[Forwarder.forward()] ${JSON.stringify(err)}`);
				}
				//this.logger.info('Forwarded messages: '+JSON.stringify(messages));
			});
		} catch(e) {
			this.logger.warn(e); //carry on
		}
	}
}

export default Forwarder;
