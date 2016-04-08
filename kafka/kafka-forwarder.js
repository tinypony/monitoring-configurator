var dgram = require('dgram');
var _ = require('underscore');
var kafka = require('kafka-node');

var firstMessageLogged = false;

var KAFKA_ERROR = {
	isNodeExists: function(err) {
		return _.isString(err.message) && err.message.indexOf('NODE_EXISTS') > -1;
	},
	isCouldNotFindBroker: function(err) {
		return _.isString(err.message) && err.message.indexOf('Could not find a broker') > -1;
	}
};

//forwards message from kafka to clients who subscribed to particular topics
var KafkaForwarder = function(config) {
	this.config = config;
	this.connections = [];
	this.ou_socket = dgram.createSocket('udp4');
};

KafkaForwarder.prototype.getConnectionString = function() {
	return this.config.monitoring.host + ':' + this.config.monitoring.port;
};

KafkaForwarder.prototype.send = function(msg, host, port) {
	this.ou_socket.send(
	 	new Buffer(msg), 
	 	0, 
	 	msg.length, 
	 	port,
	 	host, 
	 	function(err) {
	 		if (err) console.log(err);
	 		if (!firstMessageLogged) {
	 			console.log("Sent message " + msg + " to subscribed client " + host + ":" + port);
	 			firstMessageLogged = true;
	 		}
	 	}
	);
}

KafkaForwarder.prototype.getPingMessage = function() {
	var msg = {
		type: 'ping',
		host: 'self',
		port: this.config.unicast.port
	}

	return JSON.stringify(msg);
};

KafkaForwarder.prototype.hasConnection = function(sub) {
	var existing = _.findWhere(this.connections, {host: sub.host, port: sub.port });

	if(!existing) {
		return false;
	}

	return this.getClientId(existing) === this.getClientId(sub); //a quick hack, as we basically need to ensure that IDs are unique
};

KafkaForwarder.prototype.getClientId = function(sub) {
	return sub.host + "-" + sub.port + "-" + sub.topics.join('-');
};

/**
 * sub.topics,
 * sub.unicastport  //port for sending control signals
 * sub.port,		//port to send subscribed data
 * sub.host
 */

KafkaForwarder.prototype.subscribe = function(sub) {
	var self = this;

	if(this.hasConnection(sub)) {
		return;
	}

	var client = new kafka.Client(this.getConnectionString(), this.getClientId(sub));
	var payloads = _.map(sub.topics, function(topic) {
		return {
			topic: topic
		};
	});

	var consumer = new kafka.HighLevelConsumer(client, payloads, {
		autoCommit: true,
    	autoCommitIntervalMs: 5000,
    	encoding: 'utf8'
	});

	consumer.on("error", function(err) {
		console.log('[KafkaForwarder]');
		console.log(JSON.stringify(err));

		//Waiting for kafka to timeout and clear previous connection
		if( KAFKA_ERROR.isNodeExists(err) ) {
			console.log('Waiting for kafka to clear previous connection');
			setTimeout(this.subscribe.bind(this, sub), 5000);
		} 
		//Waiting for KAFKA to spin up (possibly)
		else if(KAFKA_ERROR.isCouldNotFindBroker(err)) {
			console.log('Waiting for kafka to spin up');
			setTimeout(this.subscribe.bind(this, sub), 5000);
		}
	}.bind(this));

	consumer.on('message', function(msg) {
		if(!msg.value) {
			return;
		}
		if(!firstMessageLogged) {
			console.log("Send message " + msg + " to subscribed client " + sub.host + ":" + sub.port);
		}

		this.send(msg.value, sub.host, parseInt(sub.port));
	}.bind(this));

	consumer.on('connect', function() {
		this.connections.push({
			host: sub.host,
			port: sub.unicastport,
			topics: sub.topics,
			consumer: consumer,
			liveStatus: 1			// 0 - unresponsive, 1 - live, 2 - pending check
		});
		console.log('Subscribed ' + this.getClientId(sub));
	}.bind(this));

	
};

module.exports = KafkaForwarder;
