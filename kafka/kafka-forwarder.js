var dgram = require('dgram');
var _ = require('underscore');
var kafka = require('kafka-node');

//forwards message from kafka to clients who subscribed to particular topics
var KafkaForwarder = function(config) {
	this.config = config;
	this.connections = [];
	this.ou_socket = dgram.createSocket('udp4');

};

KafkaForwarder.prototype.getConnectionString = function() {
	return this.config.monitoring.host + ':' + this.config.monitoring.port;
};

/**
 * sub.topics,
 * sub.port,
 * sub.host
 */

KafkaForwarder.prototype.subscribe = function(sub) {
	var self = this;
	var client = new kafka.Client(this.getConnectionString(), 'kafka-node-client');
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

	consumer.on("message", function(msg) {
                console.log("Send message " + msg + " to subscribed client " + sub.host + ":" + sub.port);
		self.ou_socket.send(
		 	new Buffer(msg), 
		 	0, 
		 	msg.length, 
		 	sub.port,
		 	sub.host, 
		 	function(err) {
		 		if (err) console.log(err);
		 	}
		);
	});
};

module.exports = KafkaForwarder;
