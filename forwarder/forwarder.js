var dgram = require('dgram');
var _ = require('underscore');
var kafka = require('kafka-node');
var HighLevelProducer = kafka.HighLevelProducer;

var Forwarder = function(config) {
	this.ou_socket = dgram.createSocket('udp4');
	var self = this;

	/**
	 * fwd.port,
	 * fwd.topic
	 */
	this.forward_ports = _.map(config.producers, function(fwd) {
		console.log("Forwarding configuration = " + fwd.port + "=>" + fwd.topic);
		var skt = dgram.createSocket('udp4');
		skt.bind(fwd.port, '127.0.0.1');
		skt.on("message", self.forward.bind(self, fwd.topic));
		return skt;
	});
};

Forwarder.prototype.reconfig = function(config) {
	this.forwardToAddress = config.monitoring.host;
	this.forwardToPort = config.monitoring.port;

	function createConnection() {
		var connectionString = this.forwardToAddress + ':' + this.forwardToPort;
		this.client = new kafka.Client(connectionString, 'forwarder-client');
		this.producer = new HighLevelProducer(this.client);
	}

	if (this.client) {
		this.producer = null;
		this.client.close(createConnection.bind(this));
	} else {
		createConnection.call(this);
	}	
};

Forwarder.prototype.forward = function(topic, data) {
	var msgStr = data.toString();
  	console.log('fwd:'+msgStr);	
	if(!this.forwardToPort || !this.forwardToAddress || !this.producer) {
		return ;
	}

	console.log('Forward ' + msgStr.split('\n').length);
		
	//contain possible errors if datasink is temporarily down
	try {
		this.producer.send([{
			topic: topic,
			messages: msgStr.split('\n')
		}], function(err, sent_data) {

		});
	} catch(e) {
		console.log(e); //carry on
	}
};

module.exports = Forwarder;
