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
		skt.on('error', function(er) {
			console.log(er);
		});
		skt.on("message", self.forward.bind(self, fwd.topic));
		return skt;
	});
};

Forwarder.prototype.reconfig = function(config) {
	this.forwardToAddress = config.monitoring.host;
	this.forwardToPort = config.monitoring.port;

	function createConnection() {
		var self = this;
		var connectionString = this.forwardToAddress + ':' + this.forwardToPort;
		console.log('Create zookeeper connection to ' + connectionString);

		var client = new kafka.Client(connectionString, 'hey lalala');
		var producer = new HighLevelProducer(client);
		
		producer.on('ready', function() {
			console.log('producer is ready');
			self.producer = producer;
		});
		producer.on('error', function(err) {
			console.log('What da fuck????');
			console.log(JSON.stringify(err));
		});
		
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
        var messages = msgStr.split('\n');
	messages = _.map(messages, function(m){
		var val = m.replace(/\r$/g, '');///.replace(/["']/g, '');
		return val;
	});
	
	if(!this.forwardToPort || !this.forwardToAddress || !this.producer) {
		return ;
	}
	console.log('invoke formwar');
	//console.log('Forward ' + msgStr.split('\n').lenth);
		
	//contain possible errors if datasink is temporarily down
	try {
		this.producer.send([{
			topic: topic,
			messages: messages
		}], function(err, sent_data) {
			if(err) {
				return console.log(JSON.stringify(err));
			}
			console.log(sent_data);
			
		});
	} catch(e) {
		console.log(e); //carry on
	}
};

module.exports = Forwarder;
