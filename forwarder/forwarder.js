var dgram = require('dgram');
var _ = require('underscore');

var Forwarder = function(config) {
	this.ou_socket = dgram.createSocket('udp4');
	var self = this;

	this.forward_ports = _.map(config.producers, function(fwd) {
		var skt = dgram.createSocket('udp4');
		skt.bind(fwd.port, '127.0.0.1');
		skt.on("message", self.forward.bind(self));
		return skt;
	});

	this.forwardToAddress = config.monitoring.host;
	this.forwardToPort = config.monitoring.port;
};

Forwarder.prototype.reconfig = function(config) {
	this.forwardToAddress = config.monitoring.host;
	this.forwardToPort = config.monitoring.port;
};

Forwarder.prototype.forward = function(data, rinfo) {
	var msgStr = data.toString();
	
	if(!this.forwardToPort || !this.forwardToAddress) {
		return ;
	}

	console.log('Forward '+msgStr.length);

	// this.ou_socket.send(
	// 	new Buffer(msgStr), 
	// 	0, 
	// 	msgStr.length, 
	// 	this.forwardToPort,
	// 	this.forwardToAddress, 
	// 	function (err) {
	// 		if (err) console.log(err);
	// 	}
	// );
};

module.exports = Forwarder;