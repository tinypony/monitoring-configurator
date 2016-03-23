var dgram = require('dgram');

var Forwarder = function(config) {
	this.in_socket = dgram.createSocket('udp4');
	this.ou_socket = dgram.createSocket('udp4');

	this.in_socket.bind(config.forwarder.inport, '127.0.0.1');
	this.in_socket.on("message", this.forward.bind(this));
	this.ou_socket.bind(config.forwarder.outport,   '0.0.0.0');

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

	this.ou_socket.send(
		new Buffer(msgStr), 
		0, 
		msgStr.length, 
		this.forwardToPort,
		this.forwardToAddress, 
		function (err) {
			if (err) console.log(err);
		}
	);
};

module.exports = Forwarder;