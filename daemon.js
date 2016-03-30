var dgram = require('dgram');
var _ = require('underscore');
var Netmask = require('netmask').Netmask;
var NODE_TYPE = require('./node-type.js');
var Forwarder = require('./forwarder/forwarder.js');


var ConfigurationDaemon = function(config, broadcastPort) {
	this.config = config;

	if(this.isProducer()) {
		this.forwarder = new Forwarder(config);
	}

	this.broadcastPort = broadcastPort;
	this.bc_socket = dgram.createSocket('udp4');
	this.uc_socket = dgram.createSocket('udp4');

	this.bc_socket.bind(this.broadcastPort, '0.0.0.0');
	this.uc_socket.bind(this.config.unicast.port,   '0.0.0.0');

	this.bc_socket.on('listening', this.onStartListening.bind(this));
	//Broadcast message handler
	this.bc_socket.on('message', this.getMessageHandler(true).bind(this));
	this.uc_socket.on('message', this.getMessageHandler(false).bind(this));
};

ConfigurationDaemon.prototype.onStartListening = function() {
	this.bc_socket.setBroadcast(true);
	var message;

	//send message on start depending on node type
	if ( this.isDatasink() ) 		message = this.getReconfigureMessage();
	else if ( this.isProducer() ) 	message = this.getHelloMessage();
	else							return;

	this.bc_socket.send(
		new Buffer(message), 
		0, 
		message.length, 
		this.broadcastPort,
		this.getBroadcastAddress(), 
		function (err) {
			if (err) console.log(err);
		}
	);
};

ConfigurationDaemon.prototype.getBroadcastAddress = function() {
	var block = new Netmask(this.config.monitoring.subnet);
	return block.broadcast;
};

ConfigurationDaemon.prototype.isDatasink = function() {
	return _.contains(this.config.roles, NODE_TYPE.DATASINK);
};

ConfigurationDaemon.prototype.isProducer = function() {
	return _.contains(this.config.roles, NODE_TYPE.PRODUCER);
};

ConfigurationDaemon.prototype.handleBroadcastMessage = function(msg, rinfo) {
	if(this.isDatasink() && msg.type === "hello") {
		var configMessage = this.getConfigureMessage();

		this.uc_socket.send(
			new Buffer(configMessage),
			0,
			configMessage.length,
			msg.port,
			rinfo.address
		);
	}

	//Every type of node is being monitored and needs to be reconfigured
	if(msg.type === 'reconfig' && this.isProducer()) {
		console.log('reconfig');
		this.config.monitoring = _.extend(this.config.monitoring, msg.monitoring);
		this.forwarder.reconfig(this.config);
	}
};

//Client node is provided with configuration by a manager node
ConfigurationDaemon.prototype.handleUnicastMessage = function(msg, rinfo) {
	if(this.isProducer() && msg.type === 'config') {
		console.log('configure');
		console.log(JSON.stringify(msg));
	}
};

ConfigurationDaemon.prototype.getMessageHandler = function(isBroadcast) {
	return function( data, rinfo) {
		var msgStr = data.toString();

		try {
			var msg = JSON.parse(msgStr);
			if(msg.monitoring && msg.monitoring.host === 'self') {
				msg.monitoring.host = rinfo.address;
			}

			if(isBroadcast)
				this.handleBroadcastMessage(msg, rinfo);
			else 
				this.handleUnicastMessage(msg, rinfo);
		} catch(e) {
			//silent skip
		}
	}
};

ConfigurationDaemon.prototype.getReconfigureMessage = function() {
	var msg = {
		type: 'reconfig',
		monitoring: {
			host: 'self',
			port: this.config.monitoring.port,
			keystone: this.config.monitoring.keystone
		}
	};

	return JSON.stringify(msg);
};

ConfigurationDaemon.prototype.getConfigureMessage = function() {
	var msg = {
		type: 'config',
		monitoring: {
			host: 'self',
			port: this.config.monitoring.port,
			keystone: this.config.monitoring.keystone
		}
	};

	return JSON.stringify(msg);	
};

ConfigurationDaemon.prototype.getHelloMessage = function() {
	var msg = {
		type: 'hello',
		port: this.config.unicast.port
	};

	return JSON.stringify(msg);
};

module.exports = ConfigurationDaemon;
