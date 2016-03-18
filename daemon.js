var dgram = require('dgram');
var Netmask = require('netmask').Netmask;
var NODE_TYPE = require('./node-type.js');

var bc_socket = dgram.createSocket('udp4');
var uc_socket = dgram.createSocket('udp4');


var ConfigurationDaemon = function(config, broadcastPort) {
	this.config = config;
	this.agentConfigurator = require('./custom/agent-configurator.js');
	
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
	if ( this.isManager() ) 		message = this.getReconfigureMessage();
	else if ( this.isClient() ) 	message = this.getHelloMessage();
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

ConfigurationDaemon.prototype.isManager = function() {
	return this.config.node_type === NODE_TYPE.MANAGER;
};

ConfigurationDaemon.prototype.isClient = function() {
	return this.config.node_type === NODE_TYPE.CLIENT;
};

ConfigurationDaemon.prototype.handleBroadcastMessage = function(msg, rinfo) {
	if(this.isManager() && msg.type === "hello") {
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
	if(msg.type === 'reconfig') {
		console.log('reconfigure with ' + this.config.monitoring.agentConfigurator);
		this.agentConfigurator.configure(this.config, msg);
	}
};

//Client node is provided with configuration by a manager node
ConfigurationDaemon.prototype.handleUnicastMessage = function(msg, rinfo) {
	if(this.isClient() && msg.type === 'config') {
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