var dgram = require('dgram');
var _ = require('underscore');
var Netmask = require('netmask').Netmask;
var NODE_TYPE = require('./node-type.js');
var Forwarder = require('./forwarder/forwarder.js');
var KafkaForwarder = require('./kafka/kafka-forwarder.js');
var uuid = require('node-uuid');

var ConfigurationDaemon = function(config, broadcastPort) {
	this.config = config;
	this.address = null;
	this.initId = uuid.v4();

	if(this.isProducer()) {
		this.initProducer(config);
	}

	if(this.isDatasink()) {
		this.initDatasink(config);
	}

	if(this.isConsumer()) {
		this.initConsumer(config);
	}
	
	this.broadcastPort = broadcastPort;
	this.bc_socket = dgram.createSocket('udp4');
	this.uc_socket = dgram.createSocket('udp4');

	//Attach message handlers
	this.bc_socket.on('message', this.getMessageHandler(true).bind(this));
	this.uc_socket.on('message', this.getMessageHandler(false).bind(this));

	//bind sockets and attach on listen
	this.bc_socket.bind(this.broadcastPort, '0.0.0.0');
	this.uc_socket.bind(this.config.unicast.port, '0.0.0.0');
	this.bc_socket.on('listening', this.onStartListening.bind(this));
};

ConfigurationDaemon.prototype.initDatasink = function(config) {
	this.kafkaForwarder = new KafkaForwarder(config);
};

ConfigurationDaemon.prototype.initConsumer = function(config) {

};

ConfigurationDaemon.prototype.initProducer = function(config) {
	this.forwarder = new Forwarder(config);
};

ConfigurationDaemon.prototype.onStartListening = function() {
	this.bc_socket.setBroadcast(true);
	var message;

	//send message on start depending on node type
	if ( this.isDatasink() ) {
		message = this.getReconfigureMessage();
	} else if ( this.isProducer() || this.isConsumer() ) {
		message = this.getHelloMessage();
	} else {
		return;
	}

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

ConfigurationDaemon.prototype.isConsumer = function() {
	return _.contains(this.config.roles, NODE_TYPE.CONSUMER);
};

ConfigurationDaemon.prototype.handleHello = function(msg) {
	if(this.initId && msg.uuid === this.initId) {
		this.address = msg.host;
		this.initId = null
	}

	if( this.isDatasink() ) {
		var configMessage = this.getConfigureMessage();

		this.uc_socket.send(
			new Buffer(configMessage),
			0,
			configMessage.length,
			msg.port,
			msg.host
		);
	}
};

ConfigurationDaemon.prototype.handleSubscribe = function(msg) {
	if( this.isDatasink() ) {
		var self = this;
		console.log('let\'s subscribe');

		_.each(msg.endpoints, function(ep) {
			console.log("wire " + ep.topics.join(",") + " to " + msg.host + ":" + ep.port);
			ep.host = msg.host;
			ep.unicastport = msg.port;
			self.kafkaForwarder.subscribe(ep);
		});
	}
};

ConfigurationDaemon.prototype.handleBroadcastMessage = function(msg) {
	if(msg.type === 'hello') {
		this.handleHello(msg);
	}

	//Every type of node is being monitored and needs to be reconfigured
	if( msg.type === 'reconfig' && (this.isProducer() || this.isConsumer()) ) {
		this.configureClient(msg);
	}
};

function isValidPort(port) {
	return _.isNumber(port) && port > 0 && port < 65535;
}

ConfigurationDaemon.prototype.configureClient = function(msg) {
	if( this.isProducer() || this.isConsumer() ) {
		console.log('configure client with ' + JSON.stringify(msg));
	 	this.config.monitoring = _.extend(this.config.monitoring, msg.monitoring);
			
		if(this.isProducer()) {
			this.forwarder.reconfig(this.config);
		}

		if(this.isConsumer()) {
			if(!isValidPort(msg.port)) {
				console.log('trying to send subscription message to an invalid port');
				return;
			}
			var subscribeMsg = this.getSubscribeMessage();

			this.uc_socket.send(
				new Buffer(subscribeMsg),
				0,
				subscribeMsg.length,
				msg.port,
				msg.host, 
				function(e) {
					if(e) {
						console.log(JSON.stringify(e));
						return;
					}

					console.log('Sent subscribe request to ' + msg.host + ":" + msg.port);
				}
			);
		}
	}
};

//Client node is provided with configuration by a manager node
ConfigurationDaemon.prototype.handleUnicastMessage = function(msg) {
	if( msg.type === 'config' ) {
		this.configureClient(msg);
	}

	if( msg.type === 'subscribe' ) {
		this.handleSubscribe(msg);
	}
};

ConfigurationDaemon.prototype.preprocessMessage = function(msg, rinfo) {
	if(msg.monitoring) {
		msg.monitoring.host = rinfo.address;
	}

	if(msg.host) {
		msg.host = rinfo.address;
	}

	return msg;
};

ConfigurationDaemon.prototype.getMessageHandler = function(isBroadcast) {
	return function( data, rinfo) {
		var dataString = data.toString();

		try {
			var msg JSON.parse(dataString);
			msg = this.preprocessMessage( msg, rinfo );
			
			if(isBroadcast)
				this.handleBroadcastMessage(msg);
			else 
				this.handleUnicastMessage(msg);
		} catch(e) {
			//silent skip
			console.log("Could not parse incoming data, probably malformed");
		}
	}
};

ConfigurationDaemon.prototype.getReconfigureMessage = function() {
	var msg = {
		type: 'reconfig',
		host: 'self',
		port: this.config.unicast.port,
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
		host: 'self',
		port: this.config.unicast.port,
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
		uuid: this.initId,
		host: 'self',
		port: this.config.unicast.port
	};

	return JSON.stringify(msg);
};

ConfigurationDaemon.prototype.getSubscribeMessage = function() {
	var msg = {
		type: 'subscribe',
		host: 'self',
		port: this.config.unicast.port,
		endpoints: this.config.consumers
	}

	return JSON.stringify(msg);
};


ConfigurationDaemon.prototype.getPongMessage = function() {
	var msg = {
		type: 'pong',
		host: 'self',
		port: this.config.unicast.port
	}

	return JSON.stringify(msg);
};

module.exports = ConfigurationDaemon;
