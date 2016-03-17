var dgram = require('dgram');
var config = require('./config.js');
var bc_socket = dgram.createSocket('udp4');
var uc_socket = dgram.createSocket('udp4');

var NODE_TYPE = require('./node-type.js');
var configurator_port = 12556;

bc_socket.bind(config.broadcast.port, '0.0.0.0');
uc_socket.bind(config.unicast.port,   '0.0.0.0');

//On start listener.
bc_socket.on('listening', function() {
	bc_socket.setBroadcast(true);
	var message;

	//send message on start depending on node type
	if ( isManager() ) 		message = getReconfigureMessage();
	else if ( isClient() ) 	message = getHelloMessage();
	else					return;


	bc_socket.send(
		new Buffer(message), 
		0, 
		message.length, 
		config.broadcast.port,
		config.broadcast.addr, 
		function (err) {
			if (err) console.log(err);
		}
	);
});

function isManager() {
	return config.node_type === NODE_TYPE.MANAGER;
}

function isClient() {
	return config.node_type === NODE_TYPE.CLIENT;
} 

function handleBroadcastMessage(msg) {
	if(isManager() && msg.type === 'hello') {
		var configMessage = getConfigureMessage();
		
		uc_socket.send(
			new Buffer(configMessage),
			0,
			configMessage.length,
			msg.port,
			rinfo.address
		);
	}

	if(isClient() && msg.type === 'reconfig') {
		console.log('reconfigure!!');
		console.log(JSON.stringify(msg));
	}
}

function handleUnicastMessage(msg) {
	if(isClient() && msg.type === 'config') {
		console.log('configure');
		console.log(JSON.stringify(msg));
	}
}

function getMessageHandler(isBroadcast) {
	return function( data, rinfo) {
		var msgStr = data.toString();
	
		try {
			var msg = JSON.parse(msgStr);
			if(isBroadcast)
				handleBroadcastMessage(msg);
			else 
				handleUnicastMessage(msg);
		} catch(e) {
			//silent skip
		}
	}
}

//Broadcast message handler
bc_socket.on('message', getMessageHandler(true));
uc_socket.on('message', getMessageHandler(false));

function getReconfigureMessage() {
	var msg = {
		type: 'reconfig',
		monitoring: {
			host: 'self',
			port: config.monitoring.port,
			keystone: config.monitoring.keystone
		}
	};

	return JSON.stringify(msg);
}

function getConfigureMessage() {
	var msg = {
		type: 'config',
		monitoring: {
			host: 'self',
			port: config.monitoring.port,
			keystone: config.monitoring.keystone
		}
	};

	return JSON.stringify(msg);	
}

function getHelloMessage() {
	var msg = {
		type: 'hello',
		port: config.unicast.port
	};

	return JSON.stringify(msg);
}