
var fs = require('fs');
var path = require('path');
var ConfigDaemon = require('./daemon.js');


var config;
var broadcastPort = 12555 // Hadr coded for now, must be same for all clients

if(!process.argv[2]) {
	console.error('You must pass config file as parameter: node app.js <path/to/config>');
	process.exit();
}

var configPath = path.join(__dirname, process.argv[2]);
fs.readFile(configPath, {encoding: 'utf-8'}, function(err, data) {
	if(err) {
		console.error('Could not read configuration file');
		process.exit();
	}
	try {
		config = JSON.parse(data);
		daemon = new ConfigDaemon(config, broadcastPort);
	} catch(e) {
		console.error("Could not parse configuration file, not valid JSON?");
		process.exit();
	}
});