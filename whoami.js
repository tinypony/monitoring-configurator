var dgram = require('dgram');
var Netmask = require('netmask').Netmask;
var uuid = require('node-uuid');
var exec = require('child_process').exec;

var port = 45025;

var NetworkConfigurator = function(subnet) {
	this.subnet = subnet;
	this.id = "" + uuid.v4();
};


NetworkConfigurator.prototype.getBroadcastAddress = function() {
	var block = new Netmask(this.subnet);
	return block.broadcast;
};

NetworkConfigurator.prototype.initNetwork = function() {
	var temp_socket = dgram.createSocket('udp4');
	temp_socket.bind(port, '0.0.0.0');

	temp_socket.on('message', function(data, rinfo) {
		var msg = data.toString();

		if(msg === this.id) {
			var address = rinfo.address;
			console.log('address resolved = ' + address);
			
			exec('./lifecycle/on_address_resolved.sh ' + this.address,
				function (error, stdout, stderr) {	
				    if (error !== null) {
				      console.log('exec error: ' + error);
				    }
				    temp_socket.close();
				});
		}
	}.bind(this) );

	temp_socket.on('close', function() {
		console.log('Done');
	});

	temp_socket.on('listening', function() {
		temp_socket.setBroadcast(true);

		temp_socket.send(
			new Buffer(this.id),
			0, 
			this.id.length, 
			port,
			this.getBroadcastAddress(), 
			function (err) {
				if (err) { 
					console.log(err);
					//TODO need to exit properly
				}
			}
		);
	}.bind(this));
};

if(!process.argv[2]) {
	console.error('You must pass subnet in CIDR notation as parameter: node whoami.js 10.0.0.0/16');
	process.exit();
}

var instance = new NetworkConfigurator(process.argv[2]);
instance.initNetwork();

module.exports = NetworkConfigurator;
