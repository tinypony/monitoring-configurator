var NODE_TYPE = require('./node-type.js');
var Netmask = require('netmask');

module.exports = {
	broadcast: {
 		getAddress: function() {
 			var block = new Netmask(this.monitoring.subnet);
 			return block.broadcast;
 		},
		port: 12555
	}, 
	unicast: {
		port: 12556
	},
	node_type: NODE_TYPE.MANAGER, //available types 'manager' and 'client'
	monitoring: {
		subnet: '192.168.1.0/24',
		port: 8070,
		keystone: '192.168.1.83'
	}
}