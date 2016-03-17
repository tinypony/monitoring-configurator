var NODE_TYPE = require('./node-type.js');

module.exports = {
	broadcast: {
 		addr: '192.168.1.255',
		port: 12555
	}, 
	unicast: {
		port: 12556
	},
	node_type: NODE_TYPE.MANAGER, //available types 'manager' and 'client'
	monitoring: {
		port: 8070,
		keystone: '192.168.1.83'
	}
}