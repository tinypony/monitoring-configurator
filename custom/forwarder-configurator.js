var _ = require('underscore');

// Runs whenever configuration / reconfiguration of client is requested
module.exports = {
	configure: function(config, receivedConfiguration) {
		//{"monitoring":{"host":"10.10.10.1","port":8070,"keystone":"192.168.1.83"}
		config.monitoring = _.extend(config.monitoring, receivedConfiguration.monitoring);
		return config;
	}
};