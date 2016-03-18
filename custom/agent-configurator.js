var fs = require('fs');
var sys = require('sys')
var exec = require('child_process').exec;

// Runs whenever configuration / reconfiguration of client is requested
module.exports = {
	configure: function(config, receivedConfiguration) {
		console.log('lets do this');
		//{"monitoring":{"host":"self","port":8070,"keystone":"192.168.1.83"}
		fs.readFile('./custom/agent.yaml.template', 'utf-8', function(err, data) {
			var agentConfigFileContent = data
											.replace(/\{\{keystone_ip\}\}/g, receivedConfiguration.monitoring.keystone)
											.replace(/\{\{monasca_api_ip\}\}/g, receivedConfiguration.monitoring.host)
											.replace(/\{\{monasca_api_port\}\}/g, receivedConfiguration.monitoring.port);
			//var targetFd = fs.openSync('./agent.yaml', 'w');

			fs.writeFile('./agent.yaml', agentConfigFileContent, {
				encoding: 'utf-8',
				flag: 'w'
			}, function(err) {
				function puts(error, stdout, stderr) { sys.puts(stdout) }
				exec(config.monitoring.agentRestartCommand, puts);
			});
		});
	}
};