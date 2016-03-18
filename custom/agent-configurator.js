var fs = require('fs');
var sys = require('sys')
var exec = require('child_process').exec;
var sprintf = require("sprintf-js").sprintf;

// Runs whenever configuration / reconfiguration of client is requested
module.exports = {
	configure: function(config, receivedConfiguration) {
		//{"monitoring":{"host":"self","port":8070,"keystone":"192.168.1.83"}

		var exeString = sprintf("monasca-setup -u %s -p %s --keystone_url http://%s:5000/v3 --monasca_url http://%s:%s/v2.0 " +
			"--system_only --log_level INFO --hostname virtualbox.tinypony.comptel.com --check_frequency 100",
		
			config.monitoring.username, 
			config.monitoring.password, 
			receivedConfiguration.monitoring.keystone, 
			receivedConfiguration.monitoring.host, 
			receivedConfiguration.monitoring.port
		);

		function puts(error, stdout, stderr) { sys.puts(stdout); sys.puts(stderr); }
 		exec(exeString, puts);							

		// fs.readFile('./custom/agent.yaml.template', 'utf-8', function(err, data) {
		// 	var agentConfigFileContent = data
		// 									.replace(/\{\{keystone_ip\}\}/g, receivedConfiguration.monitoring.keystone)
		// 									.replace(/\{\{monasca_api_ip\}\}/g, receivedConfiguration.monitoring.host)
		// 									.replace(/\{\{monasca_api_port\}\}/g, receivedConfiguration.monitoring.port);



		// 	console.log(agentConfigFileContent);

		// 	fs.writeFile(config.monitoring.agentConfigurationTarget, agentConfigFileContent, {
		// 		encoding: 'utf-8',
		// 		flag: 'w'
		// 	}, function(err) {
		// 		if(err) { 	console.error( JSON.stringify(err) ); 	}
		// 		else { 		console.log('WRITTEN'); 				}

		// 		function puts(error, stdout, stderr) { /*sys.puts(stdout)*/ }
		// 		exec(config.monitoring.agentRestartCommand, puts);
		// 	});
		// });
	}
};