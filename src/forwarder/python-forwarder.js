import dgram from 'dgram';
import _ from 'underscore';
import q from 'q';
import {Client, HighLevelProducer} from 'kafka-node';
import uuid from 'node-uuid';
import winston from 'winston';
import psTree from 'ps-tree';
import { exec } from 'child_process';

function isValidPort(port) {
	return _.isNumber(port) && port > 0 && port < 65535;
}

function kill(pid, signal, callback) {
    signal   = signal || 'SIGKILL';
    callback = callback || function () {};

    var killTree = false;
    
    if(killTree) {
        psTree(pid, function (err, children) {
            [pid].concat(
                children.map(p => p.PID)
            ).forEach(tpid => {
                try { process.kill(tpid, signal) }
                catch (ex) { }
            });
            callback();
        });
    } else {
        try { process.kill(pid, signal) }
        catch (ex) { }
        callback();
    }
};

class PythonForwarder {
	constructor(config) {
		this.id = uuid.v4();
		this.debug = true;
		this.config = config;

		this.logger = new winston.Logger({
			transports: [new winston.transports.Console()]
		});

		if( config.logging && config.logging.disable ) {
			this.logger.remove(winston.transports.Console);
		}

	}
	
	reconfig(config) {
		if(!isValidPort(config.monitoring.port)) {
			this.logger.info('trying to configure forwarder with an invalid port');
			return;
		}
		this.config = config;
		this.forwardToAddress = config.monitoring.host;
		this.forwardToPort = config.monitoring.port;
		this.logger.info('[Forwarder.reconfig()] Reconfiguring forwarder');
		this.reconnect();
	}

	getZK() {
		return this.forwardToAddress + ':' + this.forwardToPort;
	}

	reconnect() {
		var defer = q.defer();
		this.logger.info('[Forwarder.reconnect()] Using python forwarder');
		this.spawn_subprocess().done(defer.resolve);
		return defer.promise;
	}

	run_daemon() {
		var defer = q.defer();
		let bindings = _.map(this.config.producers, fwd => `${fwd.port}:${fwd.topic}`);
		this.logger.info(`Run python /opt/monitoring-configurator/python/forwarder/daemon.py --bindings ${bindings.join(' ')} --zk ${this.getZK()}`);
		this.python_subprocess = exec(`python /opt/monitoring-configurator/python/forwarder/daemon.py --bindings ${bindings.join(' ')} --zk ${this.getZK()}`);

		this.logger.log('Started python daemon');
		defer.resolve();
		return defer.promise;
	}

	//(re)spawns python daemon that takes care of forwarding message from local ports to datasink
	spawn_subprocess() {
		if(this.python_subprocess) { //take down existing process
			var defer = q.defer();

			this.logger.info(`Killing python subprocess id=${this.python_subprocess.pid}, parent id=${process.pid}`);
			kill(this.python_subprocess.pid, 'SIGKILL', () => {
				this.python_subprocess = null;
				this.run_daemon();
				this.logger.info('Python subprocess restarted');
				defer.resolve();
			});

			return defer.promise;
		} else {
			this.logger.info('Python subprocess is starting up');
			return this.run_daemon();
		}
	}
}

export default PythonForwarder;
