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

    var killTree = true;
    
    if(killTree) {
        psTree(pid, function (err, children) {
            [pid].concat(
                children.map(function (p) {
                    return p.PID;
                })
            ).forEach(function (tpid) {
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

class Forwarder {
	constructor(config, usePython = true) {
		this.id = uuid.v4();
		this.debug = true;
		this.use_python = usePython;
		this.config = config;

		this.logger = new winston.Logger({
			transports: [new winston.transports.Console()]
		});

		if( config.logging && config.logging.disable ) {
			this.logger.remove(winston.transports.Console);
		}

		if(!this.use_python) {

			/**
			 * fwd.port,
			 * fwd.topic
			 */
			this.forward_ports = _.map(config.producers, fwd => {
				this.logger.info("Forwarding configuration = %d => %s", fwd.port, fwd.topic);
				var skt = dgram.createSocket('udp4');
				skt.bind(fwd.port, '127.0.0.1');

				skt.on('error', er => {
					this.logger.warn(`[Forwarder.constructor()] ${er}`);
				});

				skt.on("message", this.forward.bind(this, fwd.topic));
				return skt;
			});
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

	createConnection() {
		var defer = q.defer();
		var connectionString = this.getZK();
		this.logger.info('Create zookeeper connection to %s', connectionString);
		var client = new Client(connectionString, this.id);
		var producer = new HighLevelProducer(client);
		
		producer.on('ready', () => {
			this.logger.info('Forwader is ready');
			this.producer = producer;
			this.client = client;
			defer.resolve();
		});

		producer.on('error', err => {
			this.logger.warn('[Forwarder.reconfig()] Error: %s', JSON.stringify(err));
			defer.reject(err);
		});

		this.logger.info('[Forwarder] Created new producer');
		return defer.promise;
	}

	reconnect() {
		var defer = q.defer();
		if(!this.use_python) {
			if (this.producer) {
				this.producer.close(() => {
					this.logger.info('[Forwarder.reconnect()] Closed the producer, reconnecting');
					this.producer = null;
					this.createConnection()
						.then(defer.resolve, err => defer.reject(err));
				});
			} else {
				this.createConnection()
					.then(defer.resolve, err => defer.reject(err));
			}
		} else {
			this.spawn_subprocess().done(defer.resolve);
		}

		return defer.promise;
	}

	run_daemon() {
		var defer = q.defer();
		let bindings = _.map(this.config.producers, fwd => `${fwd.port}:${fwd.topic}`);
		this.python_subprocess = exec(`python /opt/monitoring-configurator/python/forwarder/daemon.py --bindings ${bindings.join(' ')} --zk ${this.getZK()}`);
		this.python_subprocess.stdout.on('data', data => {
		    this.logger.info('stdout: ' + data);
		});
		this.python_subprocess.stderr.on('data', data => {
		    this.logger.warn('stderr: ' + data);
		});
		defer.resolve();
		return defer.promise;
	}

	//(re)spawns python daemon that takes care of forwarding message from local ports to datasink
	spawn_subprocess() {
		if(this.python_subprocess) { //take down existing process
			var defer = q.defer();

			kill(this.python_subprocess.id, 'SIGKILL', () => {
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

	forward(topic, data) {
		var msgStr = data.toString();
	    var messages = msgStr.split('\n');

		messages = _.map(messages, (m) => {
			var val = m.replace(/\r$/g, '');
			return val;
		});
		
		if(!this.forwardToPort || !this.forwardToAddress || !this.producer) {
			return ;
		}
			
		//contain possible errors if datasink is temporarily down
		try {
			this.producer.send([{
				topic: topic,
				messages: messages
			}], err => {
				if(err) {
					return this.logger.warn(`[Forwarder.forward()] ${JSON.stringify(err)}`);
				}
				if(this.debug) {
					this.logger.info(`Forwarded ${messages}`);
					this.debug = false;
				}
			});
		} catch(e) {
			this.logger.warn(e); //carry on
		}
	}
}

export default Forwarder;
