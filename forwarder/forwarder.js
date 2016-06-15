'use strict';

Object.defineProperty(exports, "__esModule", {
	value: true
});

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

var _dgram = require('dgram');

var _dgram2 = _interopRequireDefault(_dgram);

var _underscore = require('underscore');

var _underscore2 = _interopRequireDefault(_underscore);

var _q = require('q');

var _q2 = _interopRequireDefault(_q);

var _kafkaNode = require('kafka-node');

var _nodeUuid = require('node-uuid');

var _nodeUuid2 = _interopRequireDefault(_nodeUuid);

var _winston = require('winston');

var _winston2 = _interopRequireDefault(_winston);

var _psTree = require('ps-tree');

var _psTree2 = _interopRequireDefault(_psTree);

var _child_process = require('child_process');

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function isValidPort(port) {
	return _underscore2.default.isNumber(port) && port > 0 && port < 65535;
}

function kill(pid, signal, callback) {
	signal = signal || 'SIGKILL';
	callback = callback || function () {};

	var killTree = false;

	if (killTree) {
		(0, _psTree2.default)(pid, function (err, children) {
			[pid].concat(children.map(function (p) {
				return p.PID;
			})).forEach(function (tpid) {
				try {
					process.kill(tpid, signal);
				} catch (ex) {}
			});
			callback();
		});
	} else {
		try {
			process.kill(pid, signal);
		} catch (ex) {}
		callback();
	}
};

var Forwarder = function () {
	function Forwarder(config) {
		var _this = this;

		var usePython = arguments.length <= 1 || arguments[1] === undefined ? false : arguments[1];

		_classCallCheck(this, Forwarder);

		this.id = _nodeUuid2.default.v4();
		this.debug = true;
		this.use_python = usePython;
		this.config = config;

		this.logger = new _winston2.default.Logger({
			transports: [new _winston2.default.transports.Console()]
		});

		if (config.logging && config.logging.disable) {
			this.logger.remove(_winston2.default.transports.Console);
		}

		if (!this.use_python) {

			/**
    * fwd.port,
    * fwd.topic
    */
			this.forward_ports = _underscore2.default.map(config.producers, function (fwd) {
				_this.logger.info("Forwarding configuration = %d => %s", fwd.port, fwd.topic);
				var skt = _dgram2.default.createSocket('udp4');
				skt.bind(fwd.port, '127.0.0.1');

				skt.on('error', function (er) {
					_this.logger.warn('[Forwarder.constructor()] ' + er);
				});

				skt.on("message", _this.forward.bind(_this, fwd.topic));
				return skt;
			});
		}
	}

	_createClass(Forwarder, [{
		key: 'reconfig',
		value: function reconfig(config) {
			if (!isValidPort(config.monitoring.port)) {
				this.logger.info('trying to configure forwarder with an invalid port');
				return;
			}
			this.config = config;
			this.forwardToAddress = config.monitoring.host;
			this.forwardToPort = config.monitoring.port;
			this.logger.info('[Forwarder.reconfig()] Reconfiguring forwarder');
			this.reconnect();
		}
	}, {
		key: 'getZK',
		value: function getZK() {
			return this.forwardToAddress + ':' + this.forwardToPort;
		}
	}, {
		key: 'createConnection',
		value: function createConnection() {
			var _this2 = this;

			var defer = _q2.default.defer();
			var connectionString = this.getZK();
			this.logger.info('Create zookeeper connection to %s', connectionString);
			var client = new _kafkaNode.Client(connectionString, this.id);
			var producer = new _kafkaNode.HighLevelProducer(client);

			producer.on('ready', function () {
				_this2.logger.info('Forwader is ready');
				_this2.producer = producer;
				_this2.client = client;
				defer.resolve();
			});

			producer.on('error', function (err) {
				_this2.logger.warn('[Forwarder.reconfig()] Error: %s', JSON.stringify(err));
				defer.reject(err);
			});

			this.logger.info('[Forwarder] Created new producer');
			return defer.promise;
		}
	}, {
		key: 'reconnect',
		value: function reconnect() {
			var _this3 = this;

			var defer = _q2.default.defer();

			if (!this.use_python) {
				if (this.producer) {
					this.producer.close(function () {
						_this3.logger.info('[Forwarder.reconnect()] Closed the producer, reconnecting');
						_this3.producer = null;
						_this3.createConnection().then(defer.resolve, function (err) {
							return defer.reject(err);
						});
					});
				} else {
					this.createConnection().then(defer.resolve, function (err) {
						return defer.reject(err);
					});
				}
			} else {
				this.logger.info('[Forwarder.reconnect()] Using python forwarder');
				this.spawn_subprocess().done(defer.resolve);
			}

			return defer.promise;
		}
	}, {
		key: 'run_daemon',
		value: function run_daemon() {
			var defer = _q2.default.defer();
			var bindings = _underscore2.default.map(this.config.producers, function (fwd) {
				return fwd.port + ':' + fwd.topic;
			});
			this.logger.info('Run python /opt/monitoring-configurator/python/forwarder/daemon.py --bindings ' + bindings.join(' ') + ' --zk ' + this.getZK());
			this.python_subprocess = (0, _child_process.exec)('python /opt/monitoring-configurator/python/forwarder/daemon.py --bindings ' + bindings.join(' ') + ' --zk ' + this.getZK());

			this.logger.log('Started python daemon');
			defer.resolve();
			return defer.promise;
		}

		//(re)spawns python daemon that takes care of forwarding message from local ports to datasink

	}, {
		key: 'spawn_subprocess',
		value: function spawn_subprocess() {
			var _this4 = this;

			if (this.python_subprocess) {
				//take down existing process
				var defer = _q2.default.defer();

				this.logger.info('Killing python subprocess id=' + this.python_subprocess.pid + ', parent id=' + process.pid);
				kill(this.python_subprocess.pid, 'SIGKILL', function () {
					_this4.python_subprocess = null;
					_this4.run_daemon();
					_this4.logger.info('Python subprocess restarted');
					defer.resolve();
				});

				return defer.promise;
			} else {
				this.logger.info('Python subprocess is starting up');
				return this.run_daemon();
			}
		}
	}, {
		key: 'forward',
		value: function forward(topic, data) {
			var _this5 = this;

			var msgStr = data.toString();
			var messages = msgStr.split('\n');

			messages = _underscore2.default.map(messages, function (m) {
				var val = m.replace(/\r$/g, '');
				return val;
			});

			if (!this.forwardToPort || !this.forwardToAddress || !this.producer) {
				return;
			}

			//contain possible errors if datasink is temporarily down
			try {
				this.producer.send([{
					topic: topic,
					messages: messages
				}], function (err) {
					if (err) {
						return _this5.logger.warn('[Forwarder.forward()] ' + JSON.stringify(err));
					}
					if (_this5.debug) {
						_this5.logger.info('Forwarded ' + messages);
						_this5.debug = false;
					}
				});
			} catch (e) {
				this.logger.warn(e); //carry on
			}
		}
	}]);

	return Forwarder;
}();

exports.default = Forwarder;