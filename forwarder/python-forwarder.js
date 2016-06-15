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

var PythonForwarder = function () {
	function PythonForwarder(config) {
		_classCallCheck(this, PythonForwarder);

		this.id = _nodeUuid2.default.v4();
		this.debug = true;
		this.config = config;

		this.logger = new _winston2.default.Logger({
			transports: [new _winston2.default.transports.Console()]
		});

		if (config.logging && config.logging.disable) {
			this.logger.remove(_winston2.default.transports.Console);
		}
	}

	_createClass(PythonForwarder, [{
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
		key: 'reconnect',
		value: function reconnect() {
			var defer = _q2.default.defer();
			this.logger.info('[Forwarder.reconnect()] Using python forwarder');
			this.spawn_subprocess().done(defer.resolve);
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
			var _this = this;

			if (this.python_subprocess) {
				//take down existing process
				var defer = _q2.default.defer();

				this.logger.info('Killing python subprocess id=' + this.python_subprocess.pid + ', parent id=' + process.pid);
				kill(this.python_subprocess.pid, 'SIGKILL', function () {
					_this.python_subprocess = null;
					_this.run_daemon();
					_this.logger.info('Python subprocess restarted');
					defer.resolve();
				});

				return defer.promise;
			} else {
				this.logger.info('Python subprocess is starting up');
				return this.run_daemon();
			}
		}
	}]);

	return PythonForwarder;
}();

exports.default = PythonForwarder;