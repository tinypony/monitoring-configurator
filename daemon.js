'use strict';

Object.defineProperty(exports, "__esModule", {
	value: true
});

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

var _forwarder = require('./forwarder/forwarder.js');

var _forwarder2 = _interopRequireDefault(_forwarder);

var _underscore = require('underscore');

var _underscore2 = _interopRequireDefault(_underscore);

var _winston = require('winston');

var _winston2 = _interopRequireDefault(_winston);

var _datasinkRole = require('./roles/datasink-role.js');

var _datasinkRole2 = _interopRequireDefault(_datasinkRole);

var _producerRole = require('./roles/producer-role.js');

var _producerRole2 = _interopRequireDefault(_producerRole);

var _consumerRole = require('./roles/consumer-role.js');

var _consumerRole2 = _interopRequireDefault(_consumerRole);

var _datasinkSlaveRole = require('./roles/datasink-slave-role');

var _datasinkSlaveRole2 = _interopRequireDefault(_datasinkSlaveRole);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var dgram = require('dgram');
var Netmask = require('netmask').Netmask;
var NODE_TYPE = require('./node-type.js');

var KafkaForwarder = require('./kafka/kafka-forwarder.js');
var uuid = require('node-uuid');
var q = require('q');

function isValidPort(port) {
	return _underscore2.default.isNumber(port) && port > 0 && port < 65535;
}

var ConfigurationDaemon = function () {
	function ConfigurationDaemon(config, broadcastPort) {
		_classCallCheck(this, ConfigurationDaemon);

		this.logger = new _winston2.default.Logger({
			transports: [new _winston2.default.transports.Console()]
		});

		if (config.logging && config.logging.disable) {
			this.logger.remove(_winston2.default.transports.Console);
		}

		this.config = config;
		this.config.broadcastPort = broadcastPort;
		this.address = null;
		this.initId = uuid.v4();

		if (this.isProducer()) {
			this.initProducer(config);
		}

		if (this.isDatasink()) {
			this.initDatasink(config);
		}

		if (this.isConsumer()) {
			this.initConsumer(config);
		}

		this.broadcastPort = broadcastPort;
		this.bc_socket = dgram.createSocket('udp4');
		this.uc_socket = dgram.createSocket('udp4');

		//Attach message handlers
		this.bc_socket.on('message', this.getMessageHandler(true).bind(this));
		this.uc_socket.on('message', this.getMessageHandler(false).bind(this));

		//bind sockets and attach on listen
		this.bc_socket.bind(this.broadcastPort, '0.0.0.0');
		this.uc_socket.bind(this.config.unicast.port, '0.0.0.0');
		this.bc_socket.on('listening', this.onStartListening.bind(this));

		var sockets = {
			unicast: this.uc_socket,
			broadcast: this.bc_socket
		};

		this.roles = [new _datasinkRole2.default(this.initId, this.config, sockets), new _datasinkSlaveRole2.default(this.initId, this.config, sockets), new _producerRole2.default(this.initId, this.config, sockets), new _consumerRole2.default(this.initId, this.config, sockets)];

		this.hasStartedDefer = q.defer();
		this.hasStarted = this.hasStartedDefer.promise;
	}

	_createClass(ConfigurationDaemon, [{
		key: 'initDatasink',
		value: function initDatasink(config) {
			this.kafkaForwarder = new KafkaForwarder(config);
		}
	}, {
		key: 'initConsumer',
		value: function initConsumer(config) {}
	}, {
		key: 'initProducer',
		value: function initProducer(config) {
			this.forwarder = new _forwarder2.default(config);
		}
	}, {
		key: 'getRoleFunctions',
		value: function getRoleFunctions(func) {
			var funcs = [];

			_underscore2.default.each(this.roles, function (r) {
				if (r.isMe()) {
					funcs.push(r[func].bind(r));
				}
			});

			return funcs;
		}
	}, {
		key: 'onStartListening',
		value: function onStartListening() {
			var _this = this;

			this.bc_socket.setBroadcast(true);
			var funcs = this.getRoleFunctions('onStart');
			funcs.push(function () {
				_this.hasStartedDefer.resolve();
			});

			return funcs.reduce(function (promise, f) {
				return promise.then(f);
			}, q());
		}
	}, {
		key: 'getBroadcastAddress',
		value: function getBroadcastAddress() {
			var block = new Netmask(this.config.monitoring.subnet);
			return block.broadcast;
		}
	}, {
		key: 'isDatasink',
		value: function isDatasink() {
			return _underscore2.default.contains(this.config.roles, NODE_TYPE.DATASINK);
		}
	}, {
		key: 'isProducer',
		value: function isProducer() {
			return _underscore2.default.contains(this.config.roles, NODE_TYPE.PRODUCER);
		}
	}, {
		key: 'isConsumer',
		value: function isConsumer() {
			return _underscore2.default.contains(this.config.roles, NODE_TYPE.CONSUMER);
		}
	}, {
		key: 'handleInChain',
		value: function handleInChain(msg, func) {
			var funcs = this.getRoleFunctions(func);

			return funcs.reduce(function (promise, f) {
				return promise.then(f);
			}, q(msg));
		}
	}, {
		key: 'handleHello',
		value: function handleHello(msg) {
			return this.handleInChain(msg, 'handleHello');
		}
	}, {
		key: 'handleSubscribe',
		value: function handleSubscribe(msg) {
			return this.handleInChain(msg, 'handleSubscribe');
		}
	}, {
		key: 'handleReconfig',
		value: function handleReconfig(msg) {
			return this.handleInChain(msg, 'handleReconfig');
		}
	}, {
		key: 'handleConfig',
		value: function handleConfig(msg) {
			return this.handleInChain(msg, 'handleConfig');
		}
	}, {
		key: 'close',
		value: function close() {
			this.uc_socket.close();
			this.bc_socket.close();
		}

		//Client node is provided with configuration by a manager node

	}, {
		key: 'handleUnicastMessage',
		value: function handleUnicastMessage(msg) {
			if (msg.type === 'config') {
				return this.handleConfig(msg);
			}

			if (msg.type === 'subscribe') {
				return this.handleSubscribe(msg);
			}
		}
	}, {
		key: 'handleBroadcastMessage',
		value: function handleBroadcastMessage(msg) {
			if (msg.type === 'hello') {
				return this.handleHello(msg);
			}

			//Every type of node is being monitored and needs to be reconfigured
			if (msg.type === 'reconfig') {
				return this.handleReconfig(msg);
			}
		}
	}, {
		key: 'preprocessMessage',
		value: function preprocessMessage(msg, rinfo) {
			if (msg.monitoring) {
				msg.monitoring.host = rinfo.address;
			}

			if (msg.host) {
				msg.host = rinfo.address;
			}

			return msg;
		}
	}, {
		key: 'getMessageHandler',
		value: function getMessageHandler(isBroadcast) {
			var _this2 = this;

			return function (data, rinfo) {
				var dataString = data.toString();

				try {
					var msg = JSON.parse(dataString);
					msg = _this2.preprocessMessage(msg, rinfo);

					if (isBroadcast) _this2.handleBroadcastMessage(msg);else _this2.handleUnicastMessage(msg);
				} catch (e) {
					//silent skip
					_this2.logger.info("Could not parse incoming data, probably malformed");
				}
			};
		}
	}]);

	return ConfigurationDaemon;
}();

exports.default = ConfigurationDaemon;