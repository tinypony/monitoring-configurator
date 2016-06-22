'use strict';

Object.defineProperty(exports, "__esModule", {
	value: true
});

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

var _dgram = require('dgram');

var _dgram2 = _interopRequireDefault(_dgram);

var _netmask = require('netmask');

var _nodeType = require('./node-type');

var _nodeType2 = _interopRequireDefault(_nodeType);

var _messageType = require('./message-type');

var _forwarder = require('./forwarder/forwarder');

var _forwarder2 = _interopRequireDefault(_forwarder);

var _kafkaForwarder = require('./kafka/kafka-forwarder');

var _kafkaForwarder2 = _interopRequireDefault(_kafkaForwarder);

var _nodeUuid = require('node-uuid');

var _nodeUuid2 = _interopRequireDefault(_nodeUuid);

var _q = require('q');

var _q2 = _interopRequireDefault(_q);

var _underscore = require('underscore');

var _underscore2 = _interopRequireDefault(_underscore);

var _winston = require('winston');

var _winston2 = _interopRequireDefault(_winston);

var _datasinkRole = require('./roles/datasink-role');

var _datasinkRole2 = _interopRequireDefault(_datasinkRole);

var _producerRole = require('./roles/producer-role');

var _producerRole2 = _interopRequireDefault(_producerRole);

var _consumerRole = require('./roles/consumer-role');

var _consumerRole2 = _interopRequireDefault(_consumerRole);

var _tracker = require('./roles/tracker');

var _tracker2 = _interopRequireDefault(_tracker);

var _p2pProducerRole = require('./roles/p2p-producer-role');

var _p2pProducerRole2 = _interopRequireDefault(_p2pProducerRole);

var _p2pConsumerRole = require('./roles/p2p-consumer-role');

var _p2pConsumerRole2 = _interopRequireDefault(_p2pConsumerRole);

var _datasinkSlaveRole = require('./roles/datasink-slave-role');

var _datasinkSlaveRole2 = _interopRequireDefault(_datasinkSlaveRole);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function isValidPort(port) {
	return _underscore2.default.isNumber(port) && port > 0 && port < 65535;
}

var ConfigurationDaemon = function () {
	function ConfigurationDaemon(config, broadcastPort) {
		var _this = this;

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
		this.initId = _nodeUuid2.default.v4();

		this.broadcastPort = broadcastPort;
		this.bc_socket = _dgram2.default.createSocket('udp4');
		this.uc_socket = _dgram2.default.createSocket('udp4');

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

		this.roles = [new _datasinkRole2.default(this.initId, this.config, sockets), new _datasinkSlaveRole2.default(this.initId, this.config, sockets), new _producerRole2.default(this.initId, this.config, sockets), new _consumerRole2.default(this.initId, this.config, sockets),
		//p2p version
		new _tracker2.default(this.initId, this.config, sockets), new _p2pConsumerRole2.default(this.initId, this.config, sockets), new _p2pProducerRole2.default(this.initId, this.config, sockets)];

		this.hasStartedDefer = _q2.default.defer();
		this.hasStarted = this.hasStartedDefer.promise;
		this.hasStarted.catch(function (err) {
			return _this.logger.info(JSON.stringify(err));
		});
	}

	_createClass(ConfigurationDaemon, [{
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
			var _this2 = this;

			this.bc_socket.setBroadcast(true);
			var funcs = this.getRoleFunctions('onStart');
			funcs.push(function () {
				_this2.hasStartedDefer.resolve();
			});

			return funcs.reduce(function (promise, f) {
				return promise.then(f);
			}, (0, _q2.default)());
		}
	}, {
		key: 'getBroadcastAddress',
		value: function getBroadcastAddress() {
			var block = new _netmask.Netmask(this.config.monitoring.subnet);
			return block.broadcast;
		}
	}, {
		key: 'handleInChain',
		value: function handleInChain(msg, func) {
			var funcs = this.getRoleFunctions(func);

			return funcs.reduce(function (promise, f) {
				return promise.then(f);
			}, (0, _q2.default)(msg));
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
		key: 'handleRegslave',
		value: function handleRegslave(msg) {
			return this.handleInChain(msg, 'handleRegslave');
		}
	}, {
		key: 'handleClusterResize',
		value: function handleClusterResize(msg) {
			return this.handleInChain(msg, 'handleClusterResize');
		}
	}, {
		key: 'handleTConfig',
		value: function handleTConfig(msg) {
			return this.handleInChain(msg, 'handleTConfig');
		}
	}, {
		key: 'handleTReconfig',
		value: function handleTReconfig(msg) {
			return this.handleInChain(msg, 'handleTReconfig');
		}
	}, {
		key: 'handlePublish',
		value: function handlePublish(msg) {
			return this.handleInChain(msg, 'handlePublish');
		}
	}, {
		key: 'handleNewDestination',
		value: function handleNewDestination(msg) {
			return this.handleInChain(msg, 'handleNewDestination');
		}
	}, {
		key: 'close',
		value: function close() {
			this.uc_socket.close();
			this.bc_socket.close();
		}
	}, {
		key: 'handleUnicastMessage',
		value: function handleUnicastMessage(msg) {
			if (msg.type === _messageType.MESSAGE_TYPE.CONFIG) {
				return this.handleConfig(msg);
			} else if (msg.type === _messageType.MESSAGE_TYPE.TCONFIG) {
				return this.handleTConfig(msg);
			} else if (msg.type === _messageType.MESSAGE_TYPE.SUBSCRIBE) {
				return this.handleSubscribe(msg);
			} else if (msg.type === _messageType.MESSAGE_TYPE.REGISTER_SLAVE) {
				return this.handleRegslave(msg);
			} else if (msg.type === _messageType.MESSAGE_TYPE.PUBLISH) {
				return handlePublish(msg);
			} else if (msg.type === _messageType.MESSAGE_TYPE.NEW_DESTINATION) {
				return handleNewDestination(msg);
			}
		}
	}, {
		key: 'handleBroadcastMessage',
		value: function handleBroadcastMessage(msg) {
			if (msg.type === _messageType.MESSAGE_TYPE.HELLO) {
				return this.handleHello(msg);
			} else if (msg.type === _messageType.MESSAGE_TYPE.RECONFIG) {
				return this.handleReconfig(msg);
			} else if (msg.type === _messageType.MESSAGE_TYPE.CLUSTER_RESIZE) {
				return this.handleClusterResize(msg);
			} else if (msg.type === _messageType.MESSAGE_TYPE.TRECONFIG) {
				return this.handleTReconfig(msg);
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
			var _this3 = this;

			return function (data, rinfo) {
				var dataString = data.toString();

				try {
					var msg = JSON.parse(dataString);
					msg = _this3.preprocessMessage(msg, rinfo);

					if (isBroadcast) _this3.handleBroadcastMessage(msg);else _this3.handleUnicastMessage(msg);
				} catch (e) {
					//silent skip
					_this3.logger.info("Could not parse incoming data, probably malformed");
				}
			};
		}
	}]);

	return ConfigurationDaemon;
}();

exports.default = ConfigurationDaemon;