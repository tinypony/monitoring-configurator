'use strict';

Object.defineProperty(exports, "__esModule", {
	value: true
});

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

var _forwarder = require('./forwarder/forwarder.js');

var _forwarder2 = _interopRequireDefault(_forwarder);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var dgram = require('dgram');
var _ = require('underscore');
var Netmask = require('netmask').Netmask;
var NODE_TYPE = require('./node-type.js');

var KafkaForwarder = require('./kafka/kafka-forwarder.js');
var uuid = require('node-uuid');
var q = require('q');
var winston = require('winston');

function isValidPort(port) {
	return _.isNumber(port) && port > 0 && port < 65535;
}

var ConfigurationDaemon = function () {
	function ConfigurationDaemon(config, broadcastPort) {
		_classCallCheck(this, ConfigurationDaemon);

		this.logger = new winston.Logger({
			transports: [new winston.transports.Console()]
		});

		if (config.logging && config.logging.disable) {
			this.logger.remove(winston.transports.Console);
		}

		this.config = config;
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
		this.logger.info('sd');

		//bind sockets and attach on listen
		this.bc_socket.bind(this.broadcastPort, '0.0.0.0');
		this.uc_socket.bind(this.config.unicast.port, '0.0.0.0');
		this.bc_socket.on('listening', this.onStartListening.bind(this));
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
		key: 'onStartListening',
		value: function onStartListening() {
			var _this = this;

			this.bc_socket.setBroadcast(true);
			var message;

			//send message on start depending on node type
			if (this.isDatasink()) {
				message = this.getReconfigureMessage();
			} else if (this.isProducer() || this.isConsumer()) {
				message = this.getHelloMessage();
			} else {
				return;
			}

			this.bc_socket.send(new Buffer(message), 0, message.length, this.broadcastPort, this.getBroadcastAddress(), function (err) {
				if (err) {
					_this.logger.warn(err);
					return _this.hasStartedDefer.reject(err);
				}
				_this.hasStartedDefer.resolve();
			});
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
			return _.contains(this.config.roles, NODE_TYPE.DATASINK);
		}
	}, {
		key: 'isProducer',
		value: function isProducer() {
			return _.contains(this.config.roles, NODE_TYPE.PRODUCER);
		}
	}, {
		key: 'isConsumer',
		value: function isConsumer() {
			return _.contains(this.config.roles, NODE_TYPE.CONSUMER);
		}
	}, {
		key: 'handleHello',
		value: function handleHello(msg) {
			var defer = q.defer();
			if (this.initId && msg.uuid === this.initId) {
				this.address = msg.host;
				this.initId = null;
			}

			if (this.isDatasink()) {
				var configMessage = this.getConfigureMessage();

				this.uc_socket.send(new Buffer(configMessage), 0, configMessage.length, msg.port, msg.host, function (err) {
					if (err) {
						return defer.reject(err);
					}
					defer.resolve();
				});
			}

			return defer.promise;
		}
	}, {
		key: 'handleSubscribe',
		value: function handleSubscribe(msg) {
			var _this2 = this;

			if (this.isDatasink()) {
				this.logger.info('let\'s subscribe');

				_.each(msg.endpoints, function (ep) {
					_this2.logger.info('wire %s to %s:%d', ep.topics.join(","), msg.host, ep.port);
					ep.host = msg.host;
					ep.unicastport = msg.port;
					_this2.kafkaForwarder.subscribe(ep);
				});
			}
		}
	}, {
		key: 'handleBroadcastMessage',
		value: function handleBroadcastMessage(msg) {
			if (msg.type === 'hello') {
				return this.handleHello(msg);
			}

			//Every type of node is being monitored and needs to be reconfigured
			if (msg.type === 'reconfig' && (this.isProducer() || this.isConsumer())) {
				return this.configureClient(msg);
			}
		}
	}, {
		key: 'configureClient',
		value: function configureClient(msg) {
			var _this3 = this;

			var defer = q.defer();

			if (this.isProducer() || this.isConsumer()) {
				this.logger.info('configure client with ' + JSON.stringify(msg));
				this.config.monitoring = _.extend(this.config.monitoring, msg.monitoring);

				if (this.isProducer()) {
					this.forwarder.reconfig(this.config);
					defer.resolve();
				}

				if (this.isConsumer()) {
					if (!isValidPort(msg.port)) {
						this.logger.info('trying to send subscription message to an invalid port');
						return defer.reject();
					}
					var subscribeMsg = this.getSubscribeMessage();

					this.uc_socket.send(new Buffer(subscribeMsg), 0, subscribeMsg.length, msg.port, msg.host, function (e) {
						if (e) {
							_this3.logger.warn(JSON.stringify(e));
							return defer.reject(e);
						}
						defer.resolve();
						_this3.logger.info('Sent subscribe request to ' + msg.host + ":" + msg.port);
					});
				}
			} else {
				defer.reject('nothing to configure');
			}

			return defer.promise;
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
				return this.configureClient(msg);
			}

			if (msg.type === 'subscribe') {
				return this.handleSubscribe(msg);
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
			var _this4 = this;

			return function (data, rinfo) {
				var dataString = data.toString();

				try {
					var msg = JSON.parse(dataString);
					msg = _this4.preprocessMessage(msg, rinfo);

					if (isBroadcast) _this4.handleBroadcastMessage(msg);else _this4.handleUnicastMessage(msg);
				} catch (e) {
					//silent skip
					_this4.logger.info("Could not parse incoming data, probably malformed");
				}
			};
		}
	}, {
		key: 'getReconfigureMessage',
		value: function getReconfigureMessage() {
			var msg = {
				type: 'reconfig',
				host: 'self',
				port: this.config.unicast.port,
				monitoring: {
					host: 'self',
					port: this.config.monitoring.port,
					keystone: this.config.monitoring.keystone
				}
			};

			return JSON.stringify(msg);
		}
	}, {
		key: 'getConfigureMessage',
		value: function getConfigureMessage() {
			var msg = {
				type: 'config',
				host: 'self',
				port: this.config.unicast.port,
				monitoring: {
					host: 'self',
					port: this.config.monitoring.port
				}
			};

			return JSON.stringify(msg);
		}
	}, {
		key: 'getHelloMessage',
		value: function getHelloMessage() {
			var msg = {
				type: 'hello',
				uuid: this.initId,
				host: 'self',
				port: this.config.unicast.port
			};

			return JSON.stringify(msg);
		}
	}, {
		key: 'getSubscribeMessage',
		value: function getSubscribeMessage() {
			var msg = {
				type: 'subscribe',
				host: 'self',
				port: this.config.unicast.port,
				endpoints: this.config.consumers
			};

			return JSON.stringify(msg);
		}
	}, {
		key: 'getPongMessage',
		value: function getPongMessage() {
			var msg = {
				type: 'pong',
				host: 'self',
				port: this.config.unicast.port
			};

			return JSON.stringify(msg);
		}
	}]);

	return ConfigurationDaemon;
}();

exports.default = ConfigurationDaemon;