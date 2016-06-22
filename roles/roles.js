'use strict';

Object.defineProperty(exports, "__esModule", {
	value: true
});

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

var _underscore = require('underscore');

var _underscore2 = _interopRequireDefault(_underscore);

var _q = require('q');

var _q2 = _interopRequireDefault(_q);

var _nodeType = require('../node-type');

var _nodeType2 = _interopRequireDefault(_nodeType);

var _messageType = require('../message-type');

var _netmask = require('netmask');

var _winston = require('winston');

var _winston2 = _interopRequireDefault(_winston);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var Role = function () {
	function Role(initId, config, sockets) {
		_classCallCheck(this, Role);

		this.initId = initId;
		this.config = config;
		this.sockets = sockets;
		this.logger = this.getLogger();
	}

	_createClass(Role, [{
		key: 'getLogger',
		value: function getLogger() {
			var logger = new _winston2.default.Logger({
				transports: [new _winston2.default.transports.Console({ level: 'info' })]
			});

			if (this.config.logging && this.config.logging.disable) {
				logger.remove(_winston2.default.transports.Console);
			}

			return logger;
		}
	}, {
		key: 'send',
		value: function send(host, port, message) {
			var defer = _q2.default.defer();

			this.sockets.unicast.send(new Buffer(message), 0, message.length, port, host, function (err) {
				if (err) {
					return defer.reject(err);
				} else {
					return defer.resolve();
				}
			});

			return defer.promise;
		}

		/**
   * Method that is shared by all role subclasses to send unicast responses to the original sender of a message
   */

	}, {
		key: 'respondTo',
		value: function respondTo(receivedMessage, response) {
			return this.send(receivedMessage.host, receivedMessage.port, response);
		}

		/**
   * Method that is shared by all role subclasses to send broadcast messages to the monitoring network
   */

	}, {
		key: 'broadcast',
		value: function broadcast(message) {
			var _this = this;

			var defer = _q2.default.defer();
			this.logger.info('Broadcast');
			this.sockets.broadcast.send(new Buffer(message), 0, message.length, this.config.broadcastPort, this.getBroadcastAddress(), function (err) {
				if (err) {
					_this.logger.warn(err);
					defer.reject(err);
				} else {
					defer.resolve();
				}
			});

			return defer.promise;
		}
	}, {
		key: 'isTracker',
		value: function isTracker() {
			return _underscore2.default.contains(this.config.roles, _nodeType2.default.TRACKER);
		}
	}, {
		key: 'isDatasink',
		value: function isDatasink() {
			return _underscore2.default.contains(this.config.roles, _nodeType2.default.DATASINK);
		}
	}, {
		key: 'isDatasinkSlave',
		value: function isDatasinkSlave() {
			return _underscore2.default.contains(this.config.roles, _nodeType2.default.DATASINK_SLAVE);
		}
	}, {
		key: 'isProducer',
		value: function isProducer() {
			return _underscore2.default.contains(this.config.roles, _nodeType2.default.PRODUCER);
		}
	}, {
		key: 'isConsumer',
		value: function isConsumer() {
			return _underscore2.default.contains(this.config.roles, _nodeType2.default.CONSUMER);
		}
	}, {
		key: 'isP2PProducer',
		value: function isP2PProducer() {
			return _underscore2.default.contains(this.config.roles, _nodeType2.default.P2PPRODUCER);
		}
	}, {
		key: 'isP2PConsumer',
		value: function isP2PConsumer() {
			return _underscore2.default.contains(this.config.roles, _nodeType2.default.P2PCONSUMER);
		}
	}, {
		key: 'isValidPort',
		value: function isValidPort(port) {
			return _underscore2.default.isNumber(port) && port > 0 && port < 65535;
		}
	}, {
		key: 'onStart',
		value: function onStart(prev) {
			var defer = _q2.default.defer();
			defer.resolve(prev);
			return defer.promise;
		}
	}, {
		key: 'onStop',
		value: function onStop() {
			var defer = _q2.default.defer();
			defer.resolve();
			return defer.promise;
		}
	}, {
		key: 'handleHello',
		value: function handleHello(msg) {
			var defer = _q2.default.defer();
			defer.resolve(msg);
			return defer.promise;
		}
	}, {
		key: 'handleConfig',
		value: function handleConfig(msg) {
			var defer = _q2.default.defer();
			defer.resolve(msg);
			return defer.promise;
		}
	}, {
		key: 'handleReconfig',
		value: function handleReconfig(msg) {
			var defer = _q2.default.defer();
			defer.resolve(msg);
			return defer.promise;
		}
	}, {
		key: 'handleSubscribe',
		value: function handleSubscribe(msg) {
			var defer = _q2.default.defer();
			defer.resolve(msg);
			return defer.promise;
		}
	}, {
		key: 'handleTConfig',
		value: function handleTConfig(msg) {
			var defer = _q2.default.defer();
			defer.resolve(msg);
			return defer.promise;
		}
	}, {
		key: 'handleTReconfig',
		value: function handleTReconfig(msg) {
			var defer = _q2.default.defer();
			defer.resolve(msg);
			return defer.promise;
		}
	}, {
		key: 'handlePublish',
		value: function handlePublish(msg) {
			var defer = _q2.default.defer();
			defer.resolve(msg);
			return defer.promise;
		}
	}, {
		key: 'handleRegslave',
		value: function handleRegslave(msg) {
			var defer = _q2.default.defer();
			defer.resolve(msg);
			return defer.promise;
		}
	}, {
		key: 'handleNewDestination',
		value: function handleNewDestination(msg) {
			var defer = _q2.default.defer();
			defer.resolve(msg);
			return defer.promise;
		}
	}, {
		key: 'handleClusterResize',
		value: function handleClusterResize(msg) {
			var defer = _q2.default.defer();
			defer.resolve(msg);
			return defer.promise;
		}
	}, {
		key: 'getBroadcastAddress',
		value: function getBroadcastAddress() {
			var block = new _netmask.Netmask(this.config.monitoring.subnet);
			return block.broadcast;
		}
	}, {
		key: 'getReconfigureMessage',
		value: function getReconfigureMessage() {
			var msg = {
				type: _messageType.MESSAGE_TYPE.RECONFIG,
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
		key: 'getConfigureMessage',
		value: function getConfigureMessage(extension) {
			var msg = {
				type: _messageType.MESSAGE_TYPE.CONFIG,
				host: 'self',
				port: this.config.unicast.port,
				monitoring: {
					host: 'self',
					port: this.config.monitoring.port
				}
			};

			msg = _underscore2.default.extend(msg, extension);

			return JSON.stringify(msg);
		}
	}, {
		key: 'getTrackerReconfigureMessage',
		value: function getTrackerReconfigureMessage() {
			var msg = {
				type: _messageType.MESSAGE_TYPE.TRECONFIG,
				host: 'self',
				port: this.config.unicast.port
			};

			return JSON.stringify(msg);
		}
	}, {
		key: 'getTrackerConfigureMessage',
		value: function getTrackerConfigureMessage(extension) {
			var msg = {
				type: _messageType.MESSAGE_TYPE.TCONFIG,
				host: 'self',
				port: this.config.unicast.port
			};

			msg = _underscore2.default.extend(msg, extension);

			return JSON.stringify(msg);
		}
	}, {
		key: 'getHelloMessage',
		value: function getHelloMessage() {
			var msg = {
				type: _messageType.MESSAGE_TYPE.HELLO,
				roles: this.config.roles,
				uuid: this.initId,
				host: 'self',
				publish: this.config.producers ? _underscore2.default.map(this.config.producers, function (p) {
					return producers.topic;
				}) : [],
				subscribe: this.config.consumers ? this.config.consumers : [],
				port: this.config.unicast.port
			};

			return JSON.stringify(msg);
		}
	}, {
		key: 'getSubscribeMessage',
		value: function getSubscribeMessage() {
			var msg = {
				type: _messageType.MESSAGE_TYPE.SUBSCRIBE,
				host: 'self',
				port: this.config.unicast.port,
				subscribe: this.config.consumers ? this.config.consumers : []
			};

			return JSON.stringify(msg);
		}
	}, {
		key: 'getPublishMessage',
		value: function getPublishMessage() {
			var msg = {
				type: _messageType.MESSAGE_TYPE.PUBLISH,
				host: 'self',
				port: this.config.unicast.port,
				subscribe: this.config.consumers ? this.config.consumers : []
			};

			return JSON.stringify(msg);
		}
	}, {
		key: 'getSlaveRegisterMessage',
		value: function getSlaveRegisterMessage(brokerId, extension) {
			var msg = {
				type: _messageType.MESSAGE_TYPE.REGISTER_SLAVE,
				host: 'self',
				port: this.config.unicast.port,
				brokerId: brokerId
			};

			msg = _underscore2.default.extend(msg, extension);
			return JSON.stringify(msg);
		}
	}, {
		key: 'getClusterResizeMessage',
		value: function getClusterResizeMessage() {
			var msg = {
				type: _messageType.MESSAGE_TYPE.CLUSTER_RESIZE,
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
		key: 'getNewDestinationMessage',
		value: function getNewDestinationMessage(topic, endpoint) {
			var msg = {
				type: _messageType.MESSAGE_TYPE.NEW_DESTINATION,
				host: 'self',
				port: this.config.unicast.port,
				topic: topic,
				dest: endpoint
			};
		}
	}]);

	return Role;
}();

exports.default = Role;