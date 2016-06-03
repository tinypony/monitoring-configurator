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

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function isValidPort(port) {
	return _underscore2.default.isNumber(port) && port > 0 && port < 65535;
}

var Forwarder = function () {
	function Forwarder(config) {
		var _this = this;

		_classCallCheck(this, Forwarder);

		this.ou_socket = _dgram2.default.createSocket('udp4');
		this.id = _nodeUuid2.default.v4();

		this.logger = new _winston2.default.Logger({
			transports: [new _winston2.default.transports.Console()]
		});

		if (config.logging && config.logging.disable) {
			this.logger.remove(_winston2.default.transports.Console);
		}

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

	_createClass(Forwarder, [{
		key: 'reconfig',
		value: function reconfig(config) {
			if (!isValidPort(config.monitoring.port)) {
				this.logger.info('trying to configure forwarder with an invalid port');
				return;
			}
			this.forwardToAddress = config.monitoring.host;
			this.forwardToPort = config.monitoring.port;
			this.logger.info('[Forwarder.reconfig()] Reconfiguring forwarder');
			this.reconnect();
		}
	}, {
		key: 'createConnection',
		value: function createConnection() {
			var _this2 = this;

			var defer = _q2.default.defer();
			var connectionString = this.forwardToAddress + ':' + this.forwardToPort;
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

			return defer.promise;
		}
	}, {
		key: 'forward',
		value: function forward(topic, data) {
			var _this4 = this;

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
						return _this4.logger.warn('[Forwarder.forward()] ' + JSON.stringify(err));
					}
					//this.logger.info('Forwarded messages: '+JSON.stringify(messages));
				});
			} catch (e) {
				this.logger.warn(e); //carry on
			}
		}
	}]);

	return Forwarder;
}();

exports.default = Forwarder;