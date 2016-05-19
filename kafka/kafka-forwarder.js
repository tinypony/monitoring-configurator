'use strict';

Object.defineProperty(exports, "__esModule", {
	value: true
});

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

var _dgram = require('dgram');

var _dgram2 = _interopRequireDefault(_dgram);

var _underscore = require('underscore');

var _underscore2 = _interopRequireDefault(_underscore);

var _winston = require('winston');

var _winston2 = _interopRequireDefault(_winston);

var _kafkaNode = require('kafka-node');

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var firstMessageLogged = false;

var KAFKA_ERROR = {
	isNodeExists: function isNodeExists(err) {
		return _underscore2.default.isString(err.message) && err.message.indexOf('NODE_EXISTS') > -1;
	},
	isCouldNotFindBroker: function isCouldNotFindBroker(err) {
		return _underscore2.default.isString(err.message) && err.message.indexOf('Could not find a broker') > -1;
	}
};

//forwards message from kafka to clients who subscribed to particular topics

var KafkaForwarder = function () {
	function KafkaForwarder(config) {
		_classCallCheck(this, KafkaForwarder);

		this.config = config;
		this.connections = [];
		this.ou_socket = _dgram2.default.createSocket('udp4');

		this.logger = new _winston2.default.Logger({
			transports: [new _winston2.default.transports.Console({ leve: 'info' })]
		});

		if (config.logging && config.logging.disable) {
			this.logger.remove(_winston2.default.transports.Console);
		}
	}

	_createClass(KafkaForwarder, [{
		key: 'getConnectionString',
		value: function getConnectionString() {
			return this.config.monitoring.host + ':' + this.config.monitoring.port;
		}
	}, {
		key: 'send',
		value: function send(msg, host, port) {
			this.ou_socket.send(new Buffer(msg), 0, msg.length, port, host, function (err) {
				if (err) return this.logger.warn(err);
				if (!firstMessageLogged) {
					this.logger.info('Sent message "%s" to subscribed client %s:%d', msg, host, port);
					firstMessageLogged = true;
				}
			}.bind(this));
		}
	}, {
		key: 'getPingMessage',
		value: function getPingMessage() {
			var msg = {
				type: 'ping',
				host: 'self',
				port: this.config.unicast.port
			};

			return JSON.stringify(msg);
		}
	}, {
		key: 'hasConnection',
		value: function hasConnection(sub) {
			var existing = _underscore2.default.findWhere(this.connections, { host: sub.host, port: sub.port });

			if (!existing) {
				return false;
			}

			return this.getClientId(existing) === this.getClientId(sub); //a quick hack, as we basically need to ensure that IDs are unique
		}
	}, {
		key: 'getClientId',
		value: function getClientId(sub) {
			return sub.host + "-" + sub.port + "-" + sub.topics.join('-');
		}

		/**
   * sub.topics,
   * sub.unicastport  //port for sending control signals
   * sub.port,		//port to send subscribed data
   * sub.host
   */

	}, {
		key: 'subscribe',
		value: function subscribe(sub) {
			var _this = this;

			if (this.hasConnection(sub)) {
				return;
			}
			this.logger.info('[KafkaForwarder] Subscribing %s:%d', sub.host, sub.port);
			var client = new _kafkaNode.Client(this.getConnectionString(), this.getClientId(sub));
			this.logger.info('[KafkaForwarder] created client');
			var payloads = _underscore2.default.map(sub.topics, function (topic) {
				return {
					topic: topic
				};
			});

			this.logger.info('[KafkaForwarder] creating consumer');
			var consumer = new _kafkaNode.HighLevelConsumer(client, payloads, {
				autoCommit: true,
				autoCommitIntervalMs: 5000,
				encoding: 'utf8'
			});
			this.logger.info('[KafkaForwarder] created consumer');

			consumer.on("error", function (err) {
				_this.logger.warn('[KafkaForwarder]');
				_this.logger.warn(JSON.stringify(err));

				//Waiting for kafka to timeout and clear previous connection
				if (KAFKA_ERROR.isNodeExists(err)) {
					_this.logger.info('Waiting for kafka to clear previous connection');
					setTimeout(_this.subscribe.bind(_this, sub), 5000);
				}
				//Waiting for KAFKA to spin up (possibly)
				else if (KAFKA_ERROR.isCouldNotFindBroker(err)) {
						_this.logger.info('Waiting for kafka to spin up');
						setTimeout(_this.subscribe.bind(_this, sub), 5000);
					}
			});

			consumer.on('message', function (msg) {
				if (!msg.value) {
					_this.logger.warn('[KafkaForwarder] message empty, drop');

					return;
				}

				_this.send(msg.value, sub.host, parseInt(sub.port));
			});

			consumer.on('connect', function () {
				_this.connections.push({
					host: sub.host,
					port: sub.unicastport,
					topics: sub.topics,
					consumer: consumer,
					liveStatus: 1 // 0 - unresponsive, 1 - live, 2 - pending check
				});
				_this.logger.info('Subscribed ' + _this.getClientId(sub));
			});

			this.logger.info('[KafkaForwarder] Attached all required callbacks to consumer');
		}
	}]);

	return KafkaForwarder;
}();

exports.default = KafkaForwarder;