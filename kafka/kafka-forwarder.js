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

var _dequeue = require('dequeue');

var _dequeue2 = _interopRequireDefault(_dequeue);

var _kafkaNode = require('kafka-node');

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var firstMessageLogged = false;
var latencyC = 0;

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
		this.connections = {};
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
		key: 'handleRebalance',
		value: function handleRebalance() {
			var _this = this;

			_underscore2.default.each(this.connection, function (con) {
				//recreate consumer for all connections
				con.consumer.close(true, function () {
					_this.createConsumer(con.subInfo);
				});
			});
		}
	}, {
		key: 'send',
		value: function send(msg, host, port) {
			var _this2 = this;

			this.ou_socket.send(new Buffer(msg), 0, msg.length, port, host, function (err) {
				if (err) return _this2.logger.warn('[KafkaForwarder.send()] ' + JSON.stringify(err));
				if (!firstMessageLogged) {
					_this2.logger.info('Sent message "%s" to subscribed client %s:%d', msg, host, port);
					firstMessageLogged = true;
				}
			});
		}
	}, {
		key: 'hasConnection',
		value: function hasConnection(sub) {
			var existing = _underscore2.default.findWhere(_underscore2.default.values(this.connections), { host: sub.host, port: sub.port });

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
			if (this.hasConnection(sub)) {
				return;
			}
			this.logger.info('[KafkaForwarder] Subscribing %s:%d', sub.host, sub.port);
			this.createConsumer(sub);
		}
	}, {
		key: 'createConsumer',
		value: function createConsumer(sub) {
			var _this3 = this;

			var client = new _kafkaNode.Client(this.getConnectionString(), this.getClientId(sub));
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

			//Handle consumer connection error
			consumer.on("error", function (err) {
				//Waiting for kafka to timeout and clear previous connection
				if (KAFKA_ERROR.isNodeExists(err)) {
					_this3.logger.info('Waiting for kafka to clear previous connection');
					setTimeout(_this3.createConsumer.bind(_this3, sub), 5000);
				} else if (KAFKA_ERROR.isCouldNotFindBroker(err)) {
					//Waiting for KAFKA to spin up (possibly)
					_this3.logger.info('Waiting for kafka to spin up');
					setTimeout(_this3.createConsumer.bind(_this3, sub), 5000);
				}
			});

			consumer.on('message', function (msg) {
				if (!msg.value) {
					return;
				}

				_this3.send(msg.value, sub.host, parseInt(sub.port));
			});

			consumer.on('connect', function () {
				_this3.connections[_this3.getClientId(sub)] = {
					host: sub.host,
					port: sub.unicastport,
					topics: sub.topics,
					consumer: consumer,
					liveStatus: 1, // 0 - unresponsive, 1 - live, 2 - pending check
					subInfo: sub
				};
				_this3.logger.info('Subscribed ' + _this3.getClientId(sub));
			});

			this.logger.info('[KafkaForwarder] Attached all required callbacks to consumer');
		}
	}]);

	return KafkaForwarder;
}();

exports.default = KafkaForwarder;