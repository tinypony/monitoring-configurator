'use strict';

Object.defineProperty(exports, "__esModule", {
	value: true
});

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

var _dgram = require('dgram');

var _dgram2 = _interopRequireDefault(_dgram);

var _q = require('q');

var _q2 = _interopRequireDefault(_q);

var _underscore = require('underscore');

var _underscore2 = _interopRequireDefault(_underscore);

var _winston = require('winston');

var _winston2 = _interopRequireDefault(_winston);

var _dequeue = require('dequeue');

var _dequeue2 = _interopRequireDefault(_dequeue);

var _nodeUuid = require('node-uuid');

var _nodeUuid2 = _interopRequireDefault(_nodeUuid);

var _kafkaNode = require('kafka-node');

var _os = require('os');

var _os2 = _interopRequireDefault(_os);

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

var KafkaPuller = function () {
	function KafkaPuller(config) {
		_classCallCheck(this, KafkaPuller);

		this.config = config;
		this.consumer;
		this.ou_socket = _dgram2.default.createSocket('udp4');

		this.logger = new _winston2.default.Logger({
			transports: [new _winston2.default.transports.Console({ leve: 'info' })]
		});

		if (config.logging && config.logging.disable) {
			this.logger.remove(_winston2.default.transports.Console);
		}
	}

	_createClass(KafkaPuller, [{
		key: 'getConnectionString',
		value: function getConnectionString(monitoring) {
			return monitoring.host + ':' + monitoring.port;
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
		value: function send(msg, port) {
			var _this2 = this;

			this.ou_socket.send(new Buffer(msg), 0, msg.length, port, '127.0.0.1', function (err) {
				if (err) {
					return _this2.logger.warn('[KafkaPuller.send()] ' + JSON.stringify(err));
				}
				if (!firstMessageLogged) {
					_this2.logger.info('[KafkaPuller] Passed message "%s" to subscribed client 127.0.0.1:%d', msg, port);
					firstMessageLogged = true;
				}
			});
		}
	}, {
		key: 'getClientId',
		value: function getClientId(sub) {
			return _os2.default.hostname() + "-" + sub.port + "-" + sub.topics.join('-');
		}
	}, {
		key: 'handleConsumerError',
		value: function handleConsumerError(err, sub, monitoring) {
			if (KAFKA_ERROR.isNodeExists(err)) {
				this.logger.info('[KafkaPuller] Waiting for kafka to clear previous connection');
				this.consumer = null;
				setTimeout(this.subscribe.bind(this, sub, monitoring), 5000);
			} else if (KAFKA_ERROR.isCouldNotFindBroker(err)) {
				//Waiting for KAFKA to spin up (possibly)
				this.logger.info('[KafkaPuller] Waiting for kafka to spin up');
				this.consumer = null;
				setTimeout(this.subscribe.bind(this, sub, monitoring), 5000);
			} else {
				this.logger.warn(JSON.stringify(err));
			}
		}

		/**
   * sub.topics,
   * sub.port			//port to send subscribed data
   *
   * monitoring.host 	//zk host
   * monitoring.port  //zk port
   */

	}, {
		key: 'subscribe',
		value: function subscribe(sub, monitoring) {
			var _this3 = this;

			if (this.consumer) {
				this.consumer.close(function () {
					_this3.consumer = null;
					_this3.subscribe(sub.monitoring);
				});
			} else {
				this.logger.info('[KafkaPuller] Subscribing 127.0.0.1:%d', sub.port);
				this.createConsumer(sub, monitoring).then(function (args) {
					var consumer = args.consumer;
					var FIFO = args.FIFO;
					var port = args.port;

					_this3.consumer = consumer;

					_this3.logger.info('[KafkaPuller] Attach message handler consumer');
					_this3.consumer.on('message', function (msg) {
						if (!msg.value) {
							return;
						}

						FIFO.push({
							port: port,
							msg: msg.value
						});

						if (FIFO.length === 1) {
							setImmediate(_this3.run.bind(_this3, FIFO));
						}
					});

					_this3.logger.info('[KafkaPuller] Attached all required callbacks to consumer');
				}).catch(function (err) {
					_this3.logger.warn('[KafkaPuller] Here we have error in catch ' + JSON.stringify(err));
					_this3.handleConsumerError(err, sub, monitoring);
				});
			}
		}
	}, {
		key: 'createConsumer',
		value: function createConsumer(sub, monitoring) {
			var connStr = this.getConnectionString(monitoring);

			this.logger.info('[KafkaPuller] Creating consumer for ' + connStr + ', ' + sub.topics.join(' ') + ' => ' + sub.port);
			var defer = _q2.default.defer();
			var client = new _kafkaNode.Client(connStr, this.getClientId(sub));
			var FIFO = new _dequeue2.default();

			var payloads = _underscore2.default.map(sub.topics, function (topic) {
				return { topic: topic };
			});

			var consumer = new _kafkaNode.HighLevelConsumer(client, payloads, {
				autoCommit: true,
				autoCommitIntervalMs: 5000,
				encoding: 'utf8'
			});

			this.logger.info('[KafkaPuller] created consumer');

			//Handle consumer connection error
			consumer.on('error', function (err) {
				defer.reject(err);
			});

			defer.resolve({
				consumer: consumer,
				FIFO: FIFO,
				port: parseInt(sub.port)
			});

			return defer.promise;
		}
	}, {
		key: 'run',
		value: function run(FIFO) {
			while (FIFO.length) {
				var item = FIFO.shift();
				this.send(item.msg, item.port);
			}
		}
	}]);

	return KafkaPuller;
}();

exports.default = KafkaPuller;