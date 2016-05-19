'use strict';

Object.defineProperty(exports, "__esModule", {
	value: true
});

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

var _kafkaNode = require('kafka-node');

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var dgram = require('dgram');
var _ = require('underscore');

var uuid = require('node-uuid');
var winston = require('winston');

function isValidPort(port) {
	return _.isNumber(port) && port > 0 && port < 65535;
}

var Forwarder = function () {
	function Forwarder(config) {
		var _this = this;

		_classCallCheck(this, Forwarder);

		this.ou_socket = dgram.createSocket('udp4');
		this.id = uuid.v4();

		this.logger = new winston.Logger({
			transports: [new winston.transports.Console()]
		});

		if (config.logging && config.logging.disable) {
			this.logger.remove(winston.transports.Console);
		}

		/**
   * fwd.port,
   * fwd.topic
   */
		this.forward_ports = _.map(config.producers, function (fwd) {
			_this.logger.info("Forwarding configuration = %d => %s", fwd.port, fwd.topic);
			var skt = dgram.createSocket('udp4');
			skt.bind(fwd.port, '127.0.0.1');

			skt.on('error', function (er) {
				_this.logger.warn(er);
			});

			skt.on("message", _this.forward.bind(_this, fwd.topic));
			return skt;
		});
	}

	_createClass(Forwarder, [{
		key: 'reconfig',
		value: function reconfig(config) {
			var _this2 = this;

			if (!isValidPort(config.monitoring.port)) {
				this.logger.info('trying to configure forwarder with an invalid port');
				return;
			}

			this.forwardToAddress = config.monitoring.host;
			this.forwardToPort = config.monitoring.port;
			this.logger.info('[Forwarder] Reconfiguring forwarder');

			var createConnection = function createConnection() {
				var connectionString = _this2.forwardToAddress + ':' + _this2.forwardToPort;
				_this2.logger.info('Create zookeeper connection to %s', connectionString);
				var client = new _kafkaNode.Client(connectionString, _this2.id);
				var producer = new _kafkaNode.HighLevelProducer(client);

				producer.on('ready', function () {
					_this2.logger.info('Forwader is ready');
					_this2.producer = producer;
					_this2.client = client;
				});

				producer.on('error', function (err) {
					_this2.logger.warn('[Kafka producer] Error: %s', JSON.stringify(err));
				});

				_this2.logger.info('[Forwarder] Created producer');
			};

			if (this.client) {
				this.client.close(createConnection);
			} else {
				createConnection();
			}
		}
	}, {
		key: 'forward',
		value: function forward(topic, data) {
			var _this3 = this;

			var msgStr = data.toString();
			var messages = msgStr.split('\n');

			messages = _.map(messages, function (m) {
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
				}], function (err, sent_data) {
					if (err) {
						return _this3.logger.warn(JSON.stringify(err));
					}
					_this3.logger.info('Forwarded messages: ' + JSON.stringify(messages));
				});
			} catch (e) {
				this.logger.info(e); //carry on
			}
		}
	}]);

	return Forwarder;
}();

exports.default = Forwarder;