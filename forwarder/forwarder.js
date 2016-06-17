'use strict';

Object.defineProperty(exports, "__esModule", {
	value: true
});

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

var _dgram = require('dgram');

var _dgram2 = _interopRequireDefault(_dgram);

var _net = require('net');

var _net2 = _interopRequireDefault(_net);

var _underscore = require('underscore');

var _underscore2 = _interopRequireDefault(_underscore);

var _q = require('q');

var _q2 = _interopRequireDefault(_q);

var _kafkaNode = require('kafka-node');

var _nodeUuid = require('node-uuid');

var _nodeUuid2 = _interopRequireDefault(_nodeUuid);

var _winston = require('winston');

var _winston2 = _interopRequireDefault(_winston);

var _doubleEndedQueue = require('double-ended-queue');

var _doubleEndedQueue2 = _interopRequireDefault(_doubleEndedQueue);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function isValidPort(port) {
	return _underscore2.default.isNumber(port) && port > 0 && port < 65535;
}

var Forwarder = function () {
	function Forwarder(config) {
		var _this = this;

		_classCallCheck(this, Forwarder);

		this.id = _nodeUuid2.default.v4();
		this.debug = true;
		this.config = config;
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
		this.forward_ports = [];
		_underscore2.default.each(config.producers, function (fwd) {
			_this.createTcpSocket(fwd).done(function (binding) {
				_this.forward_ports.push(binding);
			});
		});
	}

	_createClass(Forwarder, [{
		key: 'createUDPSocket',
		value: function createUDPSocket(fwd) {
			var _this2 = this;

			var defer = _q2.default.defer();
			this.logger.info("Forwarding configuration = %d => %s", fwd.port, fwd.topic);
			var skt = _dgram2.default.createSocket('udp4');
			var FIFO = new _doubleEndedQueue2.default();
			skt.bind(fwd.port, '127.0.0.1');

			skt.on('error', function (er) {
				_this2.logger.warn('[Forwarder.constructor()] ' + er);
			});

			var binding = {
				protocol: 'udp',
				socket: skt,
				port: fwd.port,
				topic: fwd.topic,
				FIFO: FIFO,
				FIFO_flushed: true
			};

			skt.on("message", this.forward.bind(this, fwd.topic));

			defer.resolve(binding);
			return defer.promise;
		}
	}, {
		key: 'createTcpSocket',
		value: function createTcpSocket(fwd) {
			var _this3 = this;

			var defer = _q2.default.defer();
			var FIFO = new _doubleEndedQueue2.default();
			// Start a TCP Server
			_net2.default.createServer(function (socket) {
				var binding = {
					protocol: 'tcp',
					port: fwd.port,
					topic: fwd.topic,
					clients: [],
					FIFO: FIFO,
					FIFO_flushed: true
				};

				// Identify this client
				socket.name = socket.remoteAddress + ":" + socket.remotePort;

				// Put this new client in the list
				binding.clients.push(socket);

				// Handle incoming messages from clients.
				socket.on('data', _this3.forward.bind(_this3, fwd.topic));
				// Remove the client from the list when it leaves
				socket.on('end', function () {
					binding.clients.splice(binding.clients.indexOf(socket), 1);
				});

				defer.resolve(binding);
			}).listen(fwd.port);

			return defer.promise;
		}
	}, {
		key: 'storeInQueue',
		value: function storeInQueue(topic, binding, data_buf) {
			var data = data_buf.toString();
			if (!data) return;

			var FIFO = binding.FIFO;


			FIFO.push(data);

			this.logger.info('[Forwarder] Sotred in queue ' + data);

			if (binding.FIFO_flushed) {
				binding.FIFO_flushed = false;
				setImmediate(this.run.bind(this, binding));
			}
		}

		/* Continuously polls the queue and forwards messages from it */

	}, {
		key: 'run',
		value: function run(binding) {
			var FIFO = binding.FIFO;
			var topic = binding.topic;


			while (FIFO.length) {
				var messages = [];

				for (var i = 0; i < 10; i++) {
					var data = FIFO.shift();
					if (data) messages.push(data);
				}

				this.forward(topic, messages.join('\n'));
			}

			binding.FIFO_flushed = true;
		}
	}, {
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
		key: 'createConnection',
		value: function createConnection() {
			var _this4 = this;

			var defer = _q2.default.defer();
			var connectionString = this.getZK();
			this.logger.info('Create zookeeper connection to %s', connectionString);
			var client = new _kafkaNode.Client(connectionString, this.id);
			var producer = new _kafkaNode.HighLevelProducer(client);

			producer.on('ready', function () {
				_this4.logger.info('Forwader is ready');
				_this4.producer = producer;
				_this4.client = client;
				defer.resolve();
			});

			producer.on('error', function (err) {
				_this4.logger.warn('[Forwarder.reconfig()] Error: %s', JSON.stringify(err));
				defer.reject(err);
			});

			this.logger.info('[Forwarder] Created new producer');
			return defer.promise;
		}
	}, {
		key: 'reconnect',
		value: function reconnect() {
			var _this5 = this;

			var defer = _q2.default.defer();
			this.logger.info('[Forwarder.reconnect()] Using nodejs forwarder');

			if (this.producer) {
				this.producer.close(function () {
					_this5.logger.info('[Forwarder.reconnect()] Closed the producer, reconnecting');
					_this5.producer = null;
					_this5.createConnection().then(defer.resolve, function (err) {
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
			var _this6 = this;

			var msgStr = data.toString();
			var messages = msgStr.split('\n');

			messages = _underscore2.default.map(messages, function (m) {
				var val = m.replace(/\r$/g, '');
				return val;
			});

			if (!this.forwardToPort || !this.forwardToAddress || !this.producer || !msgStr) {
				return;
			}

			//contain possible errors if datasink is temporarily down
			try {
				this.producer.send([{
					topic: topic,
					messages: messages
				}], function (err) {
					if (err) {
						return _this6.logger.warn('[Forwarder.forward()] ' + JSON.stringify(err));
					}

					if (_this6.debug) {
						_this6.logger.info('Forwarded ' + messages);
						_this6.debug = false;
					}
				});

				if (topic === 'latency') {
					this.logger.info('Forwarding ' + messages.join(';\n'));
				}
			} catch (e) {
				this.logger.warn(e); //carry on
			}
		}
	}]);

	return Forwarder;
}();

exports.default = Forwarder;