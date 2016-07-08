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
		this.msgStr;

		this.logger = new _winston2.default.Logger({
			transports: [new _winston2.default.transports.Console()]
		});

		if (config.logging && config.logging.disable) {
			this.logger.remove(_winston2.default.transports.Console);
		}

		/**
   * fwd.port,
   * fwd.topic,
   * fwd.protocol //udp or tcp
   */
		_underscore2.default.each(config.producers, function (fwd) {
			if (fwd.protocol === 'tcp') _this.createTcpSocket(fwd);else _this.createUDPSocket(fwd);
		});
	}

	_createClass(Forwarder, [{
		key: 'createUDPSocket',
		value: function createUDPSocket(fwd) {
			var _this2 = this;

			this.logger.info("Forwarding configuration = (UDP) %d => %s", fwd.port, fwd.topic);
			var skt = _dgram2.default.createSocket('udp4');
			skt.bind(fwd.port, '127.0.0.1');

			skt.on('error', function (er) {
				_this2.logger.warn('[Forwarder.constructor()] ' + er);
			});
			skt.on("message", this.forward.bind(this, fwd.topic));
		}
	}, {
		key: 'createTcpSocket',
		value: function createTcpSocket(fwd) {
			var _this3 = this;

			this.logger.info("Forwarding configuration = (TCP) %d => %s", fwd.port, fwd.topic);
			// Start a TCP Server
			_net2.default.createServer(function (socket) {
				_this3.logger.info('New incoming connection');
				// Handle incoming messages from clients.
				socket.on('data', _this3.forward.bind(_this3, fwd.topic));
			}).listen(fwd.port);
		}

		// storeInQueue(topic, binding, data_buf) {
		// 	let data = data_buf.toString();
		// 	if(!data) return;

		// 	var { FIFO } = binding;

		// 	FIFO.push(data);

		// 	this.logger.info(`[Forwarder] Sotred in queue ${data}`);

		// 	if(binding.FIFO_flushed) {
		// 		binding.FIFO_flushed = false;
		// 		setImmediate(this.run.bind(this, binding));
		// 	}
		// }

		//  Continuously polls the queue and forwards messages from it
		// run(binding) {
		// 	let { FIFO, topic } = binding;

		// 	while(FIFO.length) {
		// 		let messages = [];

		// 		for(let i=0; i<10; i++) {
		// 			let data = FIFO.shift();
		// 			if(data) messages.push(data);
		// 		}

		// 		this.forward(topic, messages.join('\n'));
		// 	}

		// 	binding.FIFO_flushed = true;
		// }

	}, {
		key: 'reconfig',
		value: function reconfig(config) {
			var _this4 = this;

			if (!isValidPort(config.monitoring.port)) {
				this.logger.warn('trying to configure forwarder with an invalid port');
				return;
			}
			this.config = config;
			this.forwardToAddress = config.monitoring.host;
			this.forwardToPort = config.monitoring.port;
			this.logger.info('[Forwarder.reconfig()] Reconfiguring forwarder');

			this.reconnect().catch(function (err) {
				_this4.logger.warn('[Forwarder.reconfig()] ' + JSON.stringify(err));
				_this4.reconnect();
			});
		}
	}, {
		key: 'reconnect',
		value: function reconnect() {
			var _this5 = this;

			this.logger.info('[Forwarder.reconnect()] Using nodejs forwarder');

			if (this.producer) {
				this.logger.info('[Forwarder.reconnect()] Close previous producer');
				var defer = _q2.default.defer();

				this.producer.close(function () {
					_this5.logger.info('[Forwarder.reconnect()] Closed the producer, reconnecting');
					_this5.producer = null;
					_this5.createConnection().then(defer.resolve, function (err) {
						return defer.reject(err);
					});
				});

				return defer.promise;
			} else {
				this.logger.info('[Forwarder.reconnect()] No existing producer, proceed');
				return this.createConnection();
			}
		}
	}, {
		key: 'getZK',
		value: function getZK() {
			return this.forwardToAddress + ':' + this.forwardToPort;
		}
	}, {
		key: 'createConnection',
		value: function createConnection() {
			var _this6 = this;

			var defer = _q2.default.defer();
			var connectionString = this.getZK();
			this.logger.info('Create zookeeper connection to %s', connectionString);
			var client = new _kafkaNode.Client(connectionString, this.id);
			var producer = new _kafkaNode.HighLevelProducer(client);

			producer.on('ready', function () {
				_this6.logger.info('Forwader is ready');
				_this6.producer = producer;
				defer.resolve();
			});

			producer.on('error', function (err) {
				_this6.logger.warn('[Forwarder.createConnection()] Error: %s', JSON.stringify(err));
				console.trace("error");
			});

			this.logger.info('[Forwarder] Created new kafka producer and attached handlers');
			return defer.promise;
		}
	}, {
		key: 'forward',
		value: function forward(topic, data) {
			var _this7 = this;

			this.msgStr = data.toString();

			if (!this.forwardToPort || !this.forwardToAddress || !this.producer || !this.msgStr) {
				return;
			}

			//contain possible errors if datasink is temporarily down
			try {
				this.producer.send([{
					topic: topic,
					messages: _underscore2.default.map(this.msgStr.split('\n'), function (m) {
						return m.replace(/\r$/g, '');
					})
				}], function (err) {
					if (err) {
						return _this7.logger.warn('[Forwarder.forward()] ' + JSON.stringify(err));
					}
				});
			} catch (e) {
				this.logger.warn(e); //carry on
			}
		}
	}]);

	return Forwarder;
}();

exports.default = Forwarder;