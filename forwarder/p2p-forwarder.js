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

var P2PForwarder = function () {
	function P2PForwarder(config) {
		var _this = this;

		_classCallCheck(this, P2PForwarder);

		this.id = _nodeUuid2.default.v4();
		this.config = config;
		this.out = _dgram2.default.createSocket('udp4');

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
		this.forward_map = {};
		this.forward_ports = [];

		_underscore2.default.each(config.producers, function (fwd) {
			if (fwd.protocol === 'tcp') _this.createTcpSocket(fwd).done(function (binding) {
				_this.forward_ports.push(binding);
			});else _this.createUDPSocket(fwd).done(function (binding) {
				_this.forward_ports.push(binding);
			});
		});
	}

	_createClass(P2PForwarder, [{
		key: 'createUDPSocket',
		value: function createUDPSocket(fwd) {
			var _this2 = this;

			var defer = _q2.default.defer();
			this.logger.info("[p2p-Forwarder] Forwarding configuration = %d => %s", fwd.port, fwd.topic);
			var skt = _dgram2.default.createSocket('udp4');
			skt.bind(fwd.port, '127.0.0.1');

			skt.on('error', function (er) {
				_this2.logger.warn('[Forwarder.constructor()] ' + er);
			});

			var binding = {
				protocol: 'udp',
				socket: skt,
				port: fwd.port,
				topic: fwd.topic
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
					clients: []
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
		key: 'addForwardingInfo',
		value: function addForwardingInfo(topic, dest) {
			this.logger.info('[p2p-forwarder] addForwardingInfo(' + JSON.stringify(topic) + ', ' + JSON.stringify(dest) + ')');
			var destinations = this.forward_map[topic];

			if (!destinations) {
				destinations = [];
			}

			destinations.push(dest);
			this.forward_map[topic] = destinations;
			this.logger.info('[p2p-Forwarder] ' + JSON.stringify(this.forward_map));
		}
	}, {
		key: 'send',
		value: function send(host, port, message) {
			var defer = _q2.default.defer();
			this.out.send(new Buffer(message), 0, message.length, port, host, function (err) {
				if (err) {
					return defer.reject(err);
				} else {
					return defer.resolve();
				}
			});

			return defer.promise;
		}
	}, {
		key: 'forward',
		value: function forward(topic, data) {
			var _this4 = this;

			var msgStr = data.toString();

			_underscore2.default.each(this.forward_map[topic], function (endpoint) {
				_this4.logger.info('[p2p-Forwarder] Forward data ' + msgStr + ' to ' + endpoint.host + ':' + endpoint.port);
				_this4.send(endpoint.host, endpoint.port, msgStr);
			});
		}
	}]);

	return P2PForwarder;
}();

exports.default = P2PForwarder;