'use strict';

Object.defineProperty(exports, "__esModule", {
	value: true
});

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

var _nodeType = require('../node-type.js');

var _nodeType2 = _interopRequireDefault(_nodeType);

var _netmask = require('netmask');

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var _ = require('underscore');
var q = require('q');

var Role = function () {
	function Role(initId, config, sockets) {
		_classCallCheck(this, Role);

		this.initId = initId;
		this.config = config;
		this.sockets = sockets;
	}

	_createClass(Role, [{
		key: 'isDatasink',
		value: function isDatasink() {
			return _.contains(this.config.roles, _nodeType2.default.DATASINK);
		}
	}, {
		key: 'isDatasinkSlave',
		value: function isDatasinkSlave() {
			return _.contains(this.config.roles, _nodeType2.default.DATASINK_SLAVE);
		}
	}, {
		key: 'isProducer',
		value: function isProducer() {
			return _.contains(this.config.roles, _nodeType2.default.PRODUCER);
		}
	}, {
		key: 'isConsumer',
		value: function isConsumer() {
			return _.contains(this.config.roles, _nodeType2.default.CONSUMER);
		}
	}, {
		key: 'isValidPort',
		value: function isValidPort(port) {
			return _.isNumber(port) && port > 0 && port < 65535;
		}
	}, {
		key: 'onStart',
		value: function onStart() {
			var defer = q.defer();
			defer.resolve();
			return defer.promise;
		}
	}, {
		key: 'onStop',
		value: function onStop() {
			var defer = q.defer();
			defer.resolve();
			return defer.promise;
		}
	}, {
		key: 'handleHello',
		value: function handleHello(msg) {
			var defer = q.defer();
			defer.resolve(msg);
			return defer.promise;
		}
	}, {
		key: 'handleConfig',
		value: function handleConfig(msg) {
			var defer = q.defer();
			defer.resolve(msg);
			return defer.promise;
		}
	}, {
		key: 'handleReconfig',
		value: function handleReconfig(msg) {
			var defer = q.defer();
			defer.resolve(msg);
			return defer.promise;
		}
	}, {
		key: 'handleSubscribe',
		value: function handleSubscribe() {
			var defer = q.defer();
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
				type: 'reconfig',
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
	}]);

	return Role;
}();

exports.default = Role;