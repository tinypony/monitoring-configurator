'use strict';

Object.defineProperty(exports, "__esModule", {
	value: true
});

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

var _roles = require('./roles');

var _roles2 = _interopRequireDefault(_roles);

var _kafkaForwarder = require('../kafka/kafka-forwarder.js');

var _kafkaForwarder2 = _interopRequireDefault(_kafkaForwarder);

var _q = require('q');

var _q2 = _interopRequireDefault(_q);

var _underscore = require('underscore');

var _underscore2 = _interopRequireDefault(_underscore);

var _nodeType = require('../node-type');

var _nodeType2 = _interopRequireDefault(_nodeType);

var _child_process = require('child_process');

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _possibleConstructorReturn(self, call) { if (!self) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return call && (typeof call === "object" || typeof call === "function") ? call : self; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

var DatasinkRole = function (_Role) {
	_inherits(DatasinkRole, _Role);

	function DatasinkRole(initId, config, sockets) {
		_classCallCheck(this, DatasinkRole);

		var _this = _possibleConstructorReturn(this, Object.getPrototypeOf(DatasinkRole).call(this, initId, config, sockets));

		if (_this.isMe()) {
			_this.kafkaForwarder = new _kafkaForwarder2.default(config);
			_this.brokers = [0];
			_this.nextBrokerId = 1;
		}
		return _this;
	}

	_createClass(DatasinkRole, [{
		key: 'isMe',
		value: function isMe() {
			return this.isDatasink();
		}
	}, {
		key: 'onStart',
		value: function onStart() {
			var _this2 = this;

			var defer = _q2.default.defer();
			var message = this.getReconfigureMessage();

			this.broadcast(message).then(function () {
				_this2.logger.info('[Datasink] Broadcasted datasink config');
				defer.resolve();
			}, function (err) {
				return defer.reject(err);
			});

			return defer.promise;
		}
	}, {
		key: 'isSlave',
		value: function isSlave(msg) {
			return msg.roles && _underscore2.default.contains(msg.roles, _nodeType2.default.DATASINK_SLAVE);
		}
	}, {
		key: 'handleHello',
		value: function handleHello(msg) {
			var _this3 = this;

			var defer = _q2.default.defer();

			if (this.initId && msg.uuid === this.initId) {
				this.address = msg.host;
				this.initId = null;
			}

			var configMessage = this.getConfigureMessage(this.isSlave(msg) ? { brokerId: this.nextBrokerId } : undefined);

			this.respondTo(msg, configMessage).then(function () {
				_this3.logger.info('[Datasink] Responded to hello');
				if (_this3.isSlave(msg)) {
					_this3.logger.info('[Datasink] Responded to hello from slave');
					_this3.nextBrokerId++;
				}
				defer.resolve(msg);
			}, function (err) {
				return defer.reject(err);
			});

			return defer.promise;
		}
	}, {
		key: 'handleSubscribe',
		value: function handleSubscribe(msg) {
			var _this4 = this;

			var defer = _q2.default.defer();
			this.logger.info('[Datasink] handle subscribe ' + JSON.stringify(msg));

			if (_underscore2.default.isEmpty(msg.endpoints)) {
				this.logger.info('[Datasink] no endpoints specified in subscribe request');
				defer.resolve(msg);
			}

			var callback = _underscore2.default.after(msg.endpoints.length, function () {
				_this4.logger.info('[Datasink] all endpoints subscribed');
				defer.resolve(msg);
			});

			_underscore2.default.each(msg.endpoints, function (ep) {
				_this4.logger.info('[Datasink] wire %s to %s:%d', ep.topics.join(","), msg.host, ep.port);
				ep.host = msg.host;
				ep.unicastport = msg.port;
				_this4.kafkaForwarder.subscribe(ep);
				callback();
			});

			return defer.promise;
		}
	}, {
		key: 'rebalanceCluster',
		value: function rebalanceCluster() {
			var _this5 = this;

			var defer = _q2.default.defer();
			var command = '/opt/monitoring-configurator/lifecycle/on_cluster_expand.py --brokers "' + this.brokers + '"';
			this.logger.info('Running command ' + command);

			(0, _child_process.exec)(command, function (error, stdout, stderr) {
				if (error) {
					_this5.logger.warn(error);
					_this5.logger.warn(stderr);
					return defer.reject(error);
				}
				_this5.logger.info('RebalanceCluster has finished');
				_this5.logger.info(stdout);
				_this5.kafkaForwarder.handleRebalance();
				defer.resolve();
			});
			return defer.promise;
		}
	}, {
		key: 'handleRegslave',
		value: function handleRegslave(msg) {
			var defer = _q2.default.defer();
			this.brokers.push(msg.brokerId);
			this.logger.info('Registered brokers: ' + this.brokers.join(','));
			this.rebalanceCluster().then(function () {
				return defer.resolve(msg);
			}, function (er) {
				return defer.reject(er);
			});
			return defer.promise;
		}
	}]);

	return DatasinkRole;
}(_roles2.default);

exports.default = DatasinkRole;