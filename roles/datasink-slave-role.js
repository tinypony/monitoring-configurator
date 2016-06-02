'use strict';

Object.defineProperty(exports, "__esModule", {
	value: true
});

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

var _get = function get(object, property, receiver) { if (object === null) object = Function.prototype; var desc = Object.getOwnPropertyDescriptor(object, property); if (desc === undefined) { var parent = Object.getPrototypeOf(object); if (parent === null) { return undefined; } else { return get(parent, property, receiver); } } else if ("value" in desc) { return desc.value; } else { var getter = desc.get; if (getter === undefined) { return undefined; } return getter.call(receiver); } };

var _roles = require('./roles');

var _roles2 = _interopRequireDefault(_roles);

var _underscore = require('underscore');

var _underscore2 = _interopRequireDefault(_underscore);

var _kafkaForwarder = require('../kafka/kafka-forwarder.js');

var _kafkaForwarder2 = _interopRequireDefault(_kafkaForwarder);

var _q = require('q');

var _q2 = _interopRequireDefault(_q);

var _child_process = require('child_process');

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _possibleConstructorReturn(self, call) { if (!self) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return call && (typeof call === "object" || typeof call === "function") ? call : self; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

var DatasinkSlaveRole = function (_Role) {
	_inherits(DatasinkSlaveRole, _Role);

	function DatasinkSlaveRole(initId, config, sockets) {
		_classCallCheck(this, DatasinkSlaveRole);

		return _possibleConstructorReturn(this, Object.getPrototypeOf(DatasinkSlaveRole).call(this, initId, config, sockets));
	}

	_createClass(DatasinkSlaveRole, [{
		key: 'isMe',
		value: function isMe() {
			return this.isDatasinkSlave();
		}
	}, {
		key: 'onStart',
		value: function onStart(prev) {
			var _this2 = this;

			if (prev && prev.hello_sent) {
				return _get(Object.getPrototypeOf(DatasinkSlaveRole.prototype), 'onStart', this).call(this);
			}

			var defer = _q2.default.defer();
			var message = this.getHelloMessage();

			this.sockets.broadcast.send(new Buffer(message), 0, message.length, this.config.broadcastPort, this.getBroadcastAddress(), function (err) {
				if (err) {
					_this2.logger.warn(err);
					return defer.reject(err);
				} else {
					defer.resolve({
						hello_sent: true
					});
				}
			});
			return defer.promise;
		}
	}, {
		key: 'registerSlave',
		value: function registerSlave(broker_id, originalConfigMsg) {
			var _this3 = this;

			return function () {
				var registerMsg = _this3.getSlaveRegisterMessage(broker_id);
				return _this3.respondTo(originalConfigMsg, registerMsg);
			};
		}
	}, {
		key: 'modifyKafkaConfig',
		value: function modifyKafkaConfig(broker_id, zookeeper_host, zookeeper_port) {
			var _this4 = this;

			var defer = _q2.default.defer();
			(0, _child_process.exec)('/opt/monitoring-configurator/lifecycle/on_configuration_receive.sh ' + broker_id + ' ' + zookeeper_host + ' ' + zookeeper_port, function (error, stdout, stderr) {
				if (error) {
					_this4.logger.warn(error);
					_this4.logger.warn(stderr);
					return defer.reject();
				}
				_this4.logger.info('Kafka reconfigured');
				_this4.logger.info(stdout);
				defer.resolve();
			});
			return defer.promise;
		}

		//Don't configure client here, but initialize hello-config-regslave sequence.

	}, {
		key: 'reconfigureClient',
		value: function reconfigureClient(msg) {
			var defer = _q2.default.defer();
			this.onStart().then(function () {
				return defer.resolve(msg);
			}, function (err) {
				return defer.reject(err);
			});
			return defer.promise;
		}
	}, {
		key: 'configureClient',
		value: function configureClient(msg) {
			var defer = _q2.default.defer();
			this.config.monitoring = _underscore2.default.extend(this.config.monitoring, msg.monitoring);

			if (!this.isValidPort(msg.port)) {
				defer.reject();
				return defer.promise;
			}

			this.modifyKafkaConfig(msg.brokerId, msg.monitoring.host, msg.monitoring.port).then(this.registerSlave(msg.brokerId, msg), function (err) {
				return defer.reject(err);
			}).then(function () {
				return defer.resolve(msg);
			}, function (err) {
				return defer.reject(err);
			});

			return defer.promise;
		}
	}, {
		key: 'handleConfig',
		value: function handleConfig(msg) {
			return this.configureClient(msg);
		}
	}, {
		key: 'handleReconfig',
		value: function handleReconfig(msg) {
			return this.reconfigureClient(msg);
		}
	}]);

	return DatasinkSlaveRole;
}(_roles2.default);

exports.default = DatasinkSlaveRole;