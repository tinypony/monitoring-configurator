'use strict';

Object.defineProperty(exports, "__esModule", {
	value: true
});

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

var _get = function get(object, property, receiver) { if (object === null) object = Function.prototype; var desc = Object.getOwnPropertyDescriptor(object, property); if (desc === undefined) { var parent = Object.getPrototypeOf(object); if (parent === null) { return undefined; } else { return get(parent, property, receiver); } } else if ("value" in desc) { return desc.value; } else { var getter = desc.get; if (getter === undefined) { return undefined; } return getter.call(receiver); } };

var _roles = require('./roles');

var _roles2 = _interopRequireDefault(_roles);

var _kafkaForwarder = require('../kafka/kafka-forwarder.js');

var _kafkaForwarder2 = _interopRequireDefault(_kafkaForwarder);

var _q = require('q');

var _q2 = _interopRequireDefault(_q);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _possibleConstructorReturn(self, call) { if (!self) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return call && (typeof call === "object" || typeof call === "function") ? call : self; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

var DatasinkRole = function (_Role) {
	_inherits(DatasinkRole, _Role);

	function DatasinkRole(initId, config, sockets) {
		_classCallCheck(this, DatasinkRole);

		var _this = _possibleConstructorReturn(this, Object.getPrototypeOf(DatasinkRole).call(this, initId, config, sockets));

		if (_this.isDatasink()) {
			_this.kafkaForwarder = new _kafkaForwarder2.default(config);
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

			this.sockets.broadcast.send(new Buffer(message), 0, message.length, this.config.broadcastPort, this.getBroadcastAddress(), function (err) {
				if (err) {
					_this2.logger.warn(err);
					return defer.reject(err);
				}
				_this2.logger.info('[Datasink] Broadcasted datasink config');

				defer.resolve();
			});
			return defer.promise;
		}
	}, {
		key: 'handleHello',
		value: function handleHello(msg) {
			var _this3 = this;

			if (!this.isDatasink()) {
				return _get(Object.getPrototypeOf(DatasinkRole.prototype), 'handleHello', this).call(this, msg); //does nothing just returns resolved promise
			}

			var defer = _q2.default.defer();

			if (this.initId && msg.uuid === this.initId) {
				this.address = msg.host;
				this.initId = null;
			}

			if (this.isDatasink()) {
				var configMessage = this.getConfigureMessage();

				this.sockets.unicast.send(new Buffer(configMessage), 0, configMessage.length, msg.port, msg.host, function (err) {
					if (err) {
						return defer.reject(err);
					}
					_this3.logger.info('[Datasink] Responded to hello');
					defer.resolve(msg);
				});
			}

			return defer.promise;
		}
	}, {
		key: 'handleSubscribe',
		value: function handleSubscribe(msg) {
			var _this4 = this;

			var defer = _q2.default.defer();
			this.logger.info('[Datasink] let\'s subscribe');

			if (_.isEmpty(msg.endpoints)) {
				this.logger.info('[Datasink] no endpoints specified in subscribe request');
				defer.resolve(msg);
			}

			var callback = _.after(msg.endpoints.length, function () {
				_this4.logger.info('[Datasink] all endpoints subscribed');
				defer.resolve(msg);
			});

			_.each(msg.endpoints, function (ep) {
				_this4.logger.info('[Datasink] wire %s to %s:%d', ep.topics.join(","), msg.host, ep.port);
				ep.host = msg.host;
				ep.unicastport = msg.port;
				_this4.kafkaForwarder.subscribe(ep);
				callback();
			});

			return defer.promise;
		}
	}]);

	return DatasinkRole;
}(_roles2.default);

exports.default = DatasinkRole;